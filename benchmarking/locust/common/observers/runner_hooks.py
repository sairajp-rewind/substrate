# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared runner-side lifecycle, fail-open accounting, and artifact upload."""

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
from typing import Any, Callable

from .base import OBSERVERS, Observer, ObserverResult

NAME_RE = re.compile(r"^[a-z0-9_]+$")


def _upload_to_gcs(
    local_path: Path, gcs_uri: str, storage_client: Any | None = None
) -> None:
    if storage_client is None:
        from google.cloud import storage

        storage_client = storage.Client()

    bucket_name, _, blob_path = gcs_uri[len("gs://"):].partition("/")
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path)
    )


def default_upload_file(
    src: Path, dest: str, storage_client: Any | None = None
) -> None:
    """Upload one file to a GCS URI or local filesystem path."""
    if dest.startswith("gs://"):
        _upload_to_gcs(src, dest, storage_client=storage_client)
        return
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dest_path)


@dataclass(frozen=True)
class ObserverRunOutcome:
    """Aggregated status map and finished results for a benchmark run."""

    status_map: dict[str, dict[str, Any]]
    results: list[tuple[str, str, ObserverResult]]

    def format_stats_rows(
        self, timestamp: str, tag: str, test_name: str
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for name, obs_type, res in self.results:
            rows.append(
                {
                    "timestamp": timestamp,
                    "tag": tag,
                    "test_name": test_name,
                    "metric": f"observer_{obs_type}_{name}",
                    "measurements": {k: str(v) for k, v in res.counts.items()},
                }
            )
        return rows


class ObserverRunManager:
    """Validates, starts, finishes, and uploads configured benchmark observers."""

    def __init__(
        self,
        *,
        status_map: dict[str, dict[str, Any]],
        started: list[tuple[str, str, Observer]],
        active_dirs: dict[str, Path],
    ) -> None:
        self._status_map = status_map
        self._started = started
        self._active_dirs = active_dirs

    @property
    def has_observers(self) -> bool:
        return bool(self._status_map or self._started)

    @classmethod
    def start_all(
        cls,
        entries: Any,
        work_dir: Path,
        log_fn: Callable[[str], None],
    ) -> "ObserverRunManager":
        status_map: dict[str, dict[str, Any]] = {}
        started: list[tuple[str, str, Observer]] = []
        active_dirs: dict[str, Path] = {}

        if entries is None or entries == []:
            return cls(status_map=status_map, started=started, active_dirs=active_dirs)

        if not isinstance(entries, list):
            err = f"observers must be a list, got {type(entries).__name__}"
            log_fn(f"Observer config error: {err}")
            status_map["observers"] = {"status": "error", "error": err}
            return cls(status_map=status_map, started=started, active_dirs=active_dirs)

        seen_names: set[str] = set()
        for idx, entry in enumerate(entries):
            fallback_key = f"observers[{idx}]"
            if not isinstance(entry, dict):
                err = (
                    f"observer entry at index {idx} must be a mapping, "
                    f"got {type(entry).__name__}"
                )
                log_fn(f"Observer config error ({fallback_key}): {err}")
                status_map[fallback_key] = {"status": "error", "error": err}
                continue

            raw_name = entry.get("name")
            if not isinstance(raw_name, str) or not NAME_RE.fullmatch(raw_name):
                err = (
                    f"invalid observer name {raw_name!r}; "
                    "expected non-empty ^[a-z0-9_]+$"
                )
                log_fn(f"Observer config error ({fallback_key}): {err}")
                status_map[fallback_key] = {"status": "error", "error": err}
                continue
            if raw_name in seen_names:
                err = f"duplicate observer name {raw_name!r}"
                log_fn(f"Observer config error ({fallback_key}): {err}")
                status_map[fallback_key] = {"status": "error", "error": err}
                continue
            seen_names.add(raw_name)
            name = raw_name

            obs_type = entry.get("type")
            if not isinstance(obs_type, str) or obs_type not in OBSERVERS:
                err = f"unknown observer type {obs_type!r}"
                log_fn(f"Observer config error ({name}): {err}")
                status_map[name] = {"status": "error", "error": err}
                continue

            if "config" in entry:
                cfg = entry["config"]
                if not isinstance(cfg, dict):
                    err = (
                        f"observer {name!r} config must be a mapping, "
                        f"got {type(cfg).__name__}"
                    )
                    log_fn(f"Observer config error ({name}): {err}")
                    status_map[name] = {"status": "error", "error": err}
                    continue
            else:
                cfg = {}

            out_dir = work_dir / obs_type
            out_dir.mkdir(parents=True, exist_ok=True)
            active_dirs[obs_type] = out_dir

            try:
                obs = OBSERVERS[obs_type]()
                obs.start(name, cfg, out_dir)
            except Exception as exc:
                err = str(exc) or type(exc).__name__
                log_fn(f"Observer {name} ({obs_type}) failed to start: {err}")
                status_map[name] = {"status": "error", "error": err}
                continue

            started.append((name, obs_type, obs))

        return cls(status_map=status_map, started=started, active_dirs=active_dirs)

    def finish_all(self, log_fn: Callable[[str], None]) -> ObserverRunOutcome:
        status_map = dict(self._status_map)
        results: list[tuple[str, str, ObserverResult]] = []

        for name, obs_type, obs in self._started:
            try:
                res = obs.final_observation()
            except Exception as exc:
                err = str(exc) or type(exc).__name__
                log_fn(f"Observer {name} ({obs_type}) failed during final_observation: {err}")
                status_map[name] = {"status": "error", "error": err}
                continue

            entry_status: dict[str, Any] = {"status": res.status}
            if res.error is not None:
                entry_status["error"] = res.error
            if res.status == "error":
                log_fn(f"Observer {name} ({obs_type}) reported error: {res.error}")
            status_map[name] = entry_status
            results.append((name, obs_type, res))

        return ObserverRunOutcome(status_map=status_map, results=results)

    def upload_directories(
        self,
        dest_prefix: str,
        upload_file_fn: Callable[[Path, str], None] | None = None,
    ) -> None:
        prefix = dest_prefix.rstrip("/")
        if upload_file_fn is not None:
            uploader = upload_file_fn
        elif prefix.startswith("gs://"):
            gcs_client: Any | None = None

            def uploader(src: Path, dest: str) -> None:
                nonlocal gcs_client
                if gcs_client is None:
                    from google.cloud import storage

                    gcs_client = storage.Client()
                default_upload_file(src, dest, storage_client=gcs_client)
        else:
            uploader = default_upload_file

        for obs_type, out_dir in sorted(self._active_dirs.items()):
            if not out_dir.is_dir():
                continue
            files = sorted(
                (p for p in out_dir.rglob("*") if p.is_file()),
                key=lambda p: p.relative_to(out_dir).as_posix(),
            )
            for path in files:
                rel = path.relative_to(out_dir).as_posix()
                uploader(path, f"{prefix}/{obs_type}/{rel}")
