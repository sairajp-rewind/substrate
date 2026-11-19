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

"""Pprof observer collecting Go runtime profiles from Kubernetes pods."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import http.client
import os
from pathlib import Path
import re
import socket
import threading
import time
from typing import Any, Callable

from .base import OBSERVERS, ObserverResult

WINDOWED_PROFILES: tuple[str, ...] = ("profile", "allocs", "mutex", "block")
INSTANTANEOUS_PROFILES: tuple[str, ...] = (
    "heap",
    "goroutine",
    "goroutineleak",
    "threadcreate",
)
ALLOWED_PROFILES: frozenset[str] = frozenset(WINDOWED_PROFILES) | frozenset(
    INSTANTANEOUS_PROFILES
)
DEFAULT_PROFILES: tuple[str, ...] = ("profile", "allocs", "heap", "goroutine")

ALLOWED_CONFIG_KEYS: frozenset[str] = frozenset(
    {"namespace", "label_selector", "profiles", "interval", "duration"}
)

DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_DURATION_SECONDS = 15

CONNECT_TIMEOUT_SECONDS = 2.0
BASELINE_DEADLINE_SECONDS = 5.0
FINAL_DEADLINE_SECONDS = 15.0
READ_BUDGET_MARGIN_SECONDS = 10.0
STREAM_CHUNK_BYTES = 65536
ERROR_BODY_LIMIT_BYTES = 65536

_DURATION_RE = re.compile(r"^\s*([+-]?\d+)\s*([smh]?)\s*$")
_DURATION_UNITS: dict[str, int] = {"": 1, "s": 1, "m": 60, "h": 3600}

# Coordinates /debug/pprof/profile across all PprofObserver instances in the process.
_CPU_LOCKS_MU = threading.Lock()
_CPU_LOCKS: dict[str, threading.Lock] = {}


def _get_cpu_lock(pod_uid: str) -> threading.Lock:
    with _CPU_LOCKS_MU:
        lock = _CPU_LOCKS.get(pod_uid)
        if lock is None:
            lock = threading.Lock()
            _CPU_LOCKS[pod_uid] = lock
        return lock


def parse_duration_seconds(value: Any) -> int:
    """Parse an integer duration with optional [smh] suffix into whole seconds."""
    if isinstance(value, bool):
        raise ValueError(f"duration must be an integer or <int>[smh] string, got {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        match = _DURATION_RE.fullmatch(value)
        if match is None:
            raise ValueError(f"invalid duration {value!r}; expected integer or <int>[smh]")
        amount = int(match.group(1))
        unit = _DURATION_UNITS[match.group(2)]
        return amount * unit
    raise ValueError(f"duration must be an integer or <int>[smh] string, got {value!r}")


def _default_core_v1_api() -> Any:
    from kubernetes import client, config as k8s_config

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return client.CoreV1Api()


def _utc_ms_timestamp() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%S')}.{now.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class _TargetPod:
    uid: str
    namespace: str
    name: str
    ip: str
    pprof_port: int
    pprof_container: str
    restart_count: int | None


class PprofObserver:
    """Discovers pods exposing a `pprof` container port and scrapes Go profiles."""

    OBSERVER_TYPE = "pprof"

    def __init__(self, core_v1_factory: Callable[[], Any] | None = None) -> None:
        self._core_v1_factory = core_v1_factory or _default_core_v1_api
        self._core_v1: Any = None
        self._name = ""
        self._out_dir = Path(".")
        self._log_path: Path | None = None
        self._namespace = ""
        self._label_selector = ""
        self._profiles: list[str] = list(DEFAULT_PROFILES)
        self._interval = DEFAULT_INTERVAL_SECONDS
        self._duration = DEFAULT_DURATION_SECONDS

        self._log_mu = threading.Lock()
        self._stats_mu = threading.Lock()
        self._conns_mu = threading.Lock()
        self._stop_event = threading.Event()
        self._active_conns: set[http.client.HTTPConnection] = set()
        self._thread: threading.Thread | None = None
        self._finished = False

        self._pods_discovered: set[str] = set()
        self._pods_profiled: set[str] = set()
        self._seen_restarts: dict[str, int] = {}
        self._scrapes_succeeded = 0
        self._scrapes_failed = 0
        self._scrapes_busy_skipped = 0
        self._scrapes_overlap_skipped = 0
        self._bytes_collected = 0

    def _log(self, msg: str) -> None:
        if self._log_path is None:
            return
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"[{ts}] {msg}\n"
        with self._log_mu:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(line)

    def _validate_and_configure(self, name: str, cfg: dict[str, Any], out_dir: Path) -> None:
        if not isinstance(cfg, dict):
            raise ValueError(f"config must be a mapping, got {type(cfg).__name__}")
        unknown = set(cfg) - ALLOWED_CONFIG_KEYS
        if unknown:
            raise ValueError(f"unknown config keys: {', '.join(sorted(unknown))}")

        namespace = cfg.get("namespace", "")
        if not isinstance(namespace, str):
            raise ValueError(f"namespace must be a string, got {type(namespace).__name__}")

        label_selector = cfg.get("label_selector", "")
        if not isinstance(label_selector, str):
            raise ValueError(
                f"label_selector must be a string, got {type(label_selector).__name__}"
            )

        if "profiles" in cfg:
            raw_profiles = cfg["profiles"]
            if not isinstance(raw_profiles, list) or not raw_profiles:
                raise ValueError("profiles must be a non-empty list")
            deduped: list[str] = []
            seen: set[str] = set()
            for prof in raw_profiles:
                if not isinstance(prof, str):
                    raise ValueError(
                        f"profile names must be strings, got {type(prof).__name__}"
                    )
                if prof == "trace":
                    raise ValueError("profile 'trace' is not supported in v1")
                if prof not in ALLOWED_PROFILES:
                    raise ValueError(f"unknown profile {prof!r}")
                if prof not in seen:
                    seen.add(prof)
                    deduped.append(prof)
            profiles = deduped
        else:
            profiles = list(DEFAULT_PROFILES)

        interval = parse_duration_seconds(cfg.get("interval", DEFAULT_INTERVAL_SECONDS))
        duration = parse_duration_seconds(cfg.get("duration", DEFAULT_DURATION_SECONDS))
        if duration < 1:
            raise ValueError(f"duration must be >= 1s, got {duration}s")
        if interval < 1:
            raise ValueError(f"interval must be >= 1s, got {interval}s")
        if duration > interval:
            raise ValueError(
                f"duration ({duration}s) must not exceed interval ({interval}s)"
            )

        self._name = name
        self._out_dir = out_dir
        self._namespace = namespace
        self._label_selector = label_selector
        self._profiles = profiles
        self._interval = interval
        self._duration = duration

    def start(self, name: str, config: dict[str, Any], out_dir: Path) -> None:
        self._validate_and_configure(name, config, out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = out_dir / f"{name}.log"
        self._core_v1 = self._core_v1_factory()

        self._log(
            f"Starting pprof observer {name!r}: namespace={self._namespace!r} "
            f"label_selector={self._label_selector!r} profiles={self._profiles} "
            f"interval={self._interval}s duration={self._duration}s"
        )

        # Capture pre-load baseline for instantaneous profiles before returning.
        pods = self._discover_pods()
        if not pods:
            self._log("Warning: no Ready pods with a pprof port matched at start(); retrying periodically")
        elif self._instantaneous_profiles():
            deadline = time.monotonic() + BASELINE_DEADLINE_SECONDS
            self._scrape_instantaneous_phase(pods, final_gc=False, deadline=deadline)

        self._thread = threading.Thread(
            target=self._periodic_loop,
            name=f"pprof-observer-{name}",
            daemon=True,
        )
        self._thread.start()

    def final_observation(self) -> ObserverResult:
        if not self._finished:
            self._finished = True
            self._stop_event.set()
            self._cancel_active_connections()
            if self._thread is not None:
                self._thread.join()

            pods = self._discover_pods()
            if pods and self._instantaneous_profiles():
                deadline = time.monotonic() + FINAL_DEADLINE_SECONDS
                self._scrape_instantaneous_phase(pods, final_gc=True, deadline=deadline)

        counts = self._snapshot_counts()
        if counts["pods_profiled"] == 0:
            self._log("Completed with 0 pods profiled")
            return ObserverResult(
                status="error",
                error="no pods profiled",
                counts=counts,
            )
        self._log(f"Completed cleanly: {counts}")
        return ObserverResult(status="ok", counts=counts)

    def _windowed_profiles(self) -> list[str]:
        return [p for p in self._profiles if p in WINDOWED_PROFILES]

    def _instantaneous_profiles(self) -> list[str]:
        return [p for p in self._profiles if p in INSTANTANEOUS_PROFILES]

    def _snapshot_counts(self) -> dict[str, int]:
        with self._stats_mu:
            return {
                "pods_discovered": len(self._pods_discovered),
                "pods_profiled": len(self._pods_profiled),
                "scrapes_succeeded": self._scrapes_succeeded,
                "scrapes_failed": self._scrapes_failed,
                "scrapes_busy_skipped": self._scrapes_busy_skipped,
                "scrapes_overlap_skipped": self._scrapes_overlap_skipped,
                "bytes_collected": self._bytes_collected,
            }

    def _cancel_active_connections(self) -> None:
        with self._conns_mu:
            conns = list(self._active_conns)
        for conn in conns:
            sock = conn.sock
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

    def _discover_pods(self) -> list[_TargetPod]:
        if self._core_v1 is None:
            return []
        try:
            if self._namespace:
                resp = self._core_v1.list_namespaced_pod(
                    namespace=self._namespace,
                    label_selector=self._label_selector,
                )
            else:
                resp = self._core_v1.list_pod_for_all_namespaces(
                    label_selector=self._label_selector,
                )
        except Exception as exc:
            self._log(f"Warning: pod discovery failed: {exc}")
            return []

        targets: list[_TargetPod] = []
        for pod in getattr(resp, "items", None) or []:
            target = self._extract_target_pod(pod)
            if target is None:
                continue
            targets.append(target)
            with self._stats_mu:
                self._pods_discovered.add(target.uid)
                if target.restart_count is not None:
                    prev = self._seen_restarts.get(target.uid)
                    if prev is not None and target.restart_count > prev:
                        self._log(
                            f"Warning: pod {target.namespace}/{target.name} container "
                            f"{target.pprof_container} restarted ({prev} -> {target.restart_count})"
                        )
                    self._seen_restarts[target.uid] = target.restart_count
        return targets

    def _extract_target_pod(self, pod: Any) -> _TargetPod | None:
        status = getattr(pod, "status", None)
        if getattr(status, "phase", None) != "Running":
            return None
        conditions = getattr(status, "conditions", None) or []
        if not any(
            getattr(c, "type", None) == "Ready" and getattr(c, "status", None) == "True"
            for c in conditions
        ):
            return None
        pod_ip = getattr(status, "pod_ip", None)
        if not isinstance(pod_ip, str) or not pod_ip:
            return None

        spec = getattr(pod, "spec", None)
        pprof_container = ""
        pprof_port = 0
        for container in getattr(spec, "containers", None) or []:
            for port in getattr(container, "ports", None) or []:
                if getattr(port, "name", None) == "pprof":
                    cport = getattr(port, "container_port", None)
                    if isinstance(cport, int) and not isinstance(cport, bool) and cport > 0:
                        pprof_container = getattr(container, "name", "") or ""
                        pprof_port = cport
                        break
            if pprof_port:
                break
        if not pprof_port:
            return None

        meta = getattr(pod, "metadata", None)
        pod_ns = getattr(meta, "namespace", None) or "default"
        pod_name = getattr(meta, "name", None) or ""
        pod_uid = getattr(meta, "uid", None) or f"{pod_ns}/{pod_name}"

        restart_count: int | None = None
        for cs in getattr(status, "container_statuses", None) or []:
            if getattr(cs, "name", None) == pprof_container:
                raw_rc = getattr(cs, "restart_count", None)
                if isinstance(raw_rc, int) and not isinstance(raw_rc, bool):
                    restart_count = raw_rc
                break

        return _TargetPod(
            uid=pod_uid,
            namespace=pod_ns,
            name=pod_name,
            ip=pod_ip,
            pprof_port=pprof_port,
            pprof_container=pprof_container,
            restart_count=restart_count,
        )

    def _periodic_loop(self) -> None:
        while not self._stop_event.is_set():
            tick_start = time.monotonic()
            self._run_periodic_tick()
            elapsed = time.monotonic() - tick_start
            wait_s = max(0.0, self._interval - elapsed)
            if self._stop_event.wait(timeout=wait_s):
                break

    def _run_periodic_tick(self) -> None:
        if self._stop_event.is_set():
            return
        pods = self._discover_pods()
        if not pods or self._stop_event.is_set():
            return
        with ThreadPoolExecutor(max_workers=len(pods)) as pool:
            futures = [pool.submit(self._scrape_pod_periodic, pod) for pod in pods]
            for fut in futures:
                fut.result()

    def _scrape_pod_periodic(self, pod: _TargetPod) -> None:
        windowed = self._windowed_profiles()
        if windowed and not self._stop_event.is_set():
            with ThreadPoolExecutor(max_workers=len(windowed)) as pool:
                futures = [
                    pool.submit(
                        self._scrape_profile,
                        pod,
                        prof,
                        final_gc=False,
                        deadline=None,
                        cancel_event=self._stop_event,
                    )
                    for prof in windowed
                ]
                for fut in futures:
                    fut.result()

        instant = self._instantaneous_profiles()
        if instant and not self._stop_event.is_set():
            with ThreadPoolExecutor(max_workers=len(instant)) as pool:
                futures = [
                    pool.submit(
                        self._scrape_profile,
                        pod,
                        prof,
                        final_gc=False,
                        deadline=None,
                        cancel_event=self._stop_event,
                    )
                    for prof in instant
                ]
                for fut in futures:
                    fut.result()

    def _scrape_instantaneous_phase(
        self, pods: list[_TargetPod], *, final_gc: bool, deadline: float
    ) -> None:
        instant = self._instantaneous_profiles()
        if not instant:
            return
        tasks = [(pod, prof) for pod in pods for prof in instant]
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            futures = [
                pool.submit(
                    self._scrape_profile,
                    pod,
                    prof,
                    final_gc=final_gc,
                    deadline=deadline,
                    cancel_event=None,
                )
                for pod, prof in tasks
            ]
            for fut in futures:
                fut.result()

    def _profile_request(self, profile: str, *, final_gc: bool) -> tuple[str, str, float]:
        if profile == "profile":
            return (
                f"/debug/pprof/profile?seconds={self._duration}",
                "profile",
                self._duration + READ_BUDGET_MARGIN_SECONDS,
            )
        if profile in ("allocs", "mutex", "block"):
            return (
                f"/debug/pprof/{profile}?seconds={self._duration}&debug=0",
                profile,
                self._duration + READ_BUDGET_MARGIN_SECONDS,
            )
        if profile == "heap":
            if final_gc:
                return "/debug/pprof/heap?gc=1", "heap_gc", READ_BUDGET_MARGIN_SECONDS
            return "/debug/pprof/heap?gc=0", "heap", READ_BUDGET_MARGIN_SECONDS
        return f"/debug/pprof/{profile}?debug=0", profile, READ_BUDGET_MARGIN_SECONDS

    def _scrape_profile(
        self,
        pod: _TargetPod,
        profile: str,
        *,
        final_gc: bool,
        deadline: float | None,
        cancel_event: threading.Event | None,
    ) -> None:
        if cancel_event is not None and cancel_event.is_set():
            return

        cpu_lock: threading.Lock | None = None
        if profile == "profile":
            cpu_lock = _get_cpu_lock(pod.uid)
            if not cpu_lock.acquire(blocking=False):
                with self._stats_mu:
                    self._scrapes_overlap_skipped += 1
                self._log(
                    f"Skipping overlapping CPU profile for {pod.namespace}/{pod.name}"
                )
                return

        try:
            self._perform_http_scrape(
                pod,
                profile,
                final_gc=final_gc,
                deadline=deadline,
                cancel_event=cancel_event,
            )
        finally:
            if cpu_lock is not None:
                cpu_lock.release()

    def _perform_http_scrape(
        self,
        pod: _TargetPod,
        profile: str,
        *,
        final_gc: bool,
        deadline: float | None,
        cancel_event: threading.Event | None,
    ) -> None:
        path, filename, read_budget = self._profile_request(profile, final_gc=final_gc)
        pod_dir = self._out_dir / pod.namespace / pod.name
        pod_dir.mkdir(parents=True, exist_ok=True)
        final_path = pod_dir / f"{_utc_ms_timestamp()}.{filename}.pb.gz"
        tmp_path = Path(f"{final_path}.tmp")

        conn = http.client.HTTPConnection(
            pod.ip, pod.pprof_port, timeout=CONNECT_TIMEOUT_SECONDS
        )
        conn.auto_open = 0
        registered = False
        try:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"phase deadline exceeded before scraping {path}")

            conn.connect()
            if cancel_event is not None:
                with self._conns_mu:
                    if cancel_event.is_set():
                        return
                    self._active_conns.add(conn)
                    registered = True

            read_timeout = read_budget
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"phase deadline exceeded before requesting {path}")
                read_timeout = min(remaining, read_budget)
            sock = conn.sock
            if sock is not None:
                sock.settimeout(read_timeout)

            if cancel_event is not None and cancel_event.is_set():
                return
            conn.request("GET", path)
            resp = conn.getresponse()

            if resp.status == 500:
                body = resp.read(ERROR_BODY_LIMIT_BYTES).decode("utf-8", errors="replace")
                if resp.getheader("X-Go-Pprof") == "1" and "already in use" in body.lower():
                    tmp_path.unlink(missing_ok=True)
                    with self._stats_mu:
                        self._scrapes_busy_skipped += 1
                    self._log(
                        f"Skipping busy pprof target {pod.namespace}/{pod.name} "
                        f"({path}): {body.strip()}"
                    )
                    return
                raise http.client.HTTPException(
                    f"HTTP 500 from {pod.namespace}/{pod.name} ({path}): {body.strip()}"
                )

            if resp.status != 200:
                resp.read(ERROR_BODY_LIMIT_BYTES)
                raise http.client.HTTPException(
                    f"HTTP {resp.status} from {pod.namespace}/{pod.name} ({path})"
                )

            written = 0
            with open(tmp_path, "wb") as out_f:
                while True:
                    chunk = resp.read(STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    out_f.write(chunk)
                    written += len(chunk)

            # Detect truncated Content-Length responses where HTTPResponse.read(n)
            # returns b"" on early EOF rather than raising IncompleteRead.
            if not resp.chunked and resp.length is not None and resp.length > 0:
                raise http.client.IncompleteRead(b"", resp.length)

            os.replace(tmp_path, final_path)
            with self._stats_mu:
                self._scrapes_succeeded += 1
                self._bytes_collected += written
                self._pods_profiled.add(pod.uid)
        except (OSError, http.client.HTTPException, TimeoutError, ValueError) as exc:
            tmp_path.unlink(missing_ok=True)
            if cancel_event is not None and cancel_event.is_set():
                return
            with self._stats_mu:
                self._scrapes_failed += 1
            self._log(
                f"Warning: failed scraping {pod.namespace}/{pod.name} {path}: {exc}"
            )
        finally:
            if registered:
                with self._conns_mu:
                    self._active_conns.discard(conn)
            try:
                conn.close()
            except OSError:
                pass


OBSERVERS["pprof"] = PprofObserver
