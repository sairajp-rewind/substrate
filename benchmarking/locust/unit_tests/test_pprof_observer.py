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

"""Unit tests for common.observers.pprof.PprofObserver."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.observers import OBSERVERS, PprofObserver


def _make_pod(
    *,
    name: str = "pod-a",
    namespace: str = "ate-system",
    uid: str = "uid-a",
    phase: str = "Running",
    ready: str = "True",
    pod_ip: str = "127.0.0.1",
    pprof_port: int | None = 6060,
    container_name: str = "app",
    restart_count: int = 0,
):
    ports = []
    if pprof_port is not None:
        ports.append(SimpleNamespace(name="pprof", container_port=pprof_port))
    conditions = (
        [] if ready is None else [SimpleNamespace(type="Ready", status=ready)]
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace, uid=uid),
        status=SimpleNamespace(
            phase=phase,
            conditions=conditions,
            pod_ip=pod_ip,
            container_statuses=[
                SimpleNamespace(name=container_name, restart_count=restart_count)
            ],
        ),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name=container_name, ports=ports)]
        ),
    )


def _start_server(handler_cls):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1], thread


def _stop_server(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


class PprofObserverConfigTest(unittest.TestCase):
    def test_registered_in_observers_map(self):
        self.assertIs(OBSERVERS["pprof"], PprofObserver)

    def test_defaults_applied_without_pods(self):
        api = mock.Mock()
        api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[])
        with tempfile.TemporaryDirectory() as tmp:
            obs = PprofObserver(core_v1_factory=lambda: api)
            obs.start("cp_pprof", {}, Path(tmp))
            res = obs.final_observation()

            self.assertEqual(obs._namespace, "")
            self.assertEqual(obs._label_selector, "")
            self.assertEqual(obs._profiles, ["profile", "allocs", "heap", "goroutine"])
            self.assertEqual(obs._interval, 60)
            self.assertEqual(obs._duration, 15)
            self.assertEqual(res.status, "error")
            self.assertEqual(res.error, "no pods profiled")
            self.assertEqual(res.counts["pods_discovered"], 0)
            self.assertEqual(res.counts["pods_profiled"], 0)
            self.assertTrue(
                "no Ready pods with a pprof port matched"
                in (Path(tmp) / "cp_pprof.log").read_text(encoding="utf-8")
            )

    def test_invalid_configs_rejected(self):
        bad_cases = [
            ("unknown key", {"bogus": 1}, "unknown config keys"),
            ("non-str namespace", {"namespace": 123}, "namespace must be a string"),
            ("non-str selector", {"label_selector": ["a"]}, "label_selector must be a string"),
            ("empty profiles", {"profiles": []}, "non-empty list"),
            ("string profiles", {"profiles": "heap"}, "non-empty list"),
            ("trace profile", {"profiles": ["heap", "trace"]}, "trace"),
            ("unknown profile", {"profiles": ["cpu"]}, "unknown profile"),
            ("duration zero", {"duration": "0s"}, ">= 1s"),
            ("duration negative", {"duration": -5}, ">= 1s"),
            ("duration exceeds interval", {"duration": "30s", "interval": "15s"}, "must not exceed interval"),
            ("float duration", {"duration": 1.5}, "integer or <int>\\[smh\\]"),
            ("float string duration", {"duration": "1.5s"}, "invalid duration"),
            ("bool duration", {"duration": True}, "integer or <int>\\[smh\\]"),
            ("bad unit", {"interval": "5d"}, "invalid duration"),
        ]
        for label, cfg, pattern in bad_cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                api = mock.Mock()
                obs = PprofObserver(core_v1_factory=lambda: api)
                with self.assertRaisesRegex(ValueError, pattern):
                    obs.start("bad", cfg, Path(tmp))
                api.list_pod_for_all_namespaces.assert_not_called()
                api.list_namespaced_pod.assert_not_called()


class PprofObserverDiscoveryTest(unittest.TestCase):
    def test_filters_ineligible_pods_and_supports_scope_and_restarts(self):
        ready_good = _make_pod(name="api-0", uid="uid-good", pprof_port=6060, restart_count=0)
        pending_pod = _make_pod(name="pending", uid="uid-p", phase="Pending")
        unready_pod = _make_pod(name="unready", uid="uid-u", ready="False")
        no_ip_pod = _make_pod(name="noip", uid="uid-ip", pod_ip="")
        postgres_pod = _make_pod(name="postgres", uid="uid-pg", pprof_port=None)

        restarted_good = _make_pod(name="api-0", uid="uid-good", pprof_port=6060, restart_count=2)

        api = mock.Mock()
        api.list_namespaced_pod.side_effect = [
            SimpleNamespace(items=[ready_good, pending_pod, unready_pod, no_ip_pod, postgres_pod]),
            SimpleNamespace(items=[restarted_good]),
        ]
        api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[ready_good])

        with tempfile.TemporaryDirectory() as tmp:
            obs = PprofObserver(core_v1_factory=lambda: api)
            obs._validate_and_configure(
                "cp",
                {"namespace": "ate-system", "label_selector": "app=ate-api-server"},
                Path(tmp),
            )
            obs._log_path = Path(tmp) / "cp.log"
            obs._core_v1 = api

            pods_first = obs._discover_pods()
            self.assertEqual([p.uid for p in pods_first], ["uid-good"])
            api.list_namespaced_pod.assert_called_with(
                namespace="ate-system",
                label_selector="app=ate-api-server",
            )

            pods_second = obs._discover_pods()
            self.assertEqual([p.uid for p in pods_second], ["uid-good"])
            log_text = (Path(tmp) / "cp.log").read_text(encoding="utf-8")
            self.assertIn("restarted (0 -> 2)", log_text)
            self.assertIn("ate-system/api-0", log_text)

            # All-namespaces mode when namespace is empty.
            obs_all = PprofObserver(core_v1_factory=lambda: api)
            obs_all._validate_and_configure("all", {"label_selector": "tier=core"}, Path(tmp))
            obs_all._log_path = Path(tmp) / "all.log"
            obs_all._core_v1 = api
            pods_all = obs_all._discover_pods()
            self.assertEqual([p.uid for p in pods_all], ["uid-good"])
            api.list_pod_for_all_namespaces.assert_called_once_with(
                label_selector="tier=core",
            )


class PprofObserverHTTPScrapingTest(unittest.TestCase):
    def test_baseline_periodic_final_lifecycle_writes_atomic_files(self):
        seen_paths: list[str] = []
        seen_mu = threading.Lock()
        heap_count = 0
        tick_windowed_done = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal heap_count
                payload = f"pprof:{self.path}".encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                self.wfile.flush()
                with seen_mu:
                    seen_paths.append(self.path)
                    if self.path == "/debug/pprof/heap?gc=0":
                        heap_count += 1
                        # Baseline calls heap?gc=0 once; the 2nd call starts after
                        # periodic tick 1's windowed profiles have completed.
                        if heap_count >= 2:
                            tick_windowed_done.set()

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(
                name="api-server-0",
                namespace="ate-system",
                uid="uid-lifecycle-test",
                pprof_port=port,
            )
            api = mock.Mock()
            api.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs.start(
                    "cp",
                    {
                        "namespace": "ate-system",
                        "profiles": ["profile", "allocs", "heap", "goroutine"],
                        "interval": "1s",
                        "duration": "1s",
                    },
                    out_dir,
                )
                self.assertTrue(
                    tick_windowed_done.wait(timeout=5),
                    "periodic scrape did not complete windowed phase and enter instantaneous phase",
                )
                res = obs.final_observation()

                self.assertEqual(res.status, "ok")
                self.assertEqual(res.counts["pods_discovered"], 1)
                self.assertEqual(res.counts["pods_profiled"], 1)
                self.assertEqual(res.counts["scrapes_failed"], 0)
                self.assertGreaterEqual(res.counts["scrapes_succeeded"], 4)
                self.assertGreater(res.counts["bytes_collected"], 0)

                self.assertIn("/debug/pprof/heap?gc=0", seen_paths)
                self.assertIn("/debug/pprof/goroutine?debug=0", seen_paths)
                self.assertIn("/debug/pprof/profile?seconds=1", seen_paths)
                self.assertIn("/debug/pprof/allocs?seconds=1&debug=0", seen_paths)
                self.assertIn("/debug/pprof/heap?gc=1", seen_paths)

                pod_dir = out_dir / "ate-system" / "api-server-0"
                files = [p.name for p in pod_dir.iterdir()]
                self.assertTrue(any(f.endswith(".heap.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".goroutine.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".profile.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".allocs.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".heap_gc.pb.gz") for f in files), files)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
        finally:
            _stop_server(server, srv_thread)

    def test_module_cpu_lock_skips_overlapping_profile_across_instances(self):
        first_in_handler = threading.Event()
        release_first = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                first_in_handler.set()
                release_first.wait(timeout=5)
                payload = b"cpu-profile"
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            shared_uid = "uid-cpu-lock-shared"
            pod = _make_pod(name="router-0", uid=shared_uid, pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
                obs1 = PprofObserver(core_v1_factory=lambda: api)
                obs2 = PprofObserver(core_v1_factory=lambda: api)

                obs1._validate_and_configure("o1", {"profiles": ["profile"], "duration": "1s", "interval": "60s"}, Path(tmp1))
                obs1._log_path = Path(tmp1) / "o1.log"
                obs1._core_v1 = api

                obs2._validate_and_configure("o2", {"profiles": ["profile"], "duration": "1s", "interval": "60s"}, Path(tmp2))
                obs2._log_path = Path(tmp2) / "o2.log"
                obs2._core_v1 = api

                target = obs1._discover_pods()[0]
                t1 = threading.Thread(
                    target=obs1._scrape_profile,
                    args=(target, "profile"),
                    kwargs={"final_gc": False, "deadline": None, "cancel_event": None},
                )
                t1.start()
                self.assertTrue(first_in_handler.wait(timeout=5))

                # Second instance attempts CPU profile while first holds _CPU_LOCKS[uid].
                obs2._discover_pods()
                obs2._scrape_profile(
                    target, "profile", final_gc=False, deadline=None, cancel_event=None
                )

                release_first.set()
                t1.join(timeout=5)

                c1 = obs1._snapshot_counts()
                c2 = obs2._snapshot_counts()
                self.assertEqual(c1["scrapes_succeeded"], 1)
                self.assertEqual(c1["scrapes_overlap_skipped"], 0)
                self.assertEqual(c2["scrapes_succeeded"], 0)
                self.assertEqual(c2["scrapes_overlap_skipped"], 1)
        finally:
            release_first.set()
            _stop_server(server, srv_thread)

    def test_http_500_busy_vs_503_and_truncated_body(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.startswith("/debug/pprof/profile"):
                    body = b"Could not enable CPU profiling: cpu profiling already in use\n"
                    self.send_response(500)
                    self.send_header("X-Go-Pprof", "1")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path.startswith("/debug/pprof/allocs"):
                    body = b"unexpected internal error without pprof header\n"
                    self.send_response(500)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path.startswith("/debug/pprof/goroutine"):
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif self.path.startswith("/debug/pprof/heap"):
                    # Declare 1024 bytes, write only a short fragment, then drop TCP.
                    self.send_response(200)
                    self.send_header("Content-Length", "1024")
                    self.end_headers()
                    self.wfile.write(b"partial-data")
                    self.wfile.flush()
                    self.close_connection = True

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(name="worker-0", uid="uid-failures-test", pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs._validate_and_configure(
                    "errtest",
                    {"profiles": ["profile", "allocs", "heap", "goroutine"], "duration": "1s", "interval": "60s"},
                    out_dir,
                )
                obs._log_path = out_dir / "errtest.log"
                obs._core_v1 = api
                target = obs._discover_pods()[0]

                obs._scrape_profile(target, "profile", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "allocs", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "goroutine", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "heap", final_gc=False, deadline=None, cancel_event=None)

                counts = obs._snapshot_counts()
                self.assertEqual(counts["scrapes_busy_skipped"], 1)
                self.assertEqual(counts["scrapes_failed"], 3)
                self.assertEqual(counts["scrapes_succeeded"], 0)
                self.assertEqual(counts["pods_discovered"], 1)
                self.assertEqual(counts["pods_profiled"], 0)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
                self.assertEqual(list(out_dir.rglob("*.pb.gz")), [])
        finally:
            _stop_server(server, srv_thread)

    def test_final_observation_cancels_slow_periodic_profile_without_counting_failure(self):
        slow_profile_started = threading.Event()
        release_slow_handler = threading.Event()
        paths: list[str] = []
        paths_mu = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                with paths_mu:
                    paths.append(self.path)
                if self.path.startswith("/debug/pprof/profile?seconds=10"):
                    slow_profile_started.set()
                    release_slow_handler.wait(timeout=15)
                    return
                payload = f"ok:{self.path}".encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(name="api-slow", uid="uid-cancel-test", pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs.start(
                    "cancel_obs",
                    {"profiles": ["profile", "heap"], "duration": "10s", "interval": "60s"},
                    out_dir,
                )
                self.assertTrue(
                    slow_profile_started.wait(timeout=5),
                    "periodic scrape did not start slow profile",
                )

                # Must abort active periodic connection immediately and complete heap?gc=1.
                t0 = time.monotonic()
                res = obs.final_observation()
                elapsed = time.monotonic() - t0
                self.assertLess(elapsed, 2.0, f"final_observation took {elapsed:.2f}s")
                self.assertEqual(res.status, "ok")
                self.assertEqual(res.counts["scrapes_failed"], 0)
                self.assertEqual(res.counts["pods_profiled"], 1)
                self.assertIn("/debug/pprof/heap?gc=0", paths)
                self.assertIn("/debug/pprof/heap?gc=1", paths)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
                self.assertTrue(
                    any(p.name.endswith(".heap_gc.pb.gz") for p in out_dir.rglob("*.pb.gz"))
                )
        finally:
            release_slow_handler.set()
            _stop_server(server, srv_thread)


if __name__ == "__main__":
    unittest.main()
