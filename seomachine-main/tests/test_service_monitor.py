import importlib.util
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_sources"
    / "modules"
    / "service_monitor.py"
)


def load_service_monitor_module():
    spec = importlib.util.spec_from_file_location(
        "service_monitor_under_test", MODULE_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def getcode(self):
        return self.status_code


class ServiceMonitorTests(unittest.TestCase):
    def setUp(self):
        self.module = load_service_monitor_module()

    def endpoint(self, endpoint_id, url=None, expected_statuses=(200,)):
        return self.module.Endpoint(
            id=endpoint_id,
            name=endpoint_id.title(),
            url=url or f"https://{endpoint_id}.example/health",
            expected_statuses=expected_statuses,
        )

    def config_file(self, config):
        temporary_directory = TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        path = Path(temporary_directory.name) / "monitoring.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    def test_healthy_response_records_latency_and_history(self):
        monitor = self.module.HealthMonitor([self.endpoint("website")])
        with patch.object(
            self.module, "urlopen", return_value=FakeResponse(200)
        ):
            status = monitor.run_once()

        self.assertEqual(status["status"], "healthy")
        service = status["services"][0]
        self.assertEqual(service["status"], "healthy")
        self.assertIsInstance(service["latency_ms"], float)
        self.assertIsNotNone(service["checked_at"])
        self.assertEqual(service["history"][0]["status"], "healthy")
        self.assertNotIn("url", service)

    def test_unexpected_status_and_connection_errors_are_reported(self):
        monitor = self.module.HealthMonitor(
            [self.endpoint("website", expected_statuses=(200,))]
        )
        with patch.object(
            self.module, "urlopen", return_value=FakeResponse(503)
        ):
            status = monitor.run_once()

        self.assertEqual(status["status"], "unhealthy")
        self.assertEqual(
            status["services"][0]["error"], "Unexpected HTTP status 503"
        )

        with patch.object(
            self.module,
            "urlopen",
            side_effect=URLError(TimeoutError("request timed out")),
        ):
            status = monitor.run_once()

        self.assertEqual(status["services"][0]["status"], "unhealthy")
        self.assertIn("TimeoutError", status["services"][0]["error"])

    def test_monitor_aggregates_mixed_endpoint_results_as_degraded(self):
        monitor = self.module.HealthMonitor(
            [self.endpoint("site"), self.endpoint("api")]
        )

        def response_for_url(request, timeout):
            return FakeResponse(200 if "site.example" in request.full_url else 503)

        with patch.object(
            self.module, "urlopen", side_effect=response_for_url
        ):
            status = monitor.run_once()

        self.assertEqual(status["status"], "degraded")
        self.assertEqual(
            [service["status"] for service in status["services"]],
            ["healthy", "unhealthy"],
        )

    def test_history_is_bounded_and_snapshots_are_isolated(self):
        monitor = self.module.HealthMonitor([self.endpoint("website")])
        with patch.object(
            self.module, "urlopen", return_value=FakeResponse(200)
        ):
            for _ in range(self.module.MAX_HISTORY + 2):
                monitor.run_once()

        snapshot = monitor.snapshot()
        self.assertEqual(
            len(snapshot["services"][0]["history"]), self.module.MAX_HISTORY
        )
        snapshot["services"][0]["history"].clear()
        self.assertEqual(
            len(monitor.snapshot()["services"][0]["history"]),
            self.module.MAX_HISTORY,
        )

    def test_background_monitor_runs_checks_until_stopped(self):
        monitor = self.module.HealthMonitor([self.endpoint("website")])
        checked = threading.Event()

        def check_endpoint(request, timeout):
            checked.set()
            return FakeResponse(200)

        with patch.object(self.module, "urlopen", side_effect=check_endpoint):
            monitor.start()
            try:
                self.assertTrue(checked.wait(2))
            finally:
                monitor.stop()

        self.assertEqual(monitor.snapshot()["status"], "healthy")

    def test_config_validation_rejects_duplicate_ids_and_unsafe_urls(self):
        duplicate = {
            "endpoints": [
                {"id": "api", "name": "API", "url": "https://api.example"},
                {"id": "api", "name": "Second API", "url": "https://other.example"},
            ]
        }
        with self.assertRaisesRegex(ValueError, "unique"):
            self.module.load_monitor_config(self.config_file(duplicate))

        invalid_url = {
            "endpoints": [
                {
                    "id": "api",
                    "name": "API",
                    "url": "https://user:password@api.example/health",
                }
            ]
        }
        with self.assertRaisesRegex(ValueError, "credentials"):
            self.module.load_monitor_config(self.config_file(invalid_url))

    def test_config_validation_rejects_invalid_intervals_and_status_codes(self):
        with self.assertRaisesRegex(ValueError, "interval_seconds"):
            self.module.load_monitor_config(
                self.config_file({"interval_seconds": True, "endpoints": []})
            )

        invalid_status = {
            "endpoints": [
                {
                    "id": "api",
                    "name": "API",
                    "url": "https://api.example",
                    "expected_statuses": [200, True],
                }
            ]
        }
        with self.assertRaisesRegex(ValueError, "expected_statuses"):
            self.module.load_monitor_config(self.config_file(invalid_status))

    def test_http_server_serves_dashboard_status_and_health_routes(self):
        monitor = self.module.HealthMonitor([])
        server = self.module.MonitoringHTTPServer(("127.0.0.1", 0), monitor)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}
        )
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        base_url = f"http://127.0.0.1:{server.server_port}"

        with urlopen(f"{base_url}/") as response:
            self.assertIn(b"Service monitor", response.read())

        with urlopen(f"{base_url}/api/status") as response:
            payload = json.loads(response.read())
        self.assertEqual(payload["status"], "unknown")
        self.assertEqual(payload["services"], [])
        with urlopen(f"{base_url}/api/status?refresh=1") as response:
            self.assertEqual(json.loads(response.read())["status"], "unknown")

        with self.assertRaises(HTTPError) as health_error:
            urlopen(f"{base_url}/healthz")
        self.assertEqual(health_error.exception.code, 503)
        health_error.exception.close()

        with self.assertRaises(HTTPError) as missing_route:
            urlopen(f"{base_url}/missing")
        self.assertEqual(missing_route.exception.code, 404)
        missing_route.exception.close()

    def test_health_endpoint_is_healthy_when_all_checks_pass(self):
        monitor = self.module.HealthMonitor([self.endpoint("website")])
        with patch.object(
            self.module, "urlopen", return_value=FakeResponse(200)
        ):
            monitor.run_once()

        server = self.module.MonitoringHTTPServer(("127.0.0.1", 0), monitor)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}
        )
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)

        with urlopen(f"http://127.0.0.1:{server.server_port}/healthz") as response:
            payload = json.loads(response.read())
        self.assertEqual(payload, {"status": "healthy"})


if __name__ == "__main__":
    unittest.main()
