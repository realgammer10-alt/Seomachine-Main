"""Configurable HTTP health checks and a small local status dashboard."""

import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


MAX_ENDPOINTS = 100
MAX_HISTORY = 20
MAX_WORKERS = 16
_ENDPOINT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class Endpoint:
    """An HTTP endpoint and the response codes considered healthy."""

    id: str
    name: str
    url: str
    method: str = "GET"
    expected_statuses: tuple[int, ...] = tuple(range(200, 400))


def _validate_number(value: Any, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number between {minimum} and {maximum}")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return float(value)


def _parse_endpoint(value: Any, index: int) -> Endpoint:
    label = f"endpoints[{index}]"
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")

    endpoint_id = value.get("id")
    if not isinstance(endpoint_id, str) or not _ENDPOINT_ID.fullmatch(endpoint_id):
        raise ValueError(
            f"{label}.id must start with a letter or number and contain at most "
            "64 letters, numbers, dots, underscores, or hyphens"
        )

    name = value.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 100:
        raise ValueError(f"{label}.name must contain 1 to 100 characters")

    url = value.get("url")
    if (
        not isinstance(url, str)
        or len(url) > 2048
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url)
    ):
        raise ValueError(f"{label}.url must be a valid HTTP or HTTPS URL")
    try:
        parsed_url = urlsplit(url)
        valid_port = parsed_url.port is None or 1 <= parsed_url.port <= 65535
    except ValueError:
        valid_port = False
        parsed_url = urlsplit("")
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.hostname
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
        or not valid_port
    ):
        raise ValueError(
            f"{label}.url must be an HTTP or HTTPS URL without whitespace, "
            "credentials, or a fragment"
        )

    method = value.get("method", "GET")
    if not isinstance(method, str) or method.upper() not in {"GET", "HEAD"}:
        raise ValueError(f"{label}.method must be GET or HEAD")

    statuses = value.get("expected_statuses", list(range(200, 400)))
    if (
        not isinstance(statuses, list)
        or not statuses
        or any(
            isinstance(status, bool)
            or not isinstance(status, int)
            or not 100 <= status <= 599
            for status in statuses
        )
    ):
        raise ValueError(
            f"{label}.expected_statuses must be a non-empty list of HTTP status codes"
        )
    if len(set(statuses)) != len(statuses):
        raise ValueError(f"{label}.expected_statuses must not contain duplicates")

    return Endpoint(
        id=endpoint_id,
        name=name.strip(),
        url=url,
        method=method.upper(),
        expected_statuses=tuple(statuses),
    )


def load_monitor_config(path: str | Path) -> "HealthMonitor":
    """Load and validate monitor settings from a JSON file."""
    config_path = Path(path)
    try:
        with config_path.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Invalid JSON in monitoring config {config_path}: {error.msg}"
        ) from error

    if not isinstance(config, dict):
        raise ValueError("Monitoring config must be a JSON object")
    interval = _validate_number(
        config.get("interval_seconds", 30), "interval_seconds", 1, 3600
    )
    timeout = _validate_number(
        config.get("timeout_seconds", 5), "timeout_seconds", 0.1, 60
    )
    endpoint_values = config.get("endpoints")
    if not isinstance(endpoint_values, list) or len(endpoint_values) > MAX_ENDPOINTS:
        raise ValueError(f"endpoints must be a list of at most {MAX_ENDPOINTS} items")

    endpoints = [
        _parse_endpoint(item, index) for index, item in enumerate(endpoint_values)
    ]
    endpoint_ids = [endpoint.id for endpoint in endpoints]
    if len(set(endpoint_ids)) != len(endpoint_ids):
        raise ValueError("Endpoint IDs must be unique")

    return HealthMonitor(endpoints, interval_seconds=interval, timeout_seconds=timeout)


class HealthMonitor:
    """Checks configured endpoints concurrently and retains bounded status history."""

    def __init__(
        self,
        endpoints: list[Endpoint],
        interval_seconds: float = 30,
        timeout_seconds: float = 5,
    ) -> None:
        if len(endpoints) > MAX_ENDPOINTS:
            raise ValueError(f"At most {MAX_ENDPOINTS} endpoints can be monitored")
        endpoint_ids = [endpoint.id for endpoint in endpoints]
        if len(set(endpoint_ids)) != len(endpoint_ids):
            raise ValueError("Endpoint IDs must be unique")

        self.endpoints = tuple(endpoints)
        self.interval_seconds = _validate_number(
            interval_seconds, "interval_seconds", 1, 3600
        )
        self.timeout_seconds = _validate_number(
            timeout_seconds, "timeout_seconds", 0.1, 60
        )
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._records: dict[str, dict[str, Any]] = {
            endpoint.id: {
                "id": endpoint.id,
                "name": endpoint.name,
                "status": "unknown",
                "checked_at": None,
                "latency_ms": None,
                "error": None,
                "history": [],
            }
            for endpoint in self.endpoints
        }

    def _check_endpoint(self, endpoint: Endpoint) -> dict[str, Any]:
        started = time.perf_counter()
        request = Request(endpoint.url, method=endpoint.method)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                status_code = response.getcode()
        except HTTPError as response:
            status_code = response.code
            response.close()
        except (URLError, TimeoutError, OSError, HTTPException) as error:
            reason = error.reason if isinstance(error, URLError) else error
            return {
                "status": "unhealthy",
                "checked_at": _utc_now(),
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                "error": f"Connection failed ({type(reason).__name__})",
            }

        healthy = status_code in endpoint.expected_statuses
        return {
            "status": "healthy" if healthy else "unhealthy",
            "checked_at": _utc_now(),
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
            "error": None if healthy else f"Unexpected HTTP status {status_code}",
        }

    def run_once(self) -> dict[str, Any]:
        """Check all endpoints once and return the resulting status snapshot."""
        if not self.endpoints:
            return self.snapshot()

        worker_count = min(len(self.endpoints), MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            results = list(executor.map(self._check_endpoint, self.endpoints))

        with self._lock:
            for endpoint, result in zip(self.endpoints, results):
                record = self._records[endpoint.id]
                record.update(result)
                record["history"].append(
                    {
                        "checked_at": result["checked_at"],
                        "status": result["status"],
                        "latency_ms": result["latency_ms"],
                        "error": result["error"],
                    }
                )
                record["history"] = record["history"][-MAX_HISTORY:]

        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return an isolated, JSON-serializable view of the latest monitor state."""
        with self._lock:
            services = [
                {
                    **record,
                    "history": [dict(item) for item in record["history"]],
                }
                for record in self._records.values()
            ]

        statuses = [service["status"] for service in services]
        if not statuses or all(status == "unknown" for status in statuses):
            overall_status = "unknown"
        else:
            healthy_count = statuses.count("healthy")
            unhealthy_count = statuses.count("unhealthy")
            if unhealthy_count == 0 and healthy_count == len(statuses):
                overall_status = "healthy"
            elif healthy_count == 0 and unhealthy_count:
                overall_status = "unhealthy"
            else:
                overall_status = "degraded"

        checked_times = [
            service["checked_at"] for service in services if service["checked_at"]
        ]
        return {
            "status": overall_status,
            "checked_at": max(checked_times) if checked_times else None,
            "interval_seconds": self.interval_seconds,
            "services": services,
        }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_once()
            if self._stop_event.wait(self.interval_seconds):
                break

    def start(self) -> None:
        """Start periodic checks in a background thread; repeated calls are safe."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="service-health-monitor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop periodic checks and wait for an in-flight check to finish."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join()
        self._thread = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


class MonitoringHTTPServer(ThreadingHTTPServer):
    """HTTP server exposing the dashboard and status endpoints."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self, server_address: tuple[str, int], monitor: HealthMonitor
    ) -> None:
        self.monitor = monitor
        dashboard_path = Path(__file__).resolve().parent.parent / "dashboard.html"
        self.dashboard_html = dashboard_path.read_text(encoding="utf-8")
        super().__init__(server_address, self._make_handler())

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        monitor = self.monitor
        dashboard_html = self.dashboard_html

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                path = urlsplit(self.path).path
                if path == "/":
                    self._send(
                        200,
                        dashboard_html.encode("utf-8"),
                        "text/html; charset=utf-8",
                    )
                elif path == "/api/status":
                    payload = json.dumps(monitor.snapshot()).encode("utf-8")
                    self._send(200, payload, "application/json; charset=utf-8")
                elif path == "/healthz":
                    status = monitor.snapshot()["status"]
                    code = 200 if status == "healthy" else 503
                    payload = json.dumps({"status": status}).encode("utf-8")
                    self._send(code, payload, "application/json; charset=utf-8")
                else:
                    self._send(
                        404,
                        b'{"error":"Not found"}',
                        "application/json; charset=utf-8",
                    )

            def _send(self, code: int, body: bytes, content_type: str) -> None:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)

        return Handler
