"""Run the local SEO Machine service monitor and dashboard."""

import argparse
from pathlib import Path

from modules.service_monitor import HealthMonitor, MonitoringHTTPServer, load_monitor_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "data_sources" / "config" / "monitoring.json"


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor configured HTTP endpoints and serve a local dashboard."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"JSON configuration file (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Dashboard bind address (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=_port, default=8765, help="Dashboard port (default: 8765)"
    )
    args = parser.parse_args()

    try:
        monitor: HealthMonitor = load_monitor_config(args.config)
        server = MonitoringHTTPServer((args.host, args.port), monitor)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    monitor.start()
    print(f"SEO Machine monitor running at http://{args.host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping SEO Machine monitor.")
    finally:
        server.server_close()
        monitor.stop()


if __name__ == "__main__":
    main()
