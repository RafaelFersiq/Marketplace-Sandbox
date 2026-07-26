"""Verification script for checking the health and connectivity of local containerized services.

This module reads configuration settings from environment variables (or root `.env` file)
and performs TCP socket and HTTP health checks for each service in the stack.
"""

import json
import logging
import os
import socket
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


# Configure structured logging in JSON format
class JsonFormatter(logging.Formatter):
    """Custom JSON log formatter for structured output auditability."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "service"):
            log_data["service"] = record.service
        if hasattr(record, "status"):
            log_data["status"] = record.status
        return json.dumps(log_data)


logger = logging.getLogger("service_verifier")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class ServiceCheck:
    """Dataclass representing a service check target."""

    name: str
    host: str
    port: int
    check_type: str  # "tcp" or "http"
    endpoint: str = ""
    expected_status: int = 200


def load_env_file(env_path: Path) -> None:
    """Parse a .env file and set missing environment variables.

    Args:
        env_path: Path to the .env file.
    """
    if not env_path.exists():
        logger.warning(f".env file not found at {env_path}, relying on system environment variables.")
        return

    try:
        with env_path.open(encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = value
    except OSError as err:
        logger.error(f"Failed to read .env file at {env_path}: {err}")


def check_tcp_port(host: str, port: int, timeout_sec: float = 3.0) -> bool:
    """Check if a TCP port is open and accepting connections.

    Args:
        host: Hostname or IP address.
        port: TCP port number.
        timeout_sec: Connection timeout in seconds.

    Returns:
        True if connection succeeded, False otherwise.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (TimeoutError, OSError):
        return False


def check_http_endpoint(url: str, expected_status: int = 200, timeout_sec: float = 3.0) -> bool:
    """Check if an HTTP endpoint returns the expected HTTP status code.

    Args:
        url: Full HTTP URL to check.
        expected_status: Expected HTTP status code (e.g. 200).
        timeout_sec: Request timeout in seconds.

    Returns:
        True if the response status matches expected status, False otherwise.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "ServiceVerifier/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            return response.status == expected_status
    except urllib.error.HTTPError as err:
        return err.code == expected_status
    except (TimeoutError, urllib.error.URLError, OSError):
        return False


def get_service_checks() -> list[ServiceCheck]:
    """Build the list of service checks from environment variables.

    Returns:
        List of ServiceCheck objects with targets and ports.
    """
    host = "localhost"

    postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
    minio_port = int(os.getenv("MINIO_PORT", "9000"))
    kafka_port = int(os.getenv("KAFKA_BROKER_PORT", "9092"))
    kafka_ui_port = int(os.getenv("KAFKA_UI_PORT", "8085"))
    schema_registry_port = int(os.getenv("SCHEMA_REGISTRY_PORT", "8081"))
    spark_master_port = int(os.getenv("SPARK_MASTER_PORT", "7077"))
    spark_ui_port = int(os.getenv("SPARK_UI_PORT", "8082"))
    airflow_port = int(os.getenv("AIRFLOW_WEBSERVER_PORT", "8080"))
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "9090"))
    grafana_port = int(os.getenv("GRAFANA_PORT", "3000"))

    return [
        ServiceCheck(name="PostgreSQL", host=host, port=postgres_port, check_type="tcp"),
        ServiceCheck(
            name="MinIO",
            host=host,
            port=minio_port,
            check_type="http",
            endpoint=f"http://{host}:{minio_port}/minio/health/live",
        ),
        ServiceCheck(name="Apache Kafka", host=host, port=kafka_port, check_type="tcp"),
        ServiceCheck(
            name="Kafka UI",
            host=host,
            port=kafka_ui_port,
            check_type="http",
            endpoint=f"http://{host}:{kafka_ui_port}/",
        ),
        ServiceCheck(
            name="Schema Registry",
            host=host,
            port=schema_registry_port,
            check_type="http",
            endpoint=f"http://{host}:{schema_registry_port}/subjects",
        ),
        ServiceCheck(name="Spark Master RPC", host=host, port=spark_master_port, check_type="tcp"),
        ServiceCheck(
            name="Spark UI",
            host=host,
            port=spark_ui_port,
            check_type="http",
            endpoint=f"http://{host}:{spark_ui_port}/",
        ),
        ServiceCheck(
            name="Airflow Webserver",
            host=host,
            port=airflow_port,
            check_type="http",
            endpoint=f"http://{host}:{airflow_port}/health",
        ),
        ServiceCheck(
            name="Prometheus",
            host=host,
            port=prometheus_port,
            check_type="http",
            endpoint=f"http://{host}:{prometheus_port}/-/healthy",
        ),
        ServiceCheck(
            name="Grafana",
            host=host,
            port=grafana_port,
            check_type="http",
            endpoint=f"http://{host}:{grafana_port}/api/health",
        ),
    ]


def run_verifications() -> bool:
    """Execute all configured service checks.

    Returns:
        True if all services are healthy, False if any service failed.
    """
    root_dir = Path(__file__).resolve().parent.parent
    load_env_file(root_dir / ".env")

    checks = get_service_checks()
    all_passed = True

    logger.info("Starting verification of local containerized services...")

    for check in checks:
        if check.check_type == "tcp":
            is_healthy = check_tcp_port(check.host, check.port)
        elif check.check_type == "http":
            is_healthy = check_http_endpoint(check.endpoint, check.expected_status)
        else:
            is_healthy = False

        status_str = "HEALTHY" if is_healthy else "UNHEALTHY"
        extra = {"service": check.name, "status": status_str}

        if is_healthy:
            logger.info(
                f"Service {check.name} on port {check.port} is operational.",
                extra=extra,
            )
        else:
            all_passed = False
            logger.error(
                f"Service {check.name} on port {check.port} failed health check.",
                extra=extra,
            )

    return all_passed


if __name__ == "__main__":
    success = run_verifications()
    if success:
        logger.info("All container services verified successfully.")
        sys.exit(0)
    else:
        logger.error("One or more container services failed verification.")
        sys.exit(1)
