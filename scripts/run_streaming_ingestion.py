"""CLI entrypoint and orchestration runner for PySpark Structured Streaming ingestion.

Coordinates pre-flight connectivity verification against Kafka brokers, Confluent Schema Registry,
and MinIO S3 object storage; configures Prometheus metrics listeners and structured JSON audit logging;
and launches the fault-tolerant StreamingIngestionPipeline under the StreamingRecoverySupervisor.
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path for standalone script execution
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from processing.streaming.config import (  # noqa: E402
    SparkStreamingConfig,
    build_streaming_arg_parser,
    parse_streaming_config,
)
from processing.streaming.metrics import (  # noqa: E402
    PrometheusMetricsRegistry,
    StreamingMetricsListener,
)
from processing.streaming.pipeline import StreamingIngestionPipeline  # noqa: E402
from processing.streaming.recovery import StreamingRecoverySupervisor  # noqa: E402

logger = logging.getLogger("streaming.runner")


class JsonLogFormatter(logging.Formatter):
    """Structured JSON log formatter for streaming ingestion audit logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a structured single-line JSON string."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include custom extra fields
        for key, value in record.__dict__.items():
            if key not in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            }:
                log_data[key] = value

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structured JSON logging handler on the root logger.

    Args:
        level: Minimum logging level threshold (defaults to logging.INFO).
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Avoid duplicate handlers if already attached
    if not any(isinstance(h.formatter, JsonLogFormatter) for h in root_logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonLogFormatter())
        handler.setLevel(level)
        root_logger.addHandler(handler)


def check_kafka_connectivity(bootstrap_servers: str, timeout: float = 3.0) -> bool:
    """Test TCP socket reachability to at least one Kafka broker endpoint.

    Args:
        bootstrap_servers: Comma-separated list of host:port broker endpoints.
        timeout: Socket connection timeout in seconds.

    Returns:
        True if at least one broker accepts TCP connection, False otherwise.
    """
    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    if not servers:
        return False

    for server in servers:
        try:
            if ":" in server:
                host, port_str = server.split(":", 1)
                port = int(port_str)
            else:
                host, port = server, 9092

            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception as exc:
            logger.debug("Kafka broker %s unreachable: %s", server, exc)
            continue

    return False


def check_schema_registry_connectivity(url: str, timeout: float = 3.0) -> bool:
    """Test HTTP reachability to Confluent Schema Registry.

    Args:
        url: Schema Registry base URL.
        timeout: Request timeout in seconds.

    Returns:
        True if Schema Registry responds with HTTP status < 500, False otherwise.
    """
    target_url = url.rstrip("/")
    if not target_url.startswith(("http://", "https://")):
        target_url = f"http://{target_url}"

    check_url = f"{target_url}/subjects"
    try:
        req = urllib.request.Request(check_url, headers={"User-Agent": "Marketplace-Streaming-Preflight"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status) < 500
    except urllib.error.HTTPError as exc:
        return int(exc.code) < 500
    except Exception as exc:
        logger.debug("Schema Registry at %s unreachable: %s", url, exc)
        return False


def check_minio_connectivity(endpoint: str, timeout: float = 3.0) -> bool:
    """Test HTTP or TCP reachability to MinIO / S3 object storage endpoint.

    Args:
        endpoint: MinIO endpoint URL or host:port.
        timeout: Connection timeout in seconds.

    Returns:
        True if endpoint responds or accepts TCP connection, False otherwise.
    """
    target = endpoint.rstrip("/")
    if target.startswith("s3a://"):
        target = target.replace("s3a://", "http://", 1)

    if not target.startswith(("http://", "https://")):
        target = f"http://{target}"

    # Try MinIO health endpoint first
    try:
        req = urllib.request.Request(
            f"{target}/minio/health/live",
            headers={"User-Agent": "Marketplace-Streaming-Preflight"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if int(resp.status) < 500:
                return True
    except urllib.error.HTTPError as exc:
        if int(exc.code) < 500:
            return True
    except Exception as exc:
        logger.debug("MinIO HTTP health probe failed for %s: %s", target, exc)

    # Fallback to general TCP probe
    try:
        parsed = urllib.parse.urlparse(target)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as exc:
        logger.debug("MinIO endpoint at %s unreachable: %s", endpoint, exc)
        return False


def run_preflight_checks(config: SparkStreamingConfig, timeout: float = 3.0) -> dict[str, bool]:
    """Execute pre-flight connectivity checks against Kafka, Schema Registry, and MinIO.

    Args:
        config: SparkStreamingConfig instance.
        timeout: Timeout per service check in seconds.

    Returns:
        Dictionary mapping service names to connectivity booleans.
    """
    results = {
        "kafka": check_kafka_connectivity(config.bootstrap_servers, timeout=timeout),
        "schema_registry": check_schema_registry_connectivity(config.schema_registry_url, timeout=timeout),
        "minio": check_minio_connectivity(config.minio_endpoint, timeout=timeout),
    }

    logger.info(
        "Pre-flight connectivity check results: Kafka=%s, SchemaRegistry=%s, MinIO=%s",
        results["kafka"],
        results["schema_registry"],
        results["minio"],
        extra={"event": "preflight_checks", "results": results},
    )
    return results


def build_runner_arg_parser() -> argparse.ArgumentParser:
    """Build CLI ArgumentParser extending base streaming arguments with runner options.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    parser = build_streaming_arg_parser(
        description="Marketplace PySpark Structured Streaming Ingestion Pipeline Runner",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        default=False,
        help="Skip pre-flight connectivity checks for Kafka, Schema Registry, and MinIO",
    )
    parser.add_argument(
        "--disable-dlq-kafka",
        action="store_true",
        default=False,
        help="Disable Dead Letter Queue event publishing to Kafka DLQ topic",
    )
    parser.add_argument(
        "--disable-dlq-storage",
        action="store_true",
        default=False,
        help="Disable Dead Letter Queue event archiving to MinIO object storage",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum supervisor automatic recovery restarts before fatal termination",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Structured log level threshold",
    )
    return parser


class StreamingIngestionRunner:
    """Coordinates preflight checks, pipeline instantiation, recovery supervisor, and execution."""

    def __init__(
        self,
        config: SparkStreamingConfig,
        skip_preflight: bool = False,
        enable_dlq_kafka: bool = True,
        enable_dlq_storage: bool = True,
        max_retries: int | None = None,
        pipeline: StreamingIngestionPipeline | None = None,
        supervisor: StreamingRecoverySupervisor | None = None,
        metrics_registry: PrometheusMetricsRegistry | None = None,
    ) -> None:
        """Initialize StreamingIngestionRunner.

        Args:
            config: SparkStreamingConfig instance.
            skip_preflight: Whether to bypass preflight connectivity verification.
            enable_dlq_kafka: Enable publishing malformed payloads to Kafka DLQ.
            enable_dlq_storage: Enable persisting malformed payloads to MinIO DLQ storage.
            max_retries: Optional override for supervisor restart retries.
            pipeline: Optional injected StreamingIngestionPipeline (for testing).
            supervisor: Optional injected StreamingRecoverySupervisor (for testing).
            metrics_registry: Optional injected PrometheusMetricsRegistry (for testing).
        """
        self.config = config
        self.skip_preflight = skip_preflight
        self.enable_dlq_kafka = enable_dlq_kafka
        self.enable_dlq_storage = enable_dlq_storage

        self.metrics_registry = metrics_registry or PrometheusMetricsRegistry()
        self.metrics_listener = StreamingMetricsListener(metrics_registry=self.metrics_registry)

        self.pipeline = pipeline or StreamingIngestionPipeline(
            config=self.config,
            metrics_listener=self.metrics_listener,
            enable_dlq_kafka=self.enable_dlq_kafka,
            enable_dlq_storage=self.enable_dlq_storage,
        )

        effective_max_retries = max_retries if max_retries is not None else self.config.max_retries
        self.supervisor = supervisor or StreamingRecoverySupervisor(
            config=self.config,
            max_retries=effective_max_retries,
        )

    def run(self) -> int:
        """Execute pre-flight checks, start streaming pipeline with supervisor, and report metrics.

        Returns:
            Exit code: 0 for clean execution, 1 for preflight or fatal runtime error.
        """
        logger.info(
            "Initializing streaming ingestion runner for app '%s' (master: %s, mode: %s)",
            self.config.app_name,
            self.config.master,
            "once" if self.config.once else "continuous",
            extra={
                "event": "runner_initializing",
                "app_name": self.config.app_name,
                "master": self.config.master,
                "topics": self.config.topics,
                "once": self.config.once,
                "enable_dlq_kafka": self.enable_dlq_kafka,
                "enable_dlq_storage": self.enable_dlq_storage,
            },
        )

        # 1. Preflight connectivity checks
        if not self.skip_preflight:
            preflight_results = run_preflight_checks(self.config)
            if not all(preflight_results.values()):
                failed_services = [svc for svc, ok in preflight_results.items() if not ok]
                logger.error(
                    "Pre-flight connectivity checks failed for services: %s. Terminating runner.",
                    failed_services,
                    extra={"event": "preflight_failed", "failed_services": failed_services},
                )
                return 1

        # 2. Run streaming queries under recovery supervisor
        try:
            exit_code = self.supervisor.run_with_recovery(self.pipeline.start_queries)
        except Exception as exc:
            logger.critical(
                "Fatal error in streaming recovery supervisor: %s",
                exc,
                extra={"event": "supervisor_fatal_error", "error": str(exc)},
            )
            return 1
        finally:
            self.pipeline.stop_queries()

        # 3. Log completion summary and Prometheus metrics
        prom_text = self.metrics_registry.format_prometheus_text()
        logger.info(
            "Streaming ingestion run complete (exit_code=%d). Metrics snapshot emitted.",
            exit_code,
            extra={
                "event": "runner_complete",
                "exit_code": exit_code,
                "restarts": self.supervisor.metrics.total_restarts,
                "successful_runs": self.supervisor.metrics.successful_runs,
                "failed_runs": self.supervisor.metrics.failed_runs,
                "elapsed_seconds": self.supervisor.metrics.elapsed_seconds,
            },
        )
        if prom_text.strip():
            logger.debug("Prometheus text export:\n%s", prom_text)

        return exit_code


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the marketplace streaming ingestion pipeline.

    Args:
        argv: Optional list of CLI arguments (defaults to sys.argv[1:]).

    Returns:
        Process exit code: 0 on success, 1 on failure.
    """
    parser = build_runner_arg_parser()
    args = parser.parse_args(args=argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level)

    config = parse_streaming_config(parser=parser, args=argv)

    runner = StreamingIngestionRunner(
        config=config,
        skip_preflight=args.skip_preflight,
        enable_dlq_kafka=not args.disable_dlq_kafka,
        enable_dlq_storage=not args.disable_dlq_storage,
        max_retries=args.max_retries,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
