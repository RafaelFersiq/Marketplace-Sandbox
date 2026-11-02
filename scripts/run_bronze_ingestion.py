"""CLI entrypoint and orchestration runner for Medallion Bronze promotion ingestion.

Coordinates pre-flight connectivity verification against MinIO S3 object storage and
the PostgreSQL metadata catalog; scans the landing zone for landed micro-batches; enriches
records with canonical technical metadata envelopes (_bronze_id, _ingested_at, _source_topic,
_kafka_partition, _kafka_offset, _schema_id, _schema_version, _payload_hash, _processing_host);
commits immutable partitioned Parquet datasets; registers partitions and audit logs in
`lakehouse_metadata.bronze_partition_catalog` and `bronze_ingestion_audit`; and records
Prometheus metrics conforming strictly to ADR-0008.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Ensure project root is in sys.path for standalone script execution
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from processing.bronze.catalog import BronzeCatalogError, BronzeCatalogManager  # noqa: E402
from processing.bronze.config import (  # noqa: E402
    BronzeStorageConfig,
    build_bronze_arg_parser,
    parse_bronze_config,
)
from processing.bronze.enrichment import BronzeMetadataEnricher  # noqa: E402
from processing.bronze.metrics import (  # noqa: E402
    BronzeMetricsRegistry,
    get_bronze_metrics_registry,
)
from processing.bronze.writer import BronzeTableWriter, BronzeWriteResult  # noqa: E402

logger = logging.getLogger("bronze.ingestion.runner")


class JsonLogFormatter(logging.Formatter):
    """Structured JSON log formatter for Bronze ingestion audit logs."""

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
            headers={"User-Agent": "Marketplace-Bronze-Preflight"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if int(resp.status) < 500:
                return True
    except urllib.error.HTTPError as exc:
        if int(exc.code) < 500:
            return True
    except Exception as exc:
        logger.debug("MinIO HTTP health probe failed for %s: %s", target, exc)

    # Fallback to TCP probe
    try:
        parsed = urllib.parse.urlparse(target)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as exc:
        logger.debug("MinIO endpoint at %s unreachable: %s", endpoint, exc)
        return False


def check_postgres_connectivity(host: str, port: int, timeout: float = 3.0) -> bool:
    """Test TCP socket reachability to PostgreSQL metadata catalog service.

    Args:
        host: PostgreSQL catalog host.
        port: PostgreSQL catalog port.
        timeout: Socket connection timeout in seconds.

    Returns:
        True if PostgreSQL accepts TCP connection, False otherwise.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as exc:
        logger.debug("PostgreSQL catalog at %s:%d unreachable: %s", host, port, exc)
        return False


def run_preflight_checks(config: BronzeStorageConfig, timeout: float = 3.0) -> dict[str, bool]:
    """Execute pre-flight connectivity verification against MinIO and PostgreSQL catalog.

    Args:
        config: BronzeStorageConfig instance.
        timeout: Timeout per service check in seconds.

    Returns:
        Dictionary mapping service names to connectivity booleans.
    """
    results = {
        "minio": check_minio_connectivity(config.minio_endpoint, timeout=timeout),
        "postgres": check_postgres_connectivity(
            config.postgres_host, config.postgres_port, timeout=timeout
        ),
    }

    logger.info(
        "Pre-flight connectivity check results: MinIO=%s, PostgreSQL=%s",
        results["minio"],
        results["postgres"],
        extra={"event": "preflight_checks", "results": results},
    )
    return results


def build_ingestion_arg_parser() -> argparse.ArgumentParser:
    """Build CLI ArgumentParser extending base Bronze arguments with ingestion runner options.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    parser = build_bronze_arg_parser(
        description="Marketplace Medallion Bronze Layer Ingestion & Promotion Engine",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional ISO date filter (YYYY-MM-DD) to restrict promotion to a specific landing partition",
    )
    parser.add_argument(
        "--landing-format",
        type=str,
        default="parquet",
        choices=["parquet", "avro", "json"],
        help="Source landing micro-batch file serialization format",
    )
    parser.add_argument(
        "--deduplicate",
        action="store_true",
        default=False,
        help="Enable batch deduplication on deterministic SHA-256 _payload_hash column",
    )
    parser.add_argument(
        "--push-metrics",
        action="store_true",
        default=False,
        help="Push Prometheus telemetry to Pushgateway upon job completion",
    )
    parser.add_argument(
        "--pushgateway-url",
        type=str,
        default=os.getenv("PROMETHEUS_PUSHGATEWAY_URL", "http://localhost:9091"),
        help="Prometheus Pushgateway base URL",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        default=False,
        help="Skip pre-flight connectivity verification for MinIO and PostgreSQL",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Structured log level threshold",
    )
    return parser


class BronzeIngestionRunner:
    """Coordinates landing batch promotion, enrichment, partitioned writing, cataloging, and metrics."""

    def __init__(
        self,
        config: BronzeStorageConfig,
        spark: Any | None = None,
        catalog: BronzeCatalogManager | None = None,
        writer: BronzeTableWriter | None = None,
        enricher: BronzeMetadataEnricher | None = None,
        metrics_registry: BronzeMetricsRegistry | None = None,
        target_date: str | None = None,
        landing_format: str = "parquet",
        deduplicate: bool = False,
        push_metrics: bool = False,
        pushgateway_url: str = "http://localhost:9091",
        skip_preflight: bool = False,
    ) -> None:
        """Initialize BronzeIngestionRunner.

        Args:
            config: BronzeStorageConfig instance.
            spark: Optional injected SparkSession (for testing or container reuse).
            catalog: Optional injected BronzeCatalogManager.
            writer: Optional injected BronzeTableWriter.
            enricher: Optional injected BronzeMetadataEnricher.
            metrics_registry: Optional injected BronzeMetricsRegistry.
            target_date: Optional partition date filter (YYYY-MM-DD).
            landing_format: Landing file format ('parquet', 'avro', 'json').
            deduplicate: Enable batch deduplication by payload hash.
            push_metrics: Whether to publish metrics to Prometheus Pushgateway.
            pushgateway_url: Prometheus Pushgateway URL.
            skip_preflight: Whether to bypass pre-flight connectivity checks.
        """
        self.config = config
        self._spark = spark
        self.catalog = catalog or BronzeCatalogManager(config=self.config)
        self.writer = writer or BronzeTableWriter(config=self.config, spark=self.spark)
        self.enricher = enricher or BronzeMetadataEnricher()
        self.metrics_registry = metrics_registry or get_bronze_metrics_registry()
        self.target_date = target_date
        self.landing_format = landing_format
        self.deduplicate = deduplicate
        self.push_metrics = push_metrics
        self.pushgateway_url = pushgateway_url
        self.skip_preflight = skip_preflight

    @property
    def spark(self) -> Any:
        """Lazily initialize or retrieve active SparkSession."""
        if self._spark is not None:
            return self._spark

        try:
            from pyspark.sql import SparkSession

            active = SparkSession.getActiveSession()
            if active is not None:
                self._spark = active
                return self._spark

            builder = SparkSession.builder.appName(self.config.app_name).master(self.config.master)
            for k, v in self.config.to_spark_conf().items():
                builder = builder.config(k, v)
            self._spark = builder.getOrCreate()
            return self._spark
        except ImportError:
            logger.warning("PySpark not installed or JVM unavailable in environment.")
            return None

    def _resolve_landing_path(self, topic: str) -> str:
        """Resolve landing zone directory path for a specific topic and date.

        Args:
            topic: Kafka domain topic identifier.

        Returns:
            Resolved S3A or local filesystem path URI.
        """
        base = f"{self.config.landing_path.rstrip('/')}/{topic}"
        if self.target_date:
            return f"{base}/ingestion_date={self.target_date}"
        return base

    def promote_topic(self, topic: str, batch_id: str) -> BronzeWriteResult:
        """Promote landing micro-batches for a single topic into canonical Bronze Parquet storage.

        Args:
            topic: Kafka business topic.
            batch_id: Batch run UUID.

        Returns:
            BronzeWriteResult detailing record counts, partition paths, and write status.
        """
        start_time = time.monotonic()
        landing_source = self._resolve_landing_path(topic)

        logger.info(
            "Promoting landing batch for topic '%s' from %s",
            topic,
            landing_source,
            extra={
                "event": "bronze_promotion_start",
                "topic": topic,
                "batch_id": batch_id,
                "landing_source": landing_source,
                "format": self.landing_format,
            },
        )

        try:
            if self.spark is None:
                raise RuntimeError("Active SparkSession is required for promotion.")

            raw_df = self.spark.read.format(self.landing_format).load(landing_source)

            # Filter by date if target_date provided and date column exists
            if self.target_date and "ingestion_date" in raw_df.columns:
                from pyspark.sql import functions as F  # noqa: N812

                raw_df = raw_df.filter(F.col("ingestion_date") == self.target_date)

            # Check if source contains records
            record_count = int(raw_df.count())
            if record_count == 0:
                duration_ms = int((time.monotonic() - start_time) * 1000)
                logger.info(
                    "Landing source path %s is empty; skipping promotion.",
                    landing_source,
                    extra={"topic": topic, "batch_id": batch_id, "duration_ms": duration_ms},
                )
                return BronzeWriteResult(
                    batch_id=batch_id,
                    topic=topic,
                    record_count=0,
                    partitions_written=[],
                    bytes_written=0,
                    files_written=0,
                    status="SUCCESS",
                    duration_ms=duration_ms,
                )

            # Enrich with 10 technical metadata columns and partition keys
            enriched_df = self.enricher.enrich(raw_df, batch_id=batch_id, topic=topic)

            # Write partitioned Parquet dataset to canonical Bronze storage
            write_result = self.writer.write_batch(
                enriched_df,
                topic=topic,
                batch_id=batch_id,
                deduplicate_by_payload_hash=self.deduplicate,
            )

            # If write succeeded, register committed partitions and log audit entry in catalog
            if write_result.status == "SUCCESS" and not self.config.dry_run:
                self._register_catalog_and_audit(write_result, landing_source)

            # Record metrics in Prometheus registry
            self.metrics_registry.record_ingestion(
                record_count=write_result.record_count,
                bytes_written=write_result.bytes_written,
                duration_seconds=write_result.duration_ms / 1000.0,
                topic=topic,
                status=write_result.status,
            )

            return write_result

        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                "Failed to promote landing batch for topic '%s': %s",
                topic,
                exc,
                exc_info=True,
                extra={"topic": topic, "batch_id": batch_id, "error": str(exc)},
            )
            write_result = BronzeWriteResult(
                batch_id=batch_id,
                topic=topic,
                record_count=0,
                partitions_written=[],
                bytes_written=0,
                files_written=0,
                status="FAILED",
                duration_ms=duration_ms,
                error_message=str(exc),
            )
            self.metrics_registry.record_ingestion(
                record_count=0,
                bytes_written=0,
                duration_seconds=duration_ms / 1000.0,
                topic=topic,
                status="FAILED",
            )
            return write_result

    def _register_catalog_and_audit(
        self, write_result: BronzeWriteResult, landing_source: str
    ) -> None:
        """Register committed partitions and log batch audit record in PostgreSQL catalog."""
        try:
            for part_path in write_result.partitions_written:
                import re

                match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", part_path)
                if match:
                    y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
                else:
                    now = datetime.now(UTC)
                    y, m, d = now.year, now.month, now.day

                self.catalog.register_partition(
                    topic=write_result.topic,
                    year=y,
                    month=m,
                    day=d,
                    partition_path=part_path,
                    record_count=write_result.record_count,
                    file_count=write_result.files_written or 1,
                    total_bytes=write_result.bytes_written,
                    is_compacted=False,
                    status="COMMITTED",
                )

            target_part = (
                write_result.partitions_written[0]
                if write_result.partitions_written
                else self.config.get_topic_root_path(write_result.topic)
            )

            self.catalog.log_ingestion_audit(
                batch_id=write_result.batch_id,
                topic=write_result.topic,
                source_landing_path=landing_source,
                target_partition_path=target_part,
                records_ingested=write_result.record_count,
                bytes_written=write_result.bytes_written,
                files_written=write_result.files_written,
                records_failed=0,
                execution_duration_ms=write_result.duration_ms,
                status=write_result.status,
                error_message=write_result.error_message,
            )
        except BronzeCatalogError as cat_exc:
            logger.warning(
                "Catalog registration warning for batch %s: %s",
                write_result.batch_id,
                cat_exc,
                extra={"batch_id": write_result.batch_id, "error": str(cat_exc)},
            )

    def run(self) -> int:
        """Execute pre-flight checks, iterate over topics, execute promotion, and export metrics.

        Returns:
            Exit code: 0 on success, 1 on preflight or unrecoverable error.
        """
        logger.info(
            "Initializing Bronze ingestion runner for topics %s (dry_run=%s)",
            self.config.topics,
            self.config.dry_run,
            extra={
                "event": "runner_initializing",
                "topics": self.config.topics,
                "landing_path": self.config.landing_path,
                "bronze_root_path": self.config.bronze_root_path,
                "dry_run": self.config.dry_run,
                "deduplicate": self.deduplicate,
            },
        )

        # 1. Preflight connectivity checks
        if not self.skip_preflight:
            preflight = run_preflight_checks(self.config)
            if not all(preflight.values()):
                failed = [svc for svc, ok in preflight.items() if not ok]
                logger.error(
                    "Pre-flight connectivity check failed for: %s. Terminating runner.",
                    failed,
                    extra={"event": "preflight_failed", "failed_services": failed},
                )
                return 1

        # 2. Iterate across business domain topics
        batch_id = str(uuid.uuid4())
        results: list[BronzeWriteResult] = []
        for topic in self.config.topics:
            res = self.promote_topic(topic=topic, batch_id=batch_id)
            results.append(res)

        # 3. Push metrics to Prometheus Pushgateway if requested
        if self.push_metrics:
            try:
                self.metrics_registry.push_to_gateway(
                    pushgateway_url=self.pushgateway_url,
                    job_name="bronze_ingestion",
                )
            except Exception as push_err:
                logger.warning("Failed to push Prometheus metrics: %s", push_err)

        # 4. Log execution summary
        has_failures = any(r.status == "FAILED" for r in results)
        total_records = sum(r.record_count for r in results)
        total_bytes = sum(r.bytes_written for r in results)

        summary = {
            "batch_id": batch_id,
            "topics_processed": len(results),
            "total_records": total_records,
            "total_bytes": total_bytes,
            "status": "FAILED" if has_failures else "SUCCESS",
        }
        logger.info(
            "Bronze ingestion execution complete: %s",
            json.dumps(summary),
            extra={"event": "ingestion_complete", "summary": summary},
        )

        return 1 if has_failures else 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the marketplace Bronze ingestion pipeline.

    Args:
        argv: Optional list of CLI arguments (defaults to sys.argv[1:]).

    Returns:
        Process exit code: 0 on success, 1 on failure.
    """
    parser = build_ingestion_arg_parser()
    args = parser.parse_args(args=argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level)

    config = parse_bronze_config(parser=parser, args=argv)

    runner = BronzeIngestionRunner(
        config=config,
        target_date=args.date,
        landing_format=args.landing_format,
        deduplicate=args.deduplicate,
        push_metrics=args.push_metrics,
        pushgateway_url=args.pushgateway_url,
        skip_preflight=args.skip_preflight,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
