"""CLI entrypoint and orchestration runner for Medallion Bronze small-file compaction.

Discovers fragmented small-file partitions in the canonical Bronze layer via
`lakehouse_metadata.bronze_partition_catalog` or direct storage scanning; executes atomic
coalescing into optimal target file sizes (default 128 MB) via `BronzeCompactor`; records
compaction status and updated file counts in PostgreSQL; and logs structured telemetry
conforming strictly to ADR-0008.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
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

from processing.bronze.catalog import BronzeCatalogError, BronzeCatalogManager  # noqa: E402
from processing.bronze.compaction import (  # noqa: E402
    BronzeCompactionResult,
    BronzeCompactor,
)
from processing.bronze.config import (  # noqa: E402
    BronzeStorageConfig,
    build_bronze_arg_parser,
    parse_bronze_config,
)
from processing.bronze.metrics import (  # noqa: E402
    BronzeMetricsRegistry,
    get_bronze_metrics_registry,
)

logger = logging.getLogger("bronze.compaction.runner")


class JsonLogFormatter(logging.Formatter):
    """Structured JSON log formatter for Bronze compaction audit logs."""

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
    """Test HTTP or TCP reachability to MinIO / S3 object storage endpoint."""
    target = endpoint.rstrip("/")
    if target.startswith("s3a://"):
        target = target.replace("s3a://", "http://", 1)

    if not target.startswith(("http://", "https://")):
        target = f"http://{target}"

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
    """Test TCP socket reachability to PostgreSQL metadata catalog service."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as exc:
        logger.debug("PostgreSQL catalog at %s:%d unreachable: %s", host, port, exc)
        return False


def run_preflight_checks(config: BronzeStorageConfig, timeout: float = 3.0) -> dict[str, bool]:
    """Execute pre-flight connectivity verification against MinIO and PostgreSQL catalog."""
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


def build_compaction_arg_parser() -> argparse.ArgumentParser:
    """Build CLI ArgumentParser extending base Bronze arguments with compaction runner options."""
    parser = build_bronze_arg_parser(
        description="Marketplace Medallion Bronze Layer Small-File Compaction Engine",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional ISO date filter (YYYY-MM-DD) to restrict compaction to a specific partition date",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force small-file compaction bypassing minimum file count and size threshold checks",
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


class BronzeCompactionRunner:
    """Coordinates partition fragmentation discovery, atomic compaction, catalog updates, and metrics."""

    def __init__(
        self,
        config: BronzeStorageConfig,
        spark: Any | None = None,
        catalog: BronzeCatalogManager | None = None,
        compactor: BronzeCompactor | None = None,
        metrics_registry: BronzeMetricsRegistry | None = None,
        target_date: str | None = None,
        force: bool = False,
        push_metrics: bool = False,
        pushgateway_url: str = "http://localhost:9091",
        skip_preflight: bool = False,
    ) -> None:
        """Initialize BronzeCompactionRunner.

        Args:
            config: BronzeStorageConfig instance.
            spark: Optional injected SparkSession.
            catalog: Optional injected BronzeCatalogManager.
            compactor: Optional injected BronzeCompactor.
            metrics_registry: Optional injected BronzeMetricsRegistry.
            target_date: Optional partition date filter (YYYY-MM-DD).
            force: Force compaction bypassing threshold checks.
            push_metrics: Whether to publish metrics to Prometheus Pushgateway.
            pushgateway_url: Prometheus Pushgateway URL.
            skip_preflight: Whether to bypass pre-flight connectivity checks.
        """
        self.config = config
        self._spark = spark
        self.catalog = catalog or BronzeCatalogManager(config=self.config)
        self.compactor = compactor or BronzeCompactor(config=self.config, spark=self._spark)
        self.metrics_registry = metrics_registry or get_bronze_metrics_registry()
        self.target_date = target_date
        self.force = force
        self.push_metrics = push_metrics
        self.pushgateway_url = pushgateway_url
        self.skip_preflight = skip_preflight

    def _discover_candidate_partitions(self) -> list[dict[str, Any]]:
        """Discover candidate uncompacted partitions using the catalog with storage fallback."""
        candidates: list[dict[str, Any]] = []

        # 1. Try PostgreSQL Catalog Discovery
        try:
            min_files = 0 if self.force else self.config.compaction_threshold_file_count
            for topic in self.config.topics:
                uncompacted = self.catalog.get_uncompacted_partitions(
                    topic=topic, min_file_count=min_files
                )
                candidates.extend(uncompacted)

            if candidates:
                logger.info(
                    "Discovered %d uncompacted partitions via catalog",
                    len(candidates),
                    extra={"event": "catalog_discovery_success", "count": len(candidates)},
                )
        except BronzeCatalogError as cat_err:
            logger.warning(
                "Catalog partition discovery failed; falling back to storage scanning: %s",
                cat_err,
                extra={"error": str(cat_err)},
            )

        # 2. Fallback to physical storage discovery if catalog returned nothing
        if not candidates:
            logger.info("Scanning physical Bronze storage for uncompacted partitions...")
            candidates = self.compactor.discover_partitions()

        # 3. Filter by target date if specified
        if self.target_date:

            date_parts = self.target_date.split("-")
            if len(date_parts) == 3:
                y, m, d = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                date_subpath = f"year={y:04d}/month={m:02d}/day={d:02d}"
                candidates = [
                    p for p in candidates if date_subpath in str(p.get("partition_path", ""))
                ]

        return candidates

    def run(self) -> int:
        """Execute pre-flight checks, discover candidate partitions, compact, and report metrics.

        Returns:
            Exit code: 0 on success, 1 on preflight or unrecoverable error.
        """
        logger.info(
            "Initializing Bronze compaction runner for topics %s (force=%s, dry_run=%s)",
            self.config.topics,
            self.force,
            self.config.dry_run,
            extra={
                "event": "runner_initializing",
                "topics": self.config.topics,
                "target_file_size_mb": self.config.target_file_size_mb,
                "force": self.force,
                "dry_run": self.config.dry_run,
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

        # 2. Discover uncompacted partitions
        candidates = self._discover_candidate_partitions()
        if not candidates:
            logger.info("No partitions qualified for compaction. Job complete.")
            return 0

        # 3. Execute compaction on candidate partitions
        results: list[BronzeCompactionResult] = []
        for cand in candidates:
            path = str(cand.get("partition_path", ""))
            topic = str(cand.get("topic", "unknown"))

            res = self.compactor.compact_partition(
                partition_path=path,
                target_file_size_mb=self.config.target_file_size_mb,
                topic=topic,
                force=self.force,
            )
            results.append(res)

            # If compaction succeeded and not dry-run, mark compacted in catalog
            if res.status == "SUCCESS" and not self.config.dry_run:
                try:
                    self.catalog.mark_partition_compacted(
                        partition_path=path,
                        file_count=res.files_after,
                        total_bytes=res.bytes_after,
                    )
                except BronzeCatalogError as cat_err:
                    logger.warning(
                        "Failed to update catalog compaction status for %s: %s",
                        path,
                        cat_err,
                        extra={"partition_path": path, "error": str(cat_err)},
                    )

            # Record telemetry in Prometheus registry
            self.metrics_registry.record_compaction(
                files_reduced=res.files_reduced,
                bytes_saved=res.bytes_reduced,
                duration_seconds=res.duration_ms / 1000.0,
                topic=res.topic,
                status=res.status,
            )
            self.metrics_registry.record_partition_gauge(
                topic=res.topic,
                partition=res.partition_path,
                file_count=res.files_after,
                bytes_total=res.bytes_after,
            )

        # 4. Push metrics to Prometheus Pushgateway if requested
        if self.push_metrics:
            try:
                self.metrics_registry.push_to_gateway(
                    pushgateway_url=self.pushgateway_url,
                    job_name="bronze_compaction",
                )
            except Exception as push_err:
                logger.warning("Failed to push Prometheus metrics: %s", push_err)

        # 5. Log execution summary
        has_failures = any(r.status == "FAILED" for r in results)
        total_files_reduced = sum(r.files_reduced for r in results)
        total_bytes_saved = sum(r.bytes_reduced for r in results)

        summary = {
            "partitions_evaluated": len(results),
            "successful": sum(1 for r in results if r.status == "SUCCESS"),
            "skipped": sum(1 for r in results if r.status.startswith("SKIPPED")),
            "failed": sum(1 for r in results if r.status == "FAILED"),
            "total_files_reduced": total_files_reduced,
            "total_bytes_saved": total_bytes_saved,
            "status": "FAILED" if has_failures else "SUCCESS",
        }
        logger.info(
            "Bronze compaction cycle complete: %s",
            json.dumps(summary),
            extra={"event": "compaction_complete", "summary": summary},
        )

        return 1 if has_failures else 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running the marketplace Bronze compaction engine."""
    parser = build_compaction_arg_parser()
    args = parser.parse_args(args=argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    configure_logging(level=log_level)

    config = parse_bronze_config(parser=parser, args=argv)

    runner = BronzeCompactionRunner(
        config=config,
        target_date=args.date,
        force=args.force,
        push_metrics=args.push_metrics,
        pushgateway_url=args.pushgateway_url,
        skip_preflight=args.skip_preflight,
    )

    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
