"""Atomic Parquet table writer for Medallion Bronze layer datasets.

Persists enriched event micro-batches into partitioned Parquet files within MinIO / S3
(s3a://lakehouse-bronze/events/{topic}/year={YYYY}/month={MM}/day={DD}/) enforcing append-only
immutability (WORM semantics), partition pruning layout, deduplication via payload hash,
and structured JSON write audit logging conforming to ADR-0008.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from processing.bronze.config import BronzeStorageConfig

logger = logging.getLogger("processing.bronze.writer")


@dataclass
class BronzeWriteResult:
    """Result metadata produced by BronzeTableWriter batch write operation."""

    batch_id: str
    topic: str
    record_count: int
    partitions_written: list[str]
    bytes_written: int
    files_written: int
    status: str
    duration_ms: int
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert write result into a JSON-serializable dictionary.

        Returns:
            Dictionary mapping audit fields to result values.
        """
        return {
            "batch_id": self.batch_id,
            "topic": self.topic,
            "record_count": self.record_count,
            "partitions_written": self.partitions_written,
            "bytes_written": self.bytes_written,
            "files_written": self.files_written,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "error_message": self.error_message,
        }


class BronzeTableWriter:
    """Atomic columnar Parquet writer for Medallion Bronze layer datasets."""

    def __init__(
        self,
        config: BronzeStorageConfig | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        """Initialize BronzeTableWriter.

        Args:
            config: BronzeStorageConfig containing storage paths, partition keys, and compression.
            spark: Optional active SparkSession instance.
        """
        self.config = config or BronzeStorageConfig()
        self.spark = spark

    def _inspect_partition_files(self, partition_paths: list[str]) -> tuple[int, int]:
        """Inspect written partition directories for local or accessible storage.

        Args:
            partition_paths: List of partition directory URIs.

        Returns:
            Tuple of (total_bytes, total_files).
        """
        total_bytes = 0
        total_files = 0
        for p in partition_paths:
            clean_path = p.replace("file://", "")
            path_obj = Path(clean_path)
            if path_obj.exists():
                for parquet_file in path_obj.rglob("*.parquet"):
                    try:
                        total_files += 1
                        total_bytes += parquet_file.stat().st_size
                    except OSError:
                        pass
        return total_bytes, total_files

    def write_batch(
        self,
        df: DataFrame,
        topic: str | None = None,
        batch_id: str | None = None,
        deduplicate_by_payload_hash: bool = False,
        raise_on_error: bool = False,
    ) -> BronzeWriteResult:
        """Write enriched DataFrame micro-batch to partitioned Bronze Parquet storage.

        Args:
            df: Enriched PySpark DataFrame containing technical envelope and partition columns.
            topic: Optional topic identifier.
            batch_id: Optional execution batch ID.
            deduplicate_by_payload_hash: If True, drops duplicate records by '_payload_hash'.
            raise_on_error: If True, re-raises any caught write exceptions.

        Returns:
            BronzeWriteResult tracking written partitions, record count, and execution status.
        """
        start_time = time.monotonic()
        effective_batch_id = batch_id or str(uuid.uuid4())

        # Resolve topic
        effective_topic = topic or "dynamic"
        if topic is not None:
            if "topic" not in df.columns:
                df = df.withColumn("topic", F.lit(topic))
        elif "topic" not in df.columns and "_source_topic" in df.columns:
            df = df.withColumn("topic", F.col("_source_topic"))

        # Deduplication within batch by payload hash if requested
        if deduplicate_by_payload_hash and "_payload_hash" in df.columns:
            initial_count = df.count()
            df = df.dropDuplicates(["_payload_hash"])
            logger.info(
                "Deduplicated batch by '_payload_hash'",
                extra={
                    "batch_id": effective_batch_id,
                    "records_before": initial_count,
                    "records_after": df.count(),
                },
            )

        record_count = int(df.count())
        if record_count == 0:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            result = BronzeWriteResult(
                batch_id=effective_batch_id,
                topic=effective_topic,
                record_count=0,
                partitions_written=[],
                bytes_written=0,
                files_written=0,
                status="SUCCESS",
                duration_ms=duration_ms,
            )
            logger.info("Empty micro-batch processed: %s", json.dumps(result.to_dict()))
            return result

        # Derive partition paths
        partition_keys = [k for k in self.config.partition_keys if k in df.columns]
        if not partition_keys:
            partition_keys = ["topic", "year", "month", "day"]

        distinct_parts = df.select(*partition_keys).distinct().collect()
        partitions_written: list[str] = []
        for row in distinct_parts:
            row_dict = row.asDict()
            t = str(row_dict.get("topic", effective_topic))
            y = int(row_dict.get("year", 0))
            m = int(row_dict.get("month", 0))
            d = int(row_dict.get("day", 0))
            partitions_written.append(self.config.get_topic_partition_path(t, y, m, d))

        # Dry-run handling
        if self.config.dry_run:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            result = BronzeWriteResult(
                batch_id=effective_batch_id,
                topic=effective_topic,
                record_count=record_count,
                partitions_written=partitions_written,
                bytes_written=0,
                files_written=0,
                status="SKIPPED_DRY_RUN",
                duration_ms=duration_ms,
            )
            logger.info("Bronze dry-run write simulated: %s", json.dumps(result.to_dict()))
            return result

        # Execute immutable partitioned Parquet append write
        target_path = self.config.bronze_root_path
        try:
            writer = (
                df.write.mode("append")
                .format("parquet")
                .option("compression", self.config.compression)
            )
            if partition_keys:
                writer = writer.partitionBy(*partition_keys)

            writer.save(target_path)

            bytes_written, files_written = self._inspect_partition_files(partitions_written)
            duration_ms = int((time.monotonic() - start_time) * 1000)

            result = BronzeWriteResult(
                batch_id=effective_batch_id,
                topic=effective_topic,
                record_count=record_count,
                partitions_written=partitions_written,
                bytes_written=bytes_written,
                files_written=files_written,
                status="SUCCESS",
                duration_ms=duration_ms,
            )
            logger.info("Bronze Parquet batch committed: %s", json.dumps(result.to_dict()))
            return result

        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            result = BronzeWriteResult(
                batch_id=effective_batch_id,
                topic=effective_topic,
                record_count=record_count,
                partitions_written=partitions_written,
                bytes_written=0,
                files_written=0,
                status="FAILED",
                duration_ms=duration_ms,
                error_message=str(exc),
            )
            logger.error("Bronze Parquet write failed: %s", json.dumps(result.to_dict()), exc_info=True)
            if raise_on_error:
                raise
            return result
