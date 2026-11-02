"""Small-file compaction engine for Medallion Bronze layer partitioned Parquet datasets.

Scans canonical Bronze partition directories (s3a://lakehouse-bronze/events/{topic}/year={YYYY}/month={MM}/day={DD}/),
detects small-file fragmentation caused by high-frequency streaming micro-batches, coalesces/repartitions files
into optimal target sizes (default 128 MB), executes atomic staging swaps to guarantee read-isolation,
validates record count conservation, and records structured compaction audit logs conforming to ADR-0008.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from processing.bronze.config import DEFAULT_TOPICS, BronzeStorageConfig

logger = logging.getLogger("processing.bronze.compaction")


@dataclass
class BronzeCompactionResult:
    """Result metadata produced by a Bronze partition compaction run."""

    batch_id: str
    partition_path: str
    topic: str
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    records_before: int
    records_after: int
    status: str
    duration_ms: int
    error_message: str | None = None

    @property
    def files_reduced(self) -> int:
        """Return the net number of fragmented files eliminated by compaction."""
        return max(0, self.files_before - self.files_after)

    @property
    def bytes_reduced(self) -> int:
        """Return the net storage size reduction in bytes."""
        return self.bytes_before - self.bytes_after

    @property
    def compression_ratio(self) -> float:
        """Return ratio of compacted size to original size."""
        if self.bytes_before <= 0:
            return 1.0
        return round(self.bytes_after / self.bytes_before, 4)

    def to_dict(self) -> dict[str, Any]:
        """Convert compaction result into a JSON-serializable dictionary.

        Returns:
            Dictionary mapping audit fields to result values.
        """
        return {
            "batch_id": self.batch_id,
            "partition_path": self.partition_path,
            "topic": self.topic,
            "files_before": self.files_before,
            "files_after": self.files_after,
            "files_reduced": self.files_reduced,
            "bytes_before": self.bytes_before,
            "bytes_after": self.bytes_after,
            "bytes_reduced": self.bytes_reduced,
            "records_before": self.records_before,
            "records_after": self.records_after,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "error_message": self.error_message,
        }


def extract_topic_from_path(partition_path: str) -> str:
    """Extract Kafka topic / domain identifier from a partition directory URI.

    Args:
        partition_path: Canonical partition directory URI.

    Returns:
        Extracted topic string or 'unknown'.
    """
    clean = partition_path.rstrip("/")
    match = re.search(r"/([^/]+)/year=\d+", clean)
    if match:
        return match.group(1)
    for t in DEFAULT_TOPICS:
        if t in clean:
            return t
    return "unknown"


def _is_s3_path(path: str) -> bool:
    """Check if the given storage URI points to S3 / MinIO object storage."""
    return path.startswith("s3a://") or path.startswith("s3://")


def _parse_s3_path(path: str) -> tuple[str, str]:
    """Split S3 URI into (bucket, prefix) tuple."""
    raw = path.replace("s3a://", "").replace("s3://", "")
    parts = raw.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    return bucket, prefix


class BronzeCompactor:
    """Small-file compaction engine for Medallion Bronze layer partitioned Parquet datasets."""

    def __init__(
        self,
        config: BronzeStorageConfig | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        """Initialize BronzeCompactor.

        Args:
            config: BronzeStorageConfig with compaction thresholds and storage paths.
            spark: Optional active SparkSession instance.
        """
        self.config = config or BronzeStorageConfig()
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Lazily retrieve or create the active SparkSession."""
        if self._spark is not None:
            return self._spark
        active = SparkSession.getActiveSession()
        if active is not None:
            return active
        builder = SparkSession.builder.appName(self.config.app_name).master(self.config.master)
        for k, v in self.config.to_spark_conf().items():
            builder = builder.config(k, v)
        self._spark = builder.getOrCreate()
        return self._spark

    def _get_s3_client(self) -> Any:
        """Create configured boto3 S3 client for MinIO object operations."""
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.config.minio_endpoint,
            aws_access_key_id=self.config.minio_access_key,
            aws_secret_access_key=self.config.minio_secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def should_compact(self, partition_path: str, file_count: int, total_bytes: int) -> bool:
        """Determine whether a partition qualifies for small-file compaction.

        Returns True if file_count >= config.compaction_threshold_file_count or if average
        file size is smaller than config.compaction_min_file_size_bytes.

        Args:
            partition_path: Canonical partition directory URI.
            file_count: Number of Parquet files in the partition.
            total_bytes: Aggregated byte size of Parquet files in the partition.

        Returns:
            True if partition meets compaction criteria, False otherwise.
        """
        logger.debug(
            "Evaluating compaction eligibility for %s (files: %d, bytes: %d)",
            partition_path,
            file_count,
            total_bytes,
        )
        if file_count <= 0:
            return False
        if file_count >= self.config.compaction_threshold_file_count:
            return True
        return bool(file_count > 1 and (total_bytes / file_count) < self.config.compaction_min_file_size_bytes)

    def inspect_partition(self, partition_path: str) -> tuple[int, int]:
        """Inspect partition directory returning (file_count, total_bytes).

        Args:
            partition_path: URI of the partition directory.

        Returns:
            Tuple of (file_count, total_bytes).
        """
        if _is_s3_path(partition_path):
            return self._inspect_s3_partition(partition_path)
        return self._inspect_local_partition(partition_path)

    def _inspect_local_partition(self, partition_path: str) -> tuple[int, int]:
        clean = partition_path.replace("file://", "")
        p = Path(clean)
        if not p.exists() or not p.is_dir():
            return 0, 0
        parquet_files = [
            f for f in p.iterdir()
            if f.is_file() and f.suffix == ".parquet" and not f.name.startswith(".")
        ]
        total_bytes = sum(f.stat().st_size for f in parquet_files)
        return len(parquet_files), total_bytes

    def _inspect_s3_partition(self, partition_path: str) -> tuple[int, int]:
        try:
            s3 = self._get_s3_client()
            bucket, prefix = _parse_s3_path(partition_path)
            prefix = f"{prefix}/" if prefix and not prefix.endswith("/") else prefix
            paginator = s3.get_paginator("list_objects_v2")
            total_files = 0
            total_bytes = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    name = key.split("/")[-1]
                    if name.endswith(".parquet") and not name.startswith(".") and "_compacting_staging_" not in key:
                        total_files += 1
                        total_bytes += int(obj.get("Size", 0))
            return total_files, total_bytes
        except Exception as exc:
            logger.warning("Failed to inspect S3 partition %s: %s", partition_path, exc)
            return 0, 0

    def _swap_files(self, partition_path: str, staging_path: str) -> tuple[int, int]:
        """Atomically swap staged compacted files into partition path and remove fragmented files."""
        if _is_s3_path(partition_path):
            return self._swap_s3_files(partition_path, staging_path)
        return self._swap_local_files(partition_path, staging_path)

    def _swap_local_files(self, partition_path: str, staging_path: str) -> tuple[int, int]:
        part_dir = Path(partition_path.replace("file://", ""))
        stage_dir = Path(staging_path.replace("file://", ""))

        staged_parquet = [
            f for f in stage_dir.iterdir()
            if f.is_file() and f.suffix == ".parquet" and not f.name.startswith(".")
        ]
        if not staged_parquet:
            raise ValueError(f"No compacted parquet files found in staging dir: {staging_path}")

        old_parquet = [
            f for f in part_dir.iterdir()
            if f.is_file() and f.suffix == ".parquet" and not f.name.startswith(".")
        ]

        for old_f in old_parquet:
            old_f.unlink()

        for staged_f in staged_parquet:
            dest = part_dir / staged_f.name
            staged_f.rename(dest)

        shutil.rmtree(stage_dir, ignore_errors=True)
        return self._inspect_local_partition(partition_path)

    def _swap_s3_files(self, partition_path: str, staging_path: str) -> tuple[int, int]:
        s3 = self._get_s3_client()
        part_bucket, part_prefix = _parse_s3_path(partition_path)
        part_prefix = f"{part_prefix}/" if part_prefix and not part_prefix.endswith("/") else part_prefix

        stage_bucket, stage_prefix = _parse_s3_path(staging_path)
        stage_prefix = f"{stage_prefix}/" if stage_prefix and not stage_prefix.endswith("/") else stage_prefix

        paginator = s3.get_paginator("list_objects_v2")
        staged_objects = []
        for page in paginator.paginate(Bucket=stage_bucket, Prefix=stage_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                name = key.split("/")[-1]
                if name.endswith(".parquet") and not name.startswith("."):
                    staged_objects.append(key)

        if not staged_objects:
            raise ValueError(f"No compacted parquet files found in S3 staging prefix: {staging_path}")

        old_objects = []
        for page in paginator.paginate(Bucket=part_bucket, Prefix=part_prefix, Delimiter="/"):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                name = key.split("/")[-1]
                if name.endswith(".parquet") and not name.startswith(".") and "_compacting_staging_" not in key:
                    old_objects.append(key)

        for staged_key in staged_objects:
            filename = staged_key.split("/")[-1]
            dest_key = f"{part_prefix}{filename}"
            s3.copy_object(
                Bucket=part_bucket,
                CopySource={"Bucket": stage_bucket, "Key": staged_key},
                Key=dest_key,
            )

        for old_key in old_objects:
            s3.delete_object(Bucket=part_bucket, Key=old_key)

        staging_all_keys = []
        for page in paginator.paginate(Bucket=stage_bucket, Prefix=stage_prefix):
            for obj in page.get("Contents", []):
                staging_all_keys.append({"Key": obj["Key"]})
        if staging_all_keys:
            s3.delete_objects(Bucket=stage_bucket, Delete={"Objects": staging_all_keys})

        return self._inspect_s3_partition(partition_path)

    def _cleanup_staging(self, staging_path: str) -> None:
        """Safely clean up any staged files or temporary directory on failure."""
        if _is_s3_path(staging_path):
            try:
                s3 = self._get_s3_client()
                bucket, prefix = _parse_s3_path(staging_path)
                prefix = f"{prefix}/" if prefix and not prefix.endswith("/") else prefix
                paginator = s3.get_paginator("list_objects_v2")
                to_delete = []
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        to_delete.append({"Key": obj["Key"]})
                if to_delete:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
            except Exception as exc:
                logger.warning("Failed to cleanup S3 staging prefix %s: %s", staging_path, exc)
        else:
            clean = staging_path.replace("file://", "")
            p = Path(clean)
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)

    def compact_partition(
        self,
        partition_path: str,
        target_file_size_mb: int | None = None,
        batch_id: str | None = None,
        topic: str | None = None,
        force: bool = False,
    ) -> BronzeCompactionResult:
        """Execute small-file compaction for a single partition directory.

        Args:
            partition_path: Canonical partition directory URI.
            target_file_size_mb: Optional target file size in MB override.
            batch_id: Optional compaction batch ID.
            topic: Optional topic identifier.
            force: If True, bypasses should_compact threshold checks.

        Returns:
            BronzeCompactionResult with compaction metrics and status.
        """
        start_time = time.monotonic()
        effective_batch_id = batch_id or str(uuid.uuid4())
        effective_topic = topic or extract_topic_from_path(partition_path)
        staging_path = f"{partition_path.rstrip('/')}/_compacting_staging_{effective_batch_id}"

        files_before, bytes_before = self.inspect_partition(partition_path)

        if not force and not self.should_compact(partition_path, files_before, bytes_before):
            duration_ms = int((time.monotonic() - start_time) * 1000)
            result = BronzeCompactionResult(
                batch_id=effective_batch_id,
                partition_path=partition_path,
                topic=effective_topic,
                files_before=files_before,
                files_after=files_before,
                bytes_before=bytes_before,
                bytes_after=bytes_before,
                records_before=0,
                records_after=0,
                status="SKIPPED_THRESHOLD",
                duration_ms=duration_ms,
            )
            logger.info("Partition skipped below compaction threshold: %s", json.dumps(result.to_dict()))
            return result

        effective_target_mb = target_file_size_mb or self.config.target_file_size_mb
        target_file_size_bytes = effective_target_mb * 1024 * 1024
        target_parts = max(1, bytes_before // target_file_size_bytes)

        try:
            df = self.spark.read.parquet(partition_path)
            records_before = int(df.count())

            if self.config.dry_run:
                duration_ms = int((time.monotonic() - start_time) * 1000)
                result = BronzeCompactionResult(
                    batch_id=effective_batch_id,
                    partition_path=partition_path,
                    topic=effective_topic,
                    files_before=files_before,
                    files_after=min(files_before, target_parts),
                    bytes_before=bytes_before,
                    bytes_after=bytes_before,
                    records_before=records_before,
                    records_after=records_before,
                    status="SKIPPED_DRY_RUN",
                    duration_ms=duration_ms,
                )
                logger.info("Compaction dry-run simulated: %s", json.dumps(result.to_dict()))
                return result

            # Coalesce without shuffle if target_parts <= current files, else repartition
            if target_parts <= (files_before or 1):
                compacted_df = df.coalesce(target_parts)
            else:
                compacted_df = df.repartition(target_parts)

            compacted_df.write.mode("overwrite").format("parquet").option(
                "compression", self.config.compression
            ).save(staging_path)

            staged_df = self.spark.read.parquet(staging_path)
            records_after = int(staged_df.count())

            if records_after != records_before:
                raise ValueError(
                    f"Record count mismatch during compaction: {records_before} before vs {records_after} after"
                )

            files_after, bytes_after = self._swap_files(partition_path, staging_path)
            duration_ms = int((time.monotonic() - start_time) * 1000)

            result = BronzeCompactionResult(
                batch_id=effective_batch_id,
                partition_path=partition_path,
                topic=effective_topic,
                files_before=files_before,
                files_after=files_after,
                bytes_before=bytes_before,
                bytes_after=bytes_after,
                records_before=records_before,
                records_after=records_after,
                status="SUCCESS",
                duration_ms=duration_ms,
            )
            logger.info("Compaction batch successfully committed: %s", json.dumps(result.to_dict()))
            return result

        except Exception as exc:
            self._cleanup_staging(staging_path)
            duration_ms = int((time.monotonic() - start_time) * 1000)
            result = BronzeCompactionResult(
                batch_id=effective_batch_id,
                partition_path=partition_path,
                topic=effective_topic,
                files_before=files_before,
                files_after=files_before,
                bytes_before=bytes_before,
                bytes_after=bytes_before,
                records_before=records_before if "records_before" in locals() else 0,
                records_after=0,
                status="FAILED",
                duration_ms=duration_ms,
                error_message=str(exc),
            )
            logger.error(
                "Compaction failed for partition %s: %s",
                partition_path,
                json.dumps(result.to_dict()),
                exc_info=True,
            )
            return result

    def discover_partitions(self, topic: str | None = None) -> list[dict[str, Any]]:
        """Scan Bronze storage hierarchy and discover all leaf event-date partition directories.

        Args:
            topic: Optional topic filter. If None, scans all configured topics.

        Returns:
            List of partition info dictionaries:
            [{'partition_path': str, 'topic': str, 'file_count': int, 'total_bytes': int}, ...]
        """
        topics = [topic] if topic else self.config.topics
        if _is_s3_path(self.config.bronze_root_path):
            return self._discover_s3_partitions(topics)
        return self._discover_local_partitions(topics)

    def _discover_local_partitions(self, topics: list[str]) -> list[dict[str, Any]]:
        discovered: list[dict[str, Any]] = []
        root = Path(self.config.bronze_root_path.replace("file://", ""))
        if not root.exists():
            return discovered
        for t in topics:
            topic_dir = root / t
            if not topic_dir.exists():
                continue
            for day_dir in topic_dir.glob("year=*/month=*/day=*"):
                if day_dir.is_dir():
                    path_str = str(day_dir)
                    f_cnt, b_cnt = self._inspect_local_partition(path_str)
                    discovered.append({
                        "partition_path": path_str,
                        "topic": t,
                        "file_count": f_cnt,
                        "total_bytes": b_cnt,
                    })
        return discovered

    def _discover_s3_partitions(self, topics: list[str]) -> list[dict[str, Any]]:
        discovered: list[dict[str, Any]] = []
        try:
            s3 = self._get_s3_client()
            bucket, root_prefix = _parse_s3_path(self.config.bronze_root_path)
            for t in topics:
                prefix = f"{root_prefix}/{t}/" if root_prefix else f"{t}/"
                paginator = s3.get_paginator("list_objects_v2")
                day_prefixes: set[str] = set()
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj.get("Key", "")
                        match = re.search(r"(.+/year=\d+/month=\d+/day=\d+)/", key)
                        if match:
                            day_prefixes.add(match.group(1))
                for p_prefix in sorted(day_prefixes):
                    full_uri = f"s3a://{bucket}/{p_prefix}"
                    f_cnt, b_cnt = self._inspect_s3_partition(full_uri)
                    discovered.append({
                        "partition_path": full_uri,
                        "topic": t,
                        "file_count": f_cnt,
                        "total_bytes": b_cnt,
                    })
        except Exception as exc:
            logger.warning("Failed to discover S3 partitions: %s", exc)
        return discovered

    def compact_all_partitions(
        self,
        partitions: list[dict[str, Any]] | None = None,
        topic: str | None = None,
        target_file_size_mb: int | None = None,
        force: bool = False,
    ) -> list[BronzeCompactionResult]:
        """Execute small-file compaction across multiple partition directories.

        Args:
            partitions: Optional list of partition dicts containing 'partition_path',
                and optionally 'file_count', 'total_bytes', and 'topic'.
                If None, partitions are automatically discovered via discover_partitions().
            topic: Optional topic filter to restrict compaction to a specific domain.
            target_file_size_mb: Optional override for target compacted file size.
            force: If True, bypasses should_compact threshold checks.

        Returns:
            List of BronzeCompactionResult instances for each evaluated partition.
        """
        partition_list = self.discover_partitions(topic=topic) if partitions is None else partitions

        if topic is not None:
            partition_list = [
                p for p in partition_list
                if p.get("topic") == topic or topic in p.get("partition_path", "")
            ]

        results: list[BronzeCompactionResult] = []
        for p in partition_list:
            path = str(p.get("partition_path") or p.get("path", ""))
            if not path:
                continue
            part_topic = p.get("topic") or topic or extract_topic_from_path(path)
            f_count = p.get("file_count")
            t_bytes = p.get("total_bytes")

            if f_count is None or t_bytes is None:
                insp_f, insp_b = self.inspect_partition(path)
                f_count = f_count if f_count is not None else insp_f
                t_bytes = t_bytes if t_bytes is not None else insp_b

            if not force and not self.should_compact(path, int(f_count), int(t_bytes)):
                res = BronzeCompactionResult(
                    batch_id=str(uuid.uuid4()),
                    partition_path=path,
                    topic=part_topic,
                    files_before=int(f_count),
                    files_after=int(f_count),
                    bytes_before=int(t_bytes),
                    bytes_after=int(t_bytes),
                    records_before=0,
                    records_after=0,
                    status="SKIPPED_THRESHOLD",
                    duration_ms=0,
                )
                results.append(res)
                continue

            res = self.compact_partition(
                partition_path=path,
                target_file_size_mb=target_file_size_mb,
                topic=part_topic,
                force=True,
            )
            results.append(res)

        summary = {
            "total_partitions": len(results),
            "successful": sum(1 for r in results if r.status == "SUCCESS"),
            "skipped_dry_run": sum(1 for r in results if r.status == "SKIPPED_DRY_RUN"),
            "skipped_threshold": sum(1 for r in results if r.status == "SKIPPED_THRESHOLD"),
            "failed": sum(1 for r in results if r.status == "FAILED"),
        }
        logger.info("Bronze compaction cycle completed: %s", json.dumps(summary))
        return results
