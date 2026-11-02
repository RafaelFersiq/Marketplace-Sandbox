"""Medallion Bronze layer dataset reader for analytical and Silver layer pipelines.

Provides high-performance analytical reading interfaces with catalog-driven partition pruning
(topic, year, month, day), schema-on-read resolution for evolving Avro schemas (mergeSchema),
time-range filtering, and physical/catalog metadata discovery conforming strictly to ADR-0008.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StructType, TimestampType

from processing.bronze.catalog import (
    BronzeCatalogError,
    BronzeCatalogManager,
    BronzeConnectionError,
    _normalize_date,
)
from processing.bronze.config import BronzeStorageConfig
from processing.bronze.schema import build_bronze_enveloped_schema

logger = logging.getLogger("processing.bronze.reader")


class BronzeReaderError(Exception):
    """Base exception for Medallion BronzeDatasetReader operations."""


class BronzeSparkInitError(BronzeReaderError):
    """Raised when SparkSession fails to initialize on the host runtime."""


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


def _extract_partition_date_from_path(partition_path: str) -> tuple[int, int, int] | None:
    """Extract year, month, and day integers from canonical partition directory URI."""
    match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", partition_path)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return None


class BronzeDatasetReader:
    """Analytical and Silver layer reader for Medallion Bronze partitioned Parquet datasets.

    Supports catalog-driven partition pruning via `lakehouse_metadata.bronze_partition_catalog`,
    graceful fallback to direct storage paths if the catalog is offline or empty, schema-on-read
    merging for evolving payloads, and SQL partition filter expressions.
    """

    def __init__(
        self,
        config: BronzeStorageConfig | None = None,
        spark: SparkSession | None = None,
        catalog: BronzeCatalogManager | None = None,
    ) -> None:
        """Initialize BronzeDatasetReader.

        Args:
            config: BronzeStorageConfig containing storage paths and catalog connection parameters.
            spark: Optional active SparkSession instance.
            catalog: Optional injected BronzeCatalogManager instance.
        """
        self.config = config or BronzeStorageConfig()
        self._spark = spark
        self._catalog = catalog
        self._owns_catalog = catalog is None

    @property
    def spark(self) -> SparkSession:
        """Lazily retrieve or build the active SparkSession."""
        if self._spark is not None:
            return self._spark

        try:
            active = SparkSession.getActiveSession()
            if active is not None:
                self._spark = active
                return self._spark

            builder = SparkSession.builder.appName(self.config.app_name).master(self.config.master)
            spark_conf = self.config.to_spark_conf()
            # Enforce Parquet schema merging for reader to handle evolving schemas seamlessly
            spark_conf["spark.sql.parquet.mergeSchema"] = "true"
            for k, v in spark_conf.items():
                builder = builder.config(k, v)

            self._spark = builder.getOrCreate()
            return self._spark
        except Exception as exc:
            logger.warning(
                "SparkSession could not be initialized on host (e.g. JVM not found or runtime error)",
                extra={"error": str(exc)},
            )
            raise BronzeSparkInitError(
                f"SparkSession initialization failed: {exc}"
            ) from exc

    @property
    def catalog(self) -> BronzeCatalogManager:
        """Lazily initialize or return the PostgreSQL metadata catalog manager."""
        if self._catalog is None:
            self._catalog = BronzeCatalogManager(config=self.config)
        return self._catalog

    def resolve_partition_paths(
        self,
        topic: str,
        start_date: str | datetime | date | None = None,
        end_date: str | datetime | date | None = None,
        use_catalog: bool = True,
    ) -> list[str]:
        """Resolve physical partition directory paths using catalog pruning or fallback.

        Args:
            topic: Kafka domain topic identifier.
            start_date: Optional inclusive start date (YYYY-MM-DD, datetime, or date).
            end_date: Optional inclusive end date (YYYY-MM-DD, datetime, or date).
            use_catalog: Whether to query the PostgreSQL metadata catalog for pruning.

        Returns:
            List of resolved partition directory URIs. Returns empty list if catalog
            contains no matching partitions or is offline/disabled.
        """
        if not use_catalog:
            return []

        try:
            paths = self.catalog.find_partitions(
                topic=topic,
                start_date=start_date,
                end_date=end_date,
            )
            logger.info(
                "Resolved %d partition paths via catalog for topic '%s'",
                len(paths),
                topic,
                extra={
                    "topic": topic,
                    "count": len(paths),
                    "start_date": str(start_date),
                    "end_date": str(end_date),
                },
            )
            return paths
        except (BronzeConnectionError, BronzeCatalogError, Exception) as exc:
            logger.warning(
                "Catalog partition lookup failed for topic '%s'; falling back to direct path: %s",
                topic,
                exc,
                extra={"topic": topic, "error": str(exc)},
            )
            return []

    def read_dataset(
        self,
        topic: str,
        start_date: str | datetime | date | None = None,
        end_date: str | datetime | date | None = None,
        partition_filter: str | None = None,
        columns: list[str] | None = None,
        merge_schema: bool = True,
        use_catalog: bool = True,
        payload_schema: StructType | None = None,
    ) -> DataFrame:
        """Read partitioned Bronze Parquet dataset for a specific business domain.

        Args:
            topic: Kafka business topic / domain identifier.
            start_date: Optional inclusive start date for partition pruning.
            end_date: Optional inclusive end date for partition pruning.
            partition_filter: Optional SQL filter expression (e.g. "year = 2026 AND month = 11").
            columns: Optional list of projected column names.
            merge_schema: Enable schema-on-read merging across evolving Parquet files.
            use_catalog: If True, uses PostgreSQL catalog for O(1) partition pruning.
            payload_schema: Optional expected domain payload schema for empty fallback.

        Returns:
            PySpark DataFrame with partition pruning and projections applied.
        """
        partition_paths = self.resolve_partition_paths(
            topic=topic,
            start_date=start_date,
            end_date=end_date,
            use_catalog=use_catalog,
        )

        merge_opt = "true" if merge_schema else "false"
        reader = self.spark.read.option("mergeSchema", merge_opt)

        if partition_paths:
            # Catalog-pruned fast path: pass resolved paths directly to Parquet reader
            df = reader.parquet(*partition_paths)
        else:
            # Direct path resolution fallback: scan topic root and apply date filter in Spark
            topic_root = self.config.get_topic_root_path(topic)
            try:
                df = reader.parquet(topic_root)
            except Exception as exc:
                err_msg = str(exc).lower()
                if "path does not exist" in err_msg or "not found" in err_msg:
                    logger.warning(
                        "Topic storage path does not exist; returning empty DataFrame",
                        extra={"topic": topic, "path": topic_root},
                    )
                    schema = build_bronze_enveloped_schema(payload_schema or StructType([]))
                    return self.spark.createDataFrame([], schema=schema)
                raise

            # Apply date range filtering in Spark SQL if dates supplied
            if start_date is not None:
                norm_start = _normalize_date(start_date)
                df = df.filter(
                    F.make_date(F.col("year"), F.col("month"), F.col("day"))
                    >= F.lit(norm_start.isoformat())
                )

            if end_date is not None:
                norm_end = _normalize_date(end_date)
                df = df.filter(
                    F.make_date(F.col("year"), F.col("month"), F.col("day"))
                    <= F.lit(norm_end.isoformat())
                )

        # Apply additional custom partition filter if provided
        if partition_filter:
            df = df.filter(partition_filter)

        # Apply columnar projection if requested
        if columns:
            df = df.select(*columns)

        return df

    def read_topic(
        self,
        topic: str,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        use_catalog_pruning: bool = True,
        payload_schema: StructType | None = None,
    ) -> DataFrame:
        """Alias for read_dataset with catalog-driven partition pruning.

        Args:
            topic: Kafka domain identifier (e.g. 'marketplace.orders').
            start_date: Optional inclusive start date (YYYY-MM-DD or date object).
            end_date: Optional inclusive end date (YYYY-MM-DD or date object).
            use_catalog_pruning: Whether to use PostgreSQL partition catalog for pruning.
            payload_schema: Optional expected domain payload schema for empty fallback.

        Returns:
            PySpark DataFrame containing the topic dataset.
        """
        return self.read_dataset(
            topic=topic,
            start_date=start_date,
            end_date=end_date,
            use_catalog=use_catalog_pruning,
            payload_schema=payload_schema,
        )

    def read_multi_topic(
        self,
        topics: list[str] | None = None,
        start_date: str | datetime | date | None = None,
        end_date: str | datetime | date | None = None,
        partition_filter: str | None = None,
        columns: list[str] | None = None,
        merge_schema: bool = True,
        use_catalog: bool = True,
    ) -> dict[str, DataFrame]:
        """Read Bronze datasets across multiple topics in batch.

        Args:
            topics: List of topic names (defaults to `config.topics` if omitted).
            start_date: Optional start date filter.
            end_date: Optional end date filter.
            partition_filter: Optional SQL filter expression.
            columns: Optional list of projected column names.
            merge_schema: Enable schema-on-read merging.
            use_catalog: Whether to use catalog partition pruning.

        Returns:
            Dictionary mapping topic name to corresponding PySpark DataFrame.
        """
        target_topics = topics or self.config.topics
        return {
            t: self.read_dataset(
                topic=t,
                start_date=start_date,
                end_date=end_date,
                partition_filter=partition_filter,
                columns=columns,
                merge_schema=merge_schema,
                use_catalog=use_catalog,
            )
            for t in target_topics
        }

    def read_partition(self, partition_path: str) -> DataFrame:
        """Read a specific partition directory directly with Parquet mergeSchema.

        Args:
            partition_path: Direct partition path URI (e.g. s3a://.../year=2026/month=11/day=02).

        Returns:
            PySpark DataFrame containing the partition dataset.
        """
        logger.info(
            "Reading partition directory directly",
            extra={"partition_path": partition_path},
        )
        return self.spark.read.option("mergeSchema", "true").parquet(partition_path)

    def read_range(
        self,
        topic: str,
        start_timestamp: datetime,
        end_timestamp: datetime,
        timestamp_column: str | None = None,
        use_catalog_pruning: bool = True,
    ) -> DataFrame:
        """Read records within an event or ingestion timestamp range.

        Prunes by date partitions first, then applies event-timestamp or ingestion-timestamp filter.

        Args:
            topic: Kafka domain identifier.
            start_timestamp: Inclusive start timestamp.
            end_timestamp: Inclusive end timestamp.
            timestamp_column: Optional column name for filtering (defaults to '_ingested_at' or event time).
            use_catalog_pruning: Whether to prune date partitions via catalog.

        Returns:
            Filtered PySpark DataFrame.
        """
        logger.info(
            "Reading range for topic",
            extra={
                "topic": topic,
                "start_timestamp": start_timestamp.isoformat(),
                "end_timestamp": end_timestamp.isoformat(),
                "timestamp_column": timestamp_column,
                "use_catalog_pruning": use_catalog_pruning,
            },
        )
        # 1. Date partition pruning
        start_date = start_timestamp.date()
        end_date = end_timestamp.date()
        df = self.read_dataset(
            topic=topic,
            start_date=start_date,
            end_date=end_date,
            use_catalog=use_catalog_pruning,
        )

        # 2. Timestamp filter (_ingested_at or event time)
        effective_ts_col: Any = None
        if timestamp_column is not None and timestamp_column in df.columns:
            effective_ts_col = F.to_timestamp(F.col(timestamp_column))
        elif "_ingested_at" in df.columns:
            effective_ts_col = F.col("_ingested_at")
        elif "event_time" in df.columns:
            effective_ts_col = F.to_timestamp(F.col("event_time"))
        elif "timestamp" in df.columns:
            effective_ts_col = F.to_timestamp(F.col("timestamp"))
        elif "kafka_timestamp" in df.columns:
            effective_ts_col = F.to_timestamp(F.col("kafka_timestamp"))
        elif "event_timestamp_ms" in df.columns:
            effective_ts_col = (F.col("event_timestamp_ms") / 1000.0).cast(TimestampType())

        if effective_ts_col is not None:
            start_iso = start_timestamp.isoformat()
            end_iso = end_timestamp.isoformat()
            df = df.filter(
                (effective_ts_col >= F.to_timestamp(F.lit(start_iso)))
                & (effective_ts_col <= F.to_timestamp(F.lit(end_iso)))
            )
        else:
            logger.warning(
                "No timestamp column found for range filtering; returning partition-pruned DataFrame",
                extra={"topic": topic, "columns": df.columns},
            )

        return df

    def get_partition_count(
        self,
        topic: str,
        start_date: str | datetime | date | None = None,
        end_date: str | datetime | date | None = None,
        use_catalog: bool = True,
    ) -> int:
        """Return count of resolved partition directories for a topic.

        Args:
            topic: Domain topic identifier.
            start_date: Optional start date.
            end_date: Optional end date.
            use_catalog: Whether to query catalog.

        Returns:
            Count of partition paths.
        """
        paths = self.resolve_partition_paths(
            topic=topic,
            start_date=start_date,
            end_date=end_date,
            use_catalog=use_catalog,
        )
        return len(paths)

    def get_partition_metadata(
        self,
        topic: str,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Discover partition metadata from PostgreSQL catalog or physical storage inspection.

        Args:
            topic: Kafka domain identifier.
            start_date: Optional inclusive start date.
            end_date: Optional inclusive end date.

        Returns:
            List of dictionaries containing partition metadata records.
        """
        logger.info(
            "Discovering partition metadata",
            extra={
                "topic": topic,
                "start_date": str(start_date) if start_date else None,
                "end_date": str(end_date) if end_date else None,
            },
        )
        try:
            metadata = self._get_metadata_from_catalog(topic, start_date, end_date)
            if metadata:
                return metadata
        except Exception as exc:
            logger.warning(
                "Catalog partition metadata discovery failed; falling back to physical inspection",
                extra={"topic": topic, "error": str(exc)},
            )

        norm_start = _normalize_date(start_date) if start_date is not None else None
        norm_end = _normalize_date(end_date) if end_date is not None else None
        return self._inspect_physical_partitions(topic, norm_start, norm_end)

    def _get_metadata_from_catalog(
        self,
        topic: str,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Query PostgreSQL catalog for partition metadata records."""
        if hasattr(self.catalog, "get_partition_metadata"):
            return self.catalog.get_partition_metadata(  # type: ignore[no-any-return]
                topic=topic,
                start_date=start_date,
                end_date=end_date,
            )

        norm_start = _normalize_date(start_date) if start_date is not None else None
        norm_end = _normalize_date(end_date) if end_date is not None else None

        table_name = self.config.full_partition_table_name
        query = f"""
            SELECT id, topic, year, month, day, partition_path, record_count,
                   file_count, total_bytes, min_event_timestamp, max_event_timestamp,
                   min_kafka_offset, max_kafka_offset, is_compacted, status
            FROM {table_name}
            WHERE topic = %s
        """
        params: list[Any] = [topic]
        if norm_start is not None:
            query += " AND MAKE_DATE(year, month, day) >= %s"
            params.append(norm_start)
        if norm_end is not None:
            query += " AND MAKE_DATE(year, month, day) <= %s"
            params.append(norm_end)
        query += " ORDER BY year ASC, month ASC, day ASC, partition_path ASC;"

        try:
            with self.catalog._cursor() as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                if not rows:
                    return []
                if isinstance(rows[0], dict):
                    return list(rows)
                col_names = [desc[0] for desc in cursor.description]
                return [dict(zip(col_names, row, strict=False)) for row in rows]
        except Exception:
            if hasattr(self.catalog, "find_partitions"):
                paths = self.catalog.find_partitions(topic=topic, start_date=start_date, end_date=end_date)
                if paths:
                    results: list[dict[str, Any]] = []
                    for p in paths:
                        parsed = _extract_partition_date_from_path(p)
                        if parsed:
                            y, m, d = parsed
                            results.append({
                                "topic": topic,
                                "year": y,
                                "month": m,
                                "day": d,
                                "partition_path": p,
                                "record_count": 0,
                                "file_count": 1,
                                "total_bytes": 0,
                                "is_compacted": False,
                                "status": "COMMITTED",
                            })
                    return results
            raise

    def _inspect_physical_partitions(
        self,
        topic: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        """Inspect storage directory physically to discover partition metadata."""
        if _is_s3_path(self.config.bronze_root_path):
            return self._inspect_physical_s3_partitions(topic, start_date, end_date)
        return self._inspect_physical_local_partitions(topic, start_date, end_date)

    def _inspect_physical_local_partitions(
        self,
        topic: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        """Inspect local filesystem partition directories."""
        clean_root = self.config.bronze_root_path.replace("file://", "")
        topic_dir = Path(clean_root) / topic
        if not topic_dir.exists() or not topic_dir.is_dir():
            return []

        results: list[dict[str, Any]] = []
        for year_dir in sorted(topic_dir.glob("year=*")):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name.split("=")[1])
            except (IndexError, ValueError):
                continue

            for month_dir in sorted(year_dir.glob("month=*")):
                if not month_dir.is_dir():
                    continue
                try:
                    month = int(month_dir.name.split("=")[1])
                except (IndexError, ValueError):
                    continue

                for day_dir in sorted(month_dir.glob("day=*")):
                    if not day_dir.is_dir():
                        continue
                    try:
                        day = int(day_dir.name.split("=")[1])
                    except (IndexError, ValueError):
                        continue

                    part_date = date(year, month, day)
                    if start_date is not None and part_date < start_date:
                        continue
                    if end_date is not None and part_date > end_date:
                        continue

                    parquet_files = [
                        f for f in day_dir.glob("*.parquet")
                        if f.is_file() and not f.name.startswith(".")
                    ]
                    total_bytes = sum(f.stat().st_size for f in parquet_files)
                    results.append({
                        "topic": topic,
                        "year": year,
                        "month": month,
                        "day": day,
                        "partition_path": str(day_dir),
                        "file_count": len(parquet_files),
                        "total_bytes": total_bytes,
                        "record_count": 0,
                        "is_compacted": len(parquet_files) <= 1,
                        "status": "COMMITTED",
                    })

        return results

    def _inspect_physical_s3_partitions(
        self,
        topic: str,
        start_date: date | None,
        end_date: date | None,
    ) -> list[dict[str, Any]]:
        """Inspect S3/MinIO bucket partition prefixes."""
        try:
            s3 = self._get_s3_client()
            bucket, base_prefix = _parse_s3_path(self.config.bronze_root_path)
            topic_prefix = f"{base_prefix}/{topic}/" if base_prefix else f"{topic}/"

            paginator = s3.get_paginator("list_objects_v2")
            partitions_map: dict[str, dict[str, Any]] = {}

            for page in paginator.paginate(Bucket=bucket, Prefix=topic_prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if (
                        not key.endswith(".parquet")
                        or "/." in key
                        or "_compacting_staging_" in key
                    ):
                        continue
                    parsed = _extract_partition_date_from_path(key)
                    if not parsed:
                        continue
                    year, month, day = parsed
                    part_date = date(year, month, day)
                    if start_date is not None and part_date < start_date:
                        continue
                    if end_date is not None and part_date > end_date:
                        continue

                    part_key = f"year={year:04d}/month={month:02d}/day={day:02d}"
                    part_uri = f"s3a://{bucket}/{topic_prefix}{part_key}"

                    if part_uri not in partitions_map:
                        partitions_map[part_uri] = {
                            "topic": topic,
                            "year": year,
                            "month": month,
                            "day": day,
                            "partition_path": part_uri,
                            "file_count": 0,
                            "total_bytes": 0,
                            "record_count": 0,
                            "is_compacted": False,
                            "status": "COMMITTED",
                        }
                    partitions_map[part_uri]["file_count"] += 1
                    partitions_map[part_uri]["total_bytes"] += int(obj.get("Size", 0))

            for meta in partitions_map.values():
                meta["is_compacted"] = meta["file_count"] <= 1

            return sorted(
                partitions_map.values(),
                key=lambda x: (x["year"], x["month"], x["day"]),
            )
        except Exception as exc:
            logger.warning(
                "S3 physical partition inspection failed",
                extra={"topic": topic, "error": str(exc)},
            )
            return []

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

    def close(self) -> None:
        """Release managed resources including catalog connection if owned."""
        if self._owns_catalog and self._catalog is not None:
            self._catalog.close()
            self._catalog = None

    def __enter__(self) -> BronzeDatasetReader:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Exit context manager and release catalog connection resources."""
        self.close()
