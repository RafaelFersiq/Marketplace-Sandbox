"""PostgreSQL metadata catalog manager for Medallion Bronze layer datasets.

Tracks physical partition lifecycle, data volume metrics, offset intervals, and
compaction status in `lakehouse_metadata.bronze_partition_catalog`, and records batch
execution telemetry in `lakehouse_metadata.bronze_ingestion_audit`. Enables fast partition
discovery and partition-pruned reading for downstream consumers without expensive recursive
S3 LIST operations, conforming strictly to ADR-0008.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg import Connection, Cursor

from processing.bronze.config import BronzeStorageConfig

logger = logging.getLogger("processing.bronze.catalog")


class BronzeCatalogError(Exception):
    """Base exception for Medallion Bronze metadata catalog operations."""


class BronzeConnectionError(BronzeCatalogError):
    """Raised when connection to PostgreSQL metadata catalog fails."""


class BronzePartitionCatalogError(BronzeCatalogError):
    """Raised when partition catalog registration, query, or update fails."""


class BronzeAuditCatalogError(BronzeCatalogError):
    """Raised when ingestion audit logging fails."""


def _normalize_date(val: str | datetime | date) -> date:
    """Normalize string, datetime, or date input into a datetime.date object.

    Args:
        val: ISO date string (YYYY-MM-DD), datetime, or date instance.

    Returns:
        Standard datetime.date instance.

    Raises:
        ValueError: If input format cannot be parsed.
    """
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        cleaned = val.strip()
        # Parse standard ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
    raise ValueError(f"Invalid date format or type: {type(val)} ({val})")


def _is_closed(obj: Any) -> bool:
    """Check if connection or cursor is closed, properly handling mocks."""
    if obj is None:
        return True
    val = getattr(obj, "closed", False)
    return bool(val is True or (isinstance(val, (bool, int)) and bool(val)))


class BronzeCatalogManager:
    """PostgreSQL metadata catalog manager for Medallion Bronze datasets.

    Provides atomic partition registration with upsert semantics, batch ingestion
    audit logging, uncompacted small-file partition discovery, compaction tracking,
    and date-filtered partition pruning queries.
    """

    def __init__(
        self,
        config: BronzeStorageConfig | None = None,
        conn: Connection[Any] | None = None,
    ) -> None:
        """Initialize BronzeCatalogManager.

        Args:
            config: BronzeStorageConfig containing PostgreSQL connection and table settings.
            conn: Optional pre-configured psycopg Connection instance for testing or injection.
        """
        self.config = config or BronzeStorageConfig()
        self._conn = conn
        self._owns_connection = conn is None

    def _connect(self, max_retries: int = 3, initial_backoff_seconds: float = 0.5) -> Connection[Any]:
        """Establish connection to PostgreSQL metadata catalog with exponential backoff retry.

        Args:
            max_retries: Maximum number of connection attempts.
            initial_backoff_seconds: Base backoff time in seconds between retries.

        Returns:
            Active psycopg Connection.

        Raises:
            BronzeConnectionError: If connection cannot be established after max_retries.
        """
        if self._conn is not None and not _is_closed(self._conn):
            return self._conn

        backoff = initial_backoff_seconds
        last_error: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to PostgreSQL metadata catalog",
                    extra={
                        "host": self.config.postgres_host,
                        "port": self.config.postgres_port,
                        "dbname": self.config.postgres_db,
                        "attempt": attempt,
                        "max_retries": max_retries,
                    },
                )
                self._conn = psycopg.connect(**self.config.postgres_connection_dict)
                self._owns_connection = True
                return self._conn
            except psycopg.Error as exc:
                last_error = exc
                logger.warning(
                    "PostgreSQL catalog connection attempt failed",
                    extra={
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "error": str(exc),
                    },
                )
                if attempt < max_retries:
                    time.sleep(backoff)
                    backoff *= 2

        raise BronzeConnectionError(
            f"Failed to connect to PostgreSQL catalog after {max_retries} attempts: {last_error}"
        ) from last_error

    @property
    def connection(self) -> Connection[Any]:
        """Return active PostgreSQL connection, connecting if necessary."""
        if self._conn is None or _is_closed(self._conn):
            return self._connect()
        return self._conn

    @contextmanager
    def _cursor(self) -> Generator[Cursor[Any], None, None]:
        """Context manager yielding database cursor with automatic transaction management."""
        conn = self.connection
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            if not _is_closed(conn):
                with suppress(psycopg.Error):
                    conn.rollback()
            raise
        finally:
            if not _is_closed(cursor):
                cursor.close()

    def register_partition(
        self,
        topic: str,
        year: int,
        month: int,
        day: int,
        partition_path: str,
        record_count: int,
        file_count: int,
        total_bytes: int,
        min_event_timestamp: datetime | None = None,
        max_event_timestamp: datetime | None = None,
        min_kafka_offset: int | None = None,
        max_kafka_offset: int | None = None,
        is_compacted: bool = False,
        status: str = "COMMITTED",
    ) -> int:
        """Register or update a Bronze partition entry in the PostgreSQL catalog.

        Applies ON CONFLICT upsert semantics on (topic, year, month, day) ensuring
        idempotency across streaming micro-batches and compaction runs.

        Args:
            topic: Kafka source topic / domain identifier.
            year: Partition UTC year (YYYY).
            month: Partition UTC month (1-12).
            day: Partition UTC day (1-31).
            partition_path: Canonical S3A partition path.
            record_count: Total record count in partition.
            file_count: Total Parquet file count in partition.
            total_bytes: Total aggregate size in bytes.
            min_event_timestamp: Earliest event timestamp in partition.
            max_event_timestamp: Latest event timestamp in partition.
            min_kafka_offset: Minimum Kafka offset in partition.
            max_kafka_offset: Maximum Kafka offset in partition.
            is_compacted: Whether small files have undergone coalescing.
            status: Partition lifecycle status (e.g. COMMITTED, COMPACTING).

        Returns:
            Synthetic surrogate integer primary key (id) of the partition record.

        Raises:
            BronzePartitionCatalogError: If catalog registration fails.
        """
        table_name = self.config.full_partition_table_name
        query = f"""
            INSERT INTO {table_name} (
                topic, year, month, day, partition_path, record_count, file_count, total_bytes,
                min_event_timestamp, max_event_timestamp, min_kafka_offset, max_kafka_offset,
                is_compacted, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (topic, year, month, day) DO UPDATE SET
                record_count = EXCLUDED.record_count,
                file_count = EXCLUDED.file_count,
                total_bytes = EXCLUDED.total_bytes,
                min_event_timestamp = COALESCE(
                    EXCLUDED.min_event_timestamp, {table_name}.min_event_timestamp
                ),
                max_event_timestamp = COALESCE(
                    EXCLUDED.max_event_timestamp, {table_name}.max_event_timestamp
                ),
                min_kafka_offset = COALESCE(
                    EXCLUDED.min_kafka_offset, {table_name}.min_kafka_offset
                ),
                max_kafka_offset = COALESCE(
                    EXCLUDED.max_kafka_offset, {table_name}.max_kafka_offset
                ),
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        params = (
            topic,
            year,
            month,
            day,
            partition_path,
            record_count,
            file_count,
            total_bytes,
            min_event_timestamp,
            max_event_timestamp,
            min_kafka_offset,
            max_kafka_offset,
            is_compacted,
            status,
        )

        try:
            with self._cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
                if row is None:
                    raise BronzePartitionCatalogError(
                        f"Failed to register partition {partition_path}: no ID returned."
                    )
                partition_id = int(row[0])
                logger.info(
                    "Registered Bronze partition in catalog",
                    extra={
                        "partition_id": partition_id,
                        "topic": topic,
                        "partition_path": partition_path,
                        "record_count": record_count,
                        "file_count": file_count,
                        "total_bytes": total_bytes,
                        "is_compacted": is_compacted,
                        "status": status,
                    },
                )
                return partition_id
        except psycopg.Error as exc:
            logger.error(
                "Database error registering partition in Bronze catalog",
                extra={
                    "topic": topic,
                    "partition_path": partition_path,
                    "error": str(exc),
                },
            )
            raise BronzePartitionCatalogError(
                f"Error registering partition {partition_path}: {exc}"
            ) from exc

    def log_ingestion_audit(
        self,
        batch_id: str,
        topic: str,
        source_landing_path: str,
        target_partition_path: str,
        records_ingested: int,
        bytes_written: int,
        files_written: int,
        records_failed: int = 0,
        schema_ids_observed: list[int] | None = None,
        min_event_timestamp: datetime | None = None,
        max_event_timestamp: datetime | None = None,
        execution_duration_ms: int = 0,
        status: str = "SUCCESS",
        error_message: str | None = None,
    ) -> None:
        """Record batch execution telemetry and audit trail in PostgreSQL.

        Args:
            batch_id: Unique batch execution identifier.
            topic: Source Kafka topic ingested.
            source_landing_path: S3A landing staging path processed.
            target_partition_path: S3A destination Bronze partition path.
            records_ingested: Count of records successfully written.
            bytes_written: Total bytes written to disk.
            files_written: Count of Parquet files produced.
            records_failed: Count of records rejected or failed validation.
            schema_ids_observed: List of Confluent Schema Registry schema IDs observed.
            min_event_timestamp: Minimum event timestamp in batch.
            max_event_timestamp: Maximum event timestamp in batch.
            execution_duration_ms: Total execution duration in milliseconds.
            status: Ingestion status ('SUCCESS', 'PARTIAL', 'FAILED').
            error_message: Error diagnostics and stack trace if failed.

        Raises:
            BronzeAuditCatalogError: If audit logging fails.
        """
        table_name = self.config.full_audit_table_name
        query = f"""
            INSERT INTO {table_name} (
                batch_id, topic, source_landing_path, target_partition_path,
                records_ingested, records_failed, bytes_written, files_written,
                schema_ids_observed, min_event_timestamp, max_event_timestamp,
                execution_duration_ms, status, error_message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (batch_id) DO UPDATE SET
                records_ingested = EXCLUDED.records_ingested,
                records_failed = EXCLUDED.records_failed,
                bytes_written = EXCLUDED.bytes_written,
                files_written = EXCLUDED.files_written,
                schema_ids_observed = EXCLUDED.schema_ids_observed,
                min_event_timestamp = EXCLUDED.min_event_timestamp,
                max_event_timestamp = EXCLUDED.max_event_timestamp,
                execution_duration_ms = EXCLUDED.execution_duration_ms,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message;
        """
        params = (
            batch_id,
            topic,
            source_landing_path,
            target_partition_path,
            records_ingested,
            records_failed,
            bytes_written,
            files_written,
            schema_ids_observed,
            min_event_timestamp,
            max_event_timestamp,
            execution_duration_ms,
            status,
            error_message,
        )

        try:
            with self._cursor() as cursor:
                cursor.execute(query, params)
                logger.info(
                    "Logged Bronze batch ingestion audit",
                    extra={
                        "batch_id": batch_id,
                        "topic": topic,
                        "records_ingested": records_ingested,
                        "bytes_written": bytes_written,
                        "status": status,
                    },
                )
        except psycopg.Error as exc:
            logger.error(
                "Database error recording batch ingestion audit",
                extra={"batch_id": batch_id, "error": str(exc)},
            )
            raise BronzeAuditCatalogError(
                f"Error recording ingestion audit for batch {batch_id}: {exc}"
            ) from exc

    def get_uncompacted_partitions(
        self,
        topic: str | None = None,
        min_file_count: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query catalog for uncompacted partitions exceeding fragmentation thresholds.

        Args:
            topic: Optional topic filter. If omitted, queries across all topics.
            min_file_count: Minimum file threshold. If omitted, defaults to
                `config.compaction_threshold_file_count`.

        Returns:
            List of dictionaries containing partition metadata ordered by file count descending.
        """
        table_name = self.config.full_partition_table_name
        effective_min_files = (
            min_file_count
            if min_file_count is not None
            else self.config.compaction_threshold_file_count
        )

        query = f"""
            SELECT id, topic, year, month, day, partition_path, record_count,
                   file_count, total_bytes, min_event_timestamp, max_event_timestamp,
                   min_kafka_offset, max_kafka_offset, is_compacted, last_compacted_at,
                   status, created_at, updated_at
            FROM {table_name}
            WHERE is_compacted = FALSE AND file_count >= %s
        """
        params: list[Any] = [effective_min_files]

        if topic is not None:
            query += " AND topic = %s"
            params.append(topic)

        query += " ORDER BY file_count DESC;"

        try:
            with self._cursor() as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                if not rows:
                    return []

                # Handle dict_row cursors or standard tuple cursors
                if isinstance(rows[0], dict):
                    return list(rows)

                col_names = [desc[0] for desc in cursor.description]
                return [dict(zip(col_names, row, strict=False)) for row in rows]
        except psycopg.Error as exc:
            logger.error("Database error querying uncompacted partitions", extra={"error": str(exc)})
            raise BronzePartitionCatalogError(
                f"Error querying uncompacted partitions: {exc}"
            ) from exc

    def mark_partition_compacted(
        self,
        partition_path: str,
        file_count: int,
        total_bytes: int,
    ) -> bool:
        """Mark partition as compacted and update file count and aggregate byte size.

        Args:
            partition_path: S3A partition path updated.
            file_count: New consolidated Parquet file count.
            total_bytes: New aggregate byte size.

        Returns:
            True if catalog row was found and updated, False otherwise.

        Raises:
            BronzePartitionCatalogError: If update query fails.
        """
        table_name = self.config.full_partition_table_name
        query = f"""
            UPDATE {table_name}
            SET is_compacted = TRUE,
                last_compacted_at = CURRENT_TIMESTAMP,
                file_count = %s,
                total_bytes = %s
            WHERE partition_path = %s;
        """
        try:
            with self._cursor() as cursor:
                cursor.execute(query, (file_count, total_bytes, partition_path))
                updated = cursor.rowcount > 0
                logger.info(
                    "Updated partition compaction status in catalog",
                    extra={
                        "partition_path": partition_path,
                        "file_count": file_count,
                        "total_bytes": total_bytes,
                        "updated": updated,
                    },
                )
                return updated
        except psycopg.Error as exc:
            logger.error(
                "Database error updating compaction status",
                extra={"partition_path": partition_path, "error": str(exc)},
            )
            raise BronzePartitionCatalogError(
                f"Error marking partition {partition_path} as compacted: {exc}"
            ) from exc

    def find_partitions(
        self,
        topic: str,
        start_date: str | datetime | date | None = None,
        end_date: str | datetime | date | None = None,
    ) -> list[str]:
        """Discover partition paths for a topic with optional date range pruning.

        Enables BronzeDatasetReader to avoid recursive S3 LIST operations by querying
        the indexed PostgreSQL partition catalog directly.

        Args:
            topic: Kafka domain identifier.
            start_date: Optional inclusive start date (YYYY-MM-DD, datetime, or date).
            end_date: Optional inclusive end date (YYYY-MM-DD, datetime, or date).

        Returns:
            Chronologically sorted list of partition directory paths.

        Raises:
            BronzePartitionCatalogError: If query execution fails.
        """
        table_name = self.config.full_partition_table_name
        query = f"SELECT partition_path FROM {table_name} WHERE topic = %s"
        params: list[Any] = [topic]

        if start_date is not None:
            norm_start = _normalize_date(start_date)
            query += " AND MAKE_DATE(year, month, day) >= %s"
            params.append(norm_start)

        if end_date is not None:
            norm_end = _normalize_date(end_date)
            query += " AND MAKE_DATE(year, month, day) <= %s"
            params.append(norm_end)

        query += " ORDER BY year ASC, month ASC, day ASC, partition_path ASC;"

        try:
            with self._cursor() as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                if not rows:
                    return []
                return [row[0] if isinstance(row, (tuple, list)) else row["partition_path"] for row in rows]
        except psycopg.Error as exc:
            logger.error("Database error querying partitions", extra={"topic": topic, "error": str(exc)})
            raise BronzePartitionCatalogError(f"Error querying partitions for topic {topic}: {exc}") from exc

    def reconcile_partitions(self, partitions: list[dict[str, Any]]) -> int:
        """Batch register or reconcile a collection of partition metadata records.

        Args:
            partitions: List of dictionaries matching partition catalog columns.

        Returns:
            Count of successfully reconciled partition records.
        """
        success_count = 0
        for p in partitions:
            self.register_partition(
                topic=p["topic"],
                year=int(p["year"]),
                month=int(p["month"]),
                day=int(p["day"]),
                partition_path=p["partition_path"],
                record_count=int(p.get("record_count", 0)),
                file_count=int(p.get("file_count", 0)),
                total_bytes=int(p.get("total_bytes", 0)),
                min_event_timestamp=p.get("min_event_timestamp"),
                max_event_timestamp=p.get("max_event_timestamp"),
                min_kafka_offset=p.get("min_kafka_offset"),
                max_kafka_offset=p.get("max_kafka_offset"),
                is_compacted=bool(p.get("is_compacted", False)),
                status=str(p.get("status", "COMMITTED")),
            )
            success_count += 1
        return success_count

    def close(self) -> None:
        """Close managed database connection if owned by this manager instance."""
        if self._owns_connection and self._conn is not None:
            if not _is_closed(self._conn):
                self._conn.close()
                logger.info("Closed PostgreSQL metadata catalog connection")
            self._conn = None

    def __enter__(self) -> BronzeCatalogManager:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Exit context manager and release connection resources."""
        self.close()
