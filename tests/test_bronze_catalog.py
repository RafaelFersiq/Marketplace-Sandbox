"""Unit test suite for Medallion BronzeCatalogManager.

Tests partition registration, ON CONFLICT upsert idempotency, batch ingestion audit
logging, uncompacted partition queries, compaction state updates, date range partition
pruning discovery, connection retry mechanisms, and database exception handling.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, call, patch

import psycopg
import pytest

from processing.bronze.catalog import (
    BronzeAuditCatalogError,
    BronzeCatalogManager,
    BronzeConnectionError,
    BronzePartitionCatalogError,
    _normalize_date,
)
from processing.bronze.config import BronzeStorageConfig


class TestBronzeCatalogManager:
    """Test suite verifying BronzeCatalogManager behavior with mocked database interactions."""

    def test_normalize_date_formats(self) -> None:
        """Verify _normalize_date handles strings, datetimes, and dates properly."""
        expected = date(2026, 11, 2)
        assert _normalize_date("2026-11-02") == expected
        assert _normalize_date("2026-11-02T15:30:00Z") == expected
        assert _normalize_date(datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC)) == expected
        assert _normalize_date(date(2026, 11, 2)) == expected

        with pytest.raises(ValueError, match="Invalid date format or type"):
            _normalize_date(12345)  # type: ignore[arg-type]

    def test_init_and_connection_properties(self) -> None:
        """Verify initialization with default and custom configs."""
        config = BronzeStorageConfig(postgres_host="test-db", postgres_port=5433)
        manager = BronzeCatalogManager(config=config)
        assert manager.config.postgres_host == "test-db"
        assert manager.config.postgres_port == 5433
        assert manager._conn is None
        assert manager._owns_connection is True

    @patch("psycopg.connect")
    def test_connection_retry_and_backoff(self, mock_connect: MagicMock) -> None:
        """Verify _connect retries with exponential backoff on transient errors."""
        mock_conn = MagicMock()
        mock_connect.side_effect = [
            psycopg.OperationalError("Connection refused"),
            psycopg.OperationalError("Connection timed out"),
            mock_conn,
        ]

        manager = BronzeCatalogManager()
        with patch("time.sleep") as mock_sleep:
            conn = manager._connect(max_retries=3, initial_backoff_seconds=0.1)
            assert conn == mock_conn
            assert mock_connect.call_count == 3
            assert mock_sleep.call_count == 2
            mock_sleep.assert_has_calls([call(0.1), call(0.2)])

    @patch("psycopg.connect")
    def test_connection_failure_max_retries(self, mock_connect: MagicMock) -> None:
        """Verify BronzeConnectionError is raised when max retries are exceeded."""
        mock_connect.side_effect = psycopg.OperationalError("Fatal connection error")

        manager = BronzeCatalogManager()
        with (
            patch("time.sleep"),
            pytest.raises(BronzeConnectionError, match="Failed to connect to PostgreSQL catalog"),
        ):
            manager._connect(max_retries=3, initial_backoff_seconds=0.01)

    def test_context_manager_lifecycle(self) -> None:
        """Verify context manager calls close on exit."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        manager = BronzeCatalogManager(conn=mock_conn)
        manager._owns_connection = True

        with manager as m:
            assert m == manager

        mock_conn.close.assert_called_once()
        assert manager._conn is None

    def test_register_partition_insert(self) -> None:
        """Verify partition registration executes parameterized upsert and returns ID."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (101,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        partition_id = manager.register_partition(
            topic="marketplace.orders",
            year=2026,
            month=11,
            day=2,
            partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
            record_count=5000,
            file_count=2,
            total_bytes=10485760,
            min_event_timestamp=datetime(2026, 11, 2, 0, 0, 0, tzinfo=UTC),
            max_event_timestamp=datetime(2026, 11, 2, 23, 59, 59, tzinfo=UTC),
            min_kafka_offset=100,
            max_kafka_offset=5100,
            is_compacted=False,
            status="COMMITTED",
        )

        assert partition_id == 101
        mock_cursor.execute.assert_called_once()
        query, params = mock_cursor.execute.call_args[0]
        assert "INSERT INTO lakehouse_metadata.bronze_partition_catalog" in query
        assert "ON CONFLICT (topic, year, month, day) DO UPDATE SET" in query
        assert "RETURNING id" in query
        assert params[0] == "marketplace.orders"
        assert params[1] == 2026
        assert params[2] == 11
        assert params[3] == 2
        assert params[5] == 5000
        assert params[6] == 2
        assert params[7] == 10485760
        mock_conn.commit.assert_called_once()

    def test_register_partition_db_error_handling(self) -> None:
        """Verify BronzePartitionCatalogError is raised and rollback executed on error."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.DatabaseError("Unique constraint violation")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        with pytest.raises(BronzePartitionCatalogError, match="Error registering partition"):
            manager.register_partition(
                topic="marketplace.orders",
                year=2026,
                month=11,
                day=2,
                partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
                record_count=10,
                file_count=1,
                total_bytes=1024,
            )
        mock_conn.rollback.assert_called_once()

    def test_log_ingestion_audit_success(self) -> None:
        """Verify batch ingestion audit log execution."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        manager.log_ingestion_audit(
            batch_id="batch-001",
            topic="marketplace.orders",
            source_landing_path="s3a://lakehouse-bronze/landing/marketplace.orders/batch-001.avro",
            target_partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
            records_ingested=1250,
            bytes_written=204800,
            files_written=1,
            records_failed=0,
            schema_ids_observed=[1, 2],
            execution_duration_ms=350,
            status="SUCCESS",
        )

        mock_cursor.execute.assert_called_once()
        query, params = mock_cursor.execute.call_args[0]
        assert "INSERT INTO lakehouse_metadata.bronze_ingestion_audit" in query
        assert "ON CONFLICT (batch_id) DO UPDATE SET" in query
        assert params[0] == "batch-001"
        assert params[1] == "marketplace.orders"
        assert params[4] == 1250
        assert params[8] == [1, 2]
        assert params[11] == 350
        assert params[12] == "SUCCESS"
        mock_conn.commit.assert_called_once()

    def test_log_ingestion_audit_error(self) -> None:
        """Verify BronzeAuditCatalogError raised on database error during audit logging."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg.Error("Disk full")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        with pytest.raises(BronzeAuditCatalogError, match="Error recording ingestion audit"):
            manager.log_ingestion_audit(
                batch_id="batch-err",
                topic="marketplace.orders",
                source_landing_path="s3a://landing",
                target_partition_path="s3a://target",
                records_ingested=0,
                bytes_written=0,
                files_written=0,
            )

    def test_get_uncompacted_partitions_default(self) -> None:
        """Verify query for uncompacted partitions with default threshold."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("topic",), ("file_count",), ("partition_path",)]
        mock_cursor.fetchall.return_value = [
            (1, "marketplace.orders", 15, "s3a://path/p1"),
            (2, "marketplace.payments", 12, "s3a://path/p2"),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        results = manager.get_uncompacted_partitions()

        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["topic"] == "marketplace.orders"
        assert results[0]["file_count"] == 15
        assert results[1]["file_count"] == 12

        query, params = mock_cursor.execute.call_args[0]
        assert "WHERE is_compacted = FALSE AND file_count >= %s" in query
        assert params == (10,)  # default threshold

    def test_get_uncompacted_partitions_with_filters(self) -> None:
        """Verify query for uncompacted partitions with topic and min_file_count filters."""
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("topic",), ("file_count",)]
        mock_cursor.fetchall.return_value = [(1, "marketplace.cart", 5)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        results = manager.get_uncompacted_partitions(topic="marketplace.cart", min_file_count=5)

        assert len(results) == 1
        query, params = mock_cursor.execute.call_args[0]
        assert "AND topic = %s" in query
        assert params == (5, "marketplace.cart")

    def test_get_uncompacted_partitions_empty(self) -> None:
        """Verify empty list returned when no uncompacted partitions exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        results = manager.get_uncompacted_partitions()
        assert results == []

    def test_mark_partition_compacted_success(self) -> None:
        """Verify marking partition as compacted updates file count and bytes."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        path = "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"
        success = manager.mark_partition_compacted(path, file_count=1, total_bytes=134217728)

        assert success is True
        query, params = mock_cursor.execute.call_args[0]
        assert "UPDATE lakehouse_metadata.bronze_partition_catalog" in query
        assert "is_compacted = TRUE" in query
        assert "last_compacted_at = CURRENT_TIMESTAMP" in query
        assert params == (1, 134217728, path)

    def test_mark_partition_compacted_not_found(self) -> None:
        """Verify mark_partition_compacted returns False when rowcount is 0."""
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        success = manager.mark_partition_compacted("s3a://non-existent", file_count=1, total_bytes=100)
        assert success is False

    def test_find_partitions_with_date_range(self) -> None:
        """Verify find_partitions applies MAKE_DATE bounds and returns path strings."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("s3a://path/year=2026/month=11/day=01",),
            ("s3a://path/year=2026/month=11/day=02",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        paths = manager.find_partitions(
            topic="marketplace.orders",
            start_date="2026-11-01",
            end_date="2026-11-02",
        )

        assert len(paths) == 2
        assert paths[0] == "s3a://path/year=2026/month=11/day=01"
        query, params = mock_cursor.execute.call_args[0]
        assert "MAKE_DATE(year, month, day) >= %s" in query
        assert "MAKE_DATE(year, month, day) <= %s" in query
        assert params[0] == "marketplace.orders"
        assert params[1] == date(2026, 11, 1)
        assert params[2] == date(2026, 11, 2)

    def test_find_partitions_no_dates(self) -> None:
        """Verify find_partitions without date bounds."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("s3a://path/year=2026/month=11/day=02",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        paths = manager.find_partitions(topic="marketplace.inventory")
        assert len(paths) == 1
        query, params = mock_cursor.execute.call_args[0]
        assert "MAKE_DATE" not in query
        assert params == ("marketplace.inventory",)

    def test_reconcile_partitions_batch(self) -> None:
        """Verify reconcile_partitions iterates and registers multiple partition dicts."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(10,), (11,)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        manager = BronzeCatalogManager(conn=mock_conn)
        partitions = [
            {
                "topic": "marketplace.orders",
                "year": 2026,
                "month": 11,
                "day": 1,
                "partition_path": "s3a://path/day=01",
                "record_count": 100,
            },
            {
                "topic": "marketplace.orders",
                "year": 2026,
                "month": 11,
                "day": 2,
                "partition_path": "s3a://path/day=02",
                "record_count": 200,
            },
        ]

        count = manager.reconcile_partitions(partitions)
        assert count == 2
        assert mock_cursor.execute.call_count == 2
