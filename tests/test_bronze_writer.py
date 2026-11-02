"""Unit test suite for Medallion BronzeTableWriter and BronzeWriteResult."""

import shutil
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.bronze.config import BronzeStorageConfig
from processing.bronze.enrichment import enrich_with_bronze_envelope
from processing.bronze.writer import BronzeTableWriter, BronzeWriteResult

HAS_JAVA = shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide local SparkSession for BronzeTableWriter unit testing if Java is available."""
    if not HAS_JAVA:
        pytest.skip("Java runtime not available on host; SparkSession requires JVM")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("BronzeWriterTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestBronzeTableWriterMocked:
    """Test suite for BronzeTableWriter using mocked DataFrames and pure Python logic."""

    def test_write_result_to_dict_serialization(self) -> None:
        """Verify BronzeWriteResult.to_dict() produces valid serializable dictionary."""
        result = BronzeWriteResult(
            batch_id="b-123",
            topic="marketplace.orders",
            record_count=100,
            partitions_written=["s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"],
            bytes_written=102400,
            files_written=1,
            status="SUCCESS",
            duration_ms=45,
        )
        d = result.to_dict()
        assert d["batch_id"] == "b-123"
        assert d["record_count"] == 100
        assert d["status"] == "SUCCESS"
        assert d["duration_ms"] == 45
        assert d["error_message"] is None

    def test_write_batch_empty_dataframe(self) -> None:
        """Verify empty DataFrame gracefully returns SUCCESS without calling writer."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 0
        mock_df.columns = ["topic", "year", "month", "day"]

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        writer = BronzeTableWriter(config=config)

        result = writer.write_batch(mock_df, batch_id="empty-b-1", topic="marketplace.orders")

        assert result.status == "SUCCESS"
        assert result.record_count == 0
        assert result.partitions_written == []
        assert result.files_written == 0
        assert mock_df.write.save.called is False

    def test_write_batch_dry_run_mode(self) -> None:
        """Verify dry-run mode returns planned partition paths without executing Parquet write."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 5
        mock_df.columns = ["topic", "year", "month", "day"]

        # Mock distinct partition row
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"topic": "marketplace.orders", "year": 2026, "month": 11, "day": 2}
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_row]

        config = BronzeStorageConfig(
            bronze_root_path="s3a://lakehouse-bronze/events",
            dry_run=True,
        )
        writer = BronzeTableWriter(config=config)

        result = writer.write_batch(mock_df, batch_id="dry-run-1")

        assert result.status == "SKIPPED_DRY_RUN"
        assert result.record_count == 5
        assert len(result.partitions_written) == 1
        assert "marketplace.orders/year=2026/month=11/day=02" in result.partitions_written[0]
        assert result.files_written == 0
        assert result.bytes_written == 0
        assert mock_df.write.save.called is False

    def test_write_batch_immutability_append_mode(self) -> None:
        """Verify write_batch sets append mode and saves to target root path."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 3
        mock_df.dropDuplicates.return_value = mock_df
        mock_df.columns = ["topic", "year", "month", "day", "_payload_hash"]

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"topic": "marketplace.payments", "year": 2026, "month": 11, "day": 2}
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_row]

        mock_save = MagicMock()
        mock_writer = mock_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value
        mock_writer.save = mock_save

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        writer = BronzeTableWriter(config=config)

        result = writer.write_batch(
            mock_df,
            batch_id="append-b-1",
            deduplicate_by_payload_hash=True,
        )

        assert result.status == "SUCCESS"
        assert result.record_count == 3
        mock_df.write.mode.assert_called_with("append")
        mock_df.dropDuplicates.assert_called_with(["_payload_hash"])

    def test_write_batch_error_handling(self) -> None:
        """Verify write exception is captured in result with FAILED status."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 2
        mock_df.columns = ["topic", "year", "month", "day"]

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"topic": "marketplace.cart", "year": 2026, "month": 11, "day": 2}
        mock_df.select.return_value.distinct.return_value.collect.return_value = [mock_row]

        mock_writer = mock_df.write.mode.return_value.format.return_value.option.return_value.partitionBy.return_value
        mock_writer.save.side_effect = OSError("S3 Connection refused")

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        writer = BronzeTableWriter(config=config)

        result = writer.write_batch(mock_df, batch_id="err-batch")

        assert result.status == "FAILED"
        assert "S3 Connection refused" in (result.error_message or "")
        assert result.record_count == 2


@pytest.mark.skipif(
    not HAS_JAVA,
    reason="Java not installed on host machine; PySpark execution runs inside Docker containers",
)
class TestBronzeTableWriterLiveSpark:
    """Live Spark execution tests for Parquet file generation (runs when Java runtime is available)."""

    def test_write_batch_success_and_partition_layout(
        self,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("schema_id", IntegerType(), False),
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp_ms", LongType(), False),
            StructField("payload_json", StringType(), False),
        ])
        data = [
            ("marketplace.orders", 0, 1, 101, "evt-01", "ORDER_CREATED", 1785000000000, '{"order_id": "ord-1"}'),
            ("marketplace.orders", 0, 2, 101, "evt-02", "ORDER_CREATED", 1785086400000, '{"order_id": "ord-2"}'),
        ]
        raw_df = spark.createDataFrame(data, schema=schema)
        enriched_df = enrich_with_bronze_envelope(raw_df, batch_id="live-writer-test")

        bronze_root = str(tmp_path / "bronze_events")
        config = BronzeStorageConfig(bronze_root_path=bronze_root)
        writer = BronzeTableWriter(config=config, spark=spark)

        result = writer.write_batch(df=enriched_df, topic="marketplace.orders", batch_id="batch-001")
        assert result.status == "SUCCESS"
        assert result.record_count == 2
        assert len(result.partitions_written) == 2
