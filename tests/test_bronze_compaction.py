"""Unit test suite for Medallion Bronze small-file compaction engine and results."""

import shutil
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.bronze.compaction import (
    BronzeCompactionResult,
    BronzeCompactor,
    extract_topic_from_path,
)
from processing.bronze.config import BronzeStorageConfig

HAS_JAVA = shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide local SparkSession for BronzeCompactor unit testing if JVM is available."""
    if not HAS_JAVA:
        pytest.skip("Java runtime not available on host; SparkSession requires JVM")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("BronzeCompactorTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestBronzeCompactionResult:
    """Test suite for BronzeCompactionResult dataclass and serialization."""

    def test_compaction_result_to_dict(self) -> None:
        """Verify to_dict serialization and field consistency."""
        result = BronzeCompactionResult(
            batch_id="batch-cmp-100",
            partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
            topic="marketplace.orders",
            files_before=12,
            files_after=1,
            bytes_before=104857600,
            bytes_after=100000000,
            records_before=5000,
            records_after=5000,
            status="SUCCESS",
            duration_ms=250,
        )
        data = result.to_dict()
        assert data["batch_id"] == "batch-cmp-100"
        assert data["topic"] == "marketplace.orders"
        assert data["files_before"] == 12
        assert data["files_after"] == 1
        assert data["files_reduced"] == 11
        assert data["bytes_before"] == 104857600
        assert data["records_before"] == 5000
        assert data["records_after"] == 5000
        assert data["status"] == "SUCCESS"
        assert data["duration_ms"] == 250
        assert data["error_message"] is None

    def test_compaction_result_efficiency_properties(self) -> None:
        """Verify files_reduced, bytes_reduced, and compression_ratio properties."""
        result = BronzeCompactionResult(
            batch_id="b-1",
            partition_path="/data/part",
            topic="marketplace.payments",
            files_before=20,
            files_after=2,
            bytes_before=200000,
            bytes_after=180000,
            records_before=1000,
            records_after=1000,
            status="SUCCESS",
            duration_ms=100,
        )
        assert result.files_reduced == 18
        assert result.bytes_reduced == 20000
        assert result.compression_ratio == 0.9

        empty_result = BronzeCompactionResult(
            batch_id="b-2",
            partition_path="/data/part2",
            topic="marketplace.cart",
            files_before=0,
            files_after=0,
            bytes_before=0,
            bytes_after=0,
            records_before=0,
            records_after=0,
            status="SKIPPED_THRESHOLD",
            duration_ms=0,
        )
        assert empty_result.files_reduced == 0
        assert empty_result.compression_ratio == 1.0


class TestBronzeCompactorThresholds:
    """Test suite for should_compact threshold heuristics."""

    def test_should_compact_zero_or_negative_files(self) -> None:
        """Partitions with 0 files must never be compacted."""
        compactor = BronzeCompactor()
        assert compactor.should_compact("/path", file_count=0, total_bytes=0) is False
        assert compactor.should_compact("/path", file_count=-1, total_bytes=1000) is False

    def test_should_compact_single_file(self) -> None:
        """A single file does not require compaction unless threshold is 1."""
        config = BronzeStorageConfig(compaction_threshold_file_count=10, compaction_min_file_size_mb=32)
        compactor = BronzeCompactor(config=config)
        # Even if 1 file is small (e.g. 5MB < 32MB), coalescing 1 file into 1 file is a no-op
        assert compactor.should_compact("/path", file_count=1, total_bytes=5 * 1024 * 1024) is False

    def test_should_compact_above_file_count_threshold(self) -> None:
        """Partitions exceeding compaction_threshold_file_count must qualify for compaction."""
        config = BronzeStorageConfig(compaction_threshold_file_count=10)
        compactor = BronzeCompactor(config=config)
        # 10 files (>= 10) must return True even if files are large
        assert compactor.should_compact("/path", file_count=10, total_bytes=500 * 1024 * 1024) is True
        assert compactor.should_compact("/path", file_count=15, total_bytes=1024 * 1024 * 1024) is True

    def test_should_compact_small_average_file_size(self) -> None:
        """Partitions with average file size below compaction_min_file_size_mb must qualify."""
        config = BronzeStorageConfig(compaction_threshold_file_count=10, compaction_min_file_size_mb=32)
        compactor = BronzeCompactor(config=config)
        # 4 files with 40 MB total (avg 10 MB < 32 MB) -> True
        assert compactor.should_compact("/path", file_count=4, total_bytes=40 * 1024 * 1024) is True

    def test_should_compact_large_files_below_threshold(self) -> None:
        """Partitions with file count below threshold and average size >= min size must NOT qualify."""
        config = BronzeStorageConfig(compaction_threshold_file_count=10, compaction_min_file_size_mb=32)
        compactor = BronzeCompactor(config=config)
        # 3 files with 150 MB total (avg 50 MB >= 32 MB) -> False
        assert compactor.should_compact("/path", file_count=3, total_bytes=150 * 1024 * 1024) is False


class TestBronzeCompactorMocked:
    """Test suite for BronzeCompactor logic using mocked Spark DataFrames."""

    def test_extract_topic_from_path(self) -> None:
        """Verify regex topic extraction from canonical Bronze partition paths."""
        p1 = "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"
        assert extract_topic_from_path(p1) == "marketplace.orders"

        p2 = "/var/data/lakehouse-bronze/marketplace.payments/year=2026/month=11/day=02/"
        assert extract_topic_from_path(p2) == "marketplace.payments"

        p3 = "/unknown/directory/structure"
        assert extract_topic_from_path(p3) == "unknown"

    def test_compact_partition_skipped_threshold(self) -> None:
        """Verify compact_partition skips execution when partition does not meet threshold."""
        compactor = BronzeCompactor()
        with patch.object(compactor, "inspect_partition", return_value=(2, 100 * 1024 * 1024)):
            result = compactor.compact_partition(
                partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"
            )
            assert result.status == "SKIPPED_THRESHOLD"
            assert result.files_before == 2
            assert result.files_after == 2
            assert result.records_before == 0

    def test_compact_partition_dry_run(self) -> None:
        """Verify dry-run mode computes target parts and counts without staging write."""
        config = BronzeStorageConfig(dry_run=True, target_file_size_mb=128)
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 1500
        mock_spark.read.parquet.return_value = mock_df

        compactor = BronzeCompactor(config=config, spark=mock_spark)
        # 12 files, 300 MB -> target_parts = max(1, 300 // 128) = 2
        with patch.object(compactor, "inspect_partition", return_value=(12, 300 * 1024 * 1024)):
            result = compactor.compact_partition(
                partition_path="s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=02",
                batch_id="dry-cmp-1",
            )
            assert result.status == "SKIPPED_DRY_RUN"
            assert result.files_before == 12
            assert result.files_after == 2  # min(12, 2)
            assert result.records_before == 1500
            assert result.records_after == 1500
            assert mock_df.write.save.called is False

    def test_compact_partition_success_mocked(self) -> None:
        """Verify successful compaction workflow with coalesce, record verification, and swap."""
        config = BronzeStorageConfig(target_file_size_mb=128)
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 2000
        mock_spark.read.parquet.return_value = mock_df

        mock_writer = mock_df.coalesce.return_value.write.mode.return_value.format.return_value.option.return_value
        mock_writer.save = MagicMock()

        compactor = BronzeCompactor(config=config, spark=mock_spark)
        with (
            patch.object(compactor, "inspect_partition", return_value=(15, 120 * 1024 * 1024)),
            patch.object(compactor, "_swap_files", return_value=(1, 115 * 1024 * 1024)),
        ):
            result = compactor.compact_partition(
                partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
                batch_id="success-batch-1",
                force=True,
            )
            assert result.status == "SUCCESS"
            assert result.files_before == 15
            assert result.files_after == 1
            assert result.records_before == 2000
            assert result.records_after == 2000
            assert result.files_reduced == 14

    def test_compact_partition_record_mismatch_error(self) -> None:
        """Verify ValueError is raised and FAILED status returned if record counts diverge."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df_before = MagicMock(spec=DataFrame)
        mock_df_before.count.return_value = 1000

        mock_df_after = MagicMock(spec=DataFrame)
        mock_df_after.count.return_value = 998  # Mismatch!

        mock_spark.read.parquet.side_effect = [mock_df_before, mock_df_after]
        compactor = BronzeCompactor(spark=mock_spark)

        with (
            patch.object(compactor, "inspect_partition", return_value=(10, 50 * 1024 * 1024)),
            patch.object(compactor, "_cleanup_staging") as mock_cleanup,
        ):
            result = compactor.compact_partition(
                partition_path="s3a://lakehouse-bronze/events/marketplace.inventory/year=2026/month=11/day=02",
                force=True,
            )
            assert result.status == "FAILED"
            assert "Record count mismatch" in (result.error_message or "")
            mock_cleanup.assert_called_once()

    def test_compact_partition_write_failure_cleans_up(self) -> None:
        """Verify exception during write triggers staging cleanup and returns FAILED status."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 500
        mock_writer = mock_df.coalesce.return_value.write.mode.return_value.format.return_value.option.return_value
        mock_writer.save.side_effect = OSError("S3 Connection Timeout")
        mock_spark.read.parquet.return_value = mock_df

        compactor = BronzeCompactor(spark=mock_spark)
        with (
            patch.object(compactor, "inspect_partition", return_value=(10, 40 * 1024 * 1024)),
            patch.object(compactor, "_cleanup_staging") as mock_cleanup,
        ):
            result = compactor.compact_partition(
                partition_path="s3a://lakehouse-bronze/events/marketplace.navigation/year=2026/month=11/day=02",
                force=True,
            )
            assert result.status == "FAILED"
            assert "S3 Connection Timeout" in (result.error_message or "")
            mock_cleanup.assert_called_once()

    def test_compact_all_partitions_filtering(self) -> None:
        """Verify compact_all_partitions respects topic filter and processes partitions."""
        compactor = BronzeCompactor()
        partitions = [
            {
                "partition_path": "/data/marketplace.orders/year=2026/month=11/day=01",
                "topic": "marketplace.orders",
                "file_count": 1,
                "total_bytes": 128 * 1024 * 1024,
            },
            {
                "partition_path": "/data/marketplace.payments/year=2026/month=11/day=01",
                "topic": "marketplace.payments",
                "file_count": 1,
                "total_bytes": 128 * 1024 * 1024,
            },
        ]
        results = compactor.compact_all_partitions(partitions=partitions, topic="marketplace.orders")
        assert len(results) == 1
        assert results[0].topic == "marketplace.orders"
        assert results[0].status == "SKIPPED_THRESHOLD"


class TestBronzeCompactorLocalSwap:
    """Test suite verifying atomic staging file swap against local filesystem."""

    def test_swap_local_files_workflow(self, tmp_path: Path) -> None:
        """Verify _swap_local_files deletes fragmented files and promotes staged files."""
        part_dir = tmp_path / "partition"
        part_dir.mkdir()
        staging_dir = part_dir / "_compacting_staging_test"
        staging_dir.mkdir()

        # Create 3 fragmented files in partition directory
        (part_dir / "part-00001.parquet").write_bytes(b"data1" * 100)
        (part_dir / "part-00002.parquet").write_bytes(b"data2" * 100)
        (part_dir / "part-00003.parquet").write_bytes(b"data3" * 100)

        # Create 1 compacted file in staging directory
        (staging_dir / "part-compacted-00000.parquet").write_bytes(b"compacted_data" * 200)

        compactor = BronzeCompactor()
        file_count, total_bytes = compactor._swap_local_files(str(part_dir), str(staging_dir))

        assert file_count == 1
        assert total_bytes == len(b"compacted_data" * 200)
        assert (part_dir / "part-compacted-00000.parquet").exists()
        assert not (part_dir / "part-00001.parquet").exists()
        assert not (part_dir / "part-00002.parquet").exists()
        assert not (part_dir / "part-00003.parquet").exists()
        assert not staging_dir.exists()

    def test_swap_local_files_empty_staging_error(self, tmp_path: Path) -> None:
        """Verify ValueError is raised if staging directory contains no Parquet files."""
        part_dir = tmp_path / "part_err"
        part_dir.mkdir()
        staging_dir = part_dir / "_compacting_staging_err"
        staging_dir.mkdir()

        compactor = BronzeCompactor()
        with pytest.raises(ValueError, match="No compacted parquet files found"):
            compactor._swap_local_files(str(part_dir), str(staging_dir))


@pytest.mark.skipif(
    not HAS_JAVA,
    reason="Java not installed on host machine; PySpark execution runs inside Docker containers",
)
class TestBronzeCompactorLiveSpark:
    """Live Spark execution tests for Parquet file compaction when Java runtime is available."""

    def test_live_compaction_reduces_files_and_preserves_records(
        self,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("topic", StringType(), False),
            StructField("timestamp", LongType(), False),
        ])

        part_dir = tmp_path / "bronze_events" / "marketplace.orders" / "year=2026" / "month=11" / "day=02"
        part_dir.mkdir(parents=True)

        # Write 5 separate micro-batch Parquet files
        for i in range(5):
            df = spark.createDataFrame([(i, "marketplace.orders", 1785000000000 + i)], schema=schema)
            df.write.mode("append").format("parquet").save(str(part_dir))

        compactor = BronzeCompactor(spark=spark)
        files_before, _ = compactor.inspect_partition(str(part_dir))
        assert files_before >= 5

        # Execute compaction forcing coalesce to 1 file
        result = compactor.compact_partition(
            partition_path=str(part_dir),
            target_file_size_mb=128,
            force=True,
        )

        assert result.status == "SUCCESS"
        assert result.files_after == 1
        assert result.records_before == 5
        assert result.records_after == 5
        assert result.files_reduced >= 4
