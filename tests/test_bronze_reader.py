"""Unit test suite for Medallion BronzeDatasetReader.

Verifies catalog-driven partition pruning, direct topic path fallback, date range filtering,
SQL partition filter expressions, schema-on-read merge options, column projection,
catalog offline error resilience, and fast mocked Spark execution without requiring a local JVM.
"""

from __future__ import annotations

import shutil
from collections.abc import Generator
from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from processing.bronze.catalog import (
    BronzeCatalogManager,
    BronzeConnectionError,
    BronzePartitionCatalogError,
)
from processing.bronze.config import BronzeStorageConfig
from processing.bronze.reader import BronzeDatasetReader

HAS_JAVA = shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide local SparkSession for BronzeReader integration tests if JVM is available."""
    if not HAS_JAVA:
        pytest.skip("Java runtime not available on host; SparkSession requires JVM")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("BronzeReaderTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestBronzeDatasetReaderMocked:
    """Test suite for BronzeDatasetReader using mocked SparkSession and catalog."""

    def test_reader_init_defaults(self) -> None:
        """Verify default initialization parameters and lazy property instantiation."""
        reader = BronzeDatasetReader()
        assert reader.config is not None
        assert reader._spark is None
        assert reader._catalog is None
        assert reader._owns_catalog is True

    def test_reader_init_injected(self) -> None:
        """Verify initialization with injected configuration, spark, and catalog."""
        config = BronzeStorageConfig(bronze_root_path="s3a://test-bucket/events")
        mock_spark = MagicMock(spec=SparkSession)
        mock_catalog = MagicMock(spec=BronzeCatalogManager)

        reader = BronzeDatasetReader(config=config, spark=mock_spark, catalog=mock_catalog)
        assert reader.config.bronze_root_path == "s3a://test-bucket/events"
        assert reader.spark == mock_spark
        assert reader.catalog == mock_catalog
        assert reader._owns_catalog is False

    def test_resolve_partition_paths_via_catalog(self) -> None:
        """Verify catalog partition path resolution delegates to find_partitions."""
        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.return_value = [
            "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=01",
            "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
        ]

        reader = BronzeDatasetReader(catalog=mock_catalog)
        paths = reader.resolve_partition_paths(
            topic="marketplace.orders",
            start_date="2026-11-01",
            end_date="2026-11-02",
            use_catalog=True,
        )

        assert len(paths) == 2
        mock_catalog.find_partitions.assert_called_once_with(
            topic="marketplace.orders",
            start_date="2026-11-01",
            end_date="2026-11-02",
        )

    def test_resolve_partition_paths_disabled_catalog(self) -> None:
        """Verify use_catalog=False returns empty list immediately without querying catalog."""
        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        reader = BronzeDatasetReader(catalog=mock_catalog)

        paths = reader.resolve_partition_paths(topic="marketplace.orders", use_catalog=False)
        assert paths == []
        mock_catalog.find_partitions.assert_not_called()

    def test_resolve_partition_paths_catalog_error_fallback(self) -> None:
        """Verify catalog errors are gracefully caught and return empty list."""
        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.side_effect = BronzeConnectionError("Connection lost")

        reader = BronzeDatasetReader(catalog=mock_catalog)
        paths = reader.resolve_partition_paths(topic="marketplace.orders", use_catalog=True)

        assert paths == []
        mock_catalog.find_partitions.assert_called_once()

    def test_read_dataset_catalog_pruned_path(self) -> None:
        """Verify catalog-pruned path resolution passes partition paths directly to parquet reader."""
        mock_df = MagicMock(spec=DataFrame)
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        catalog_paths = [
            "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=01",
            "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
        ]
        mock_catalog.find_partitions.return_value = catalog_paths

        reader = BronzeDatasetReader(spark=mock_spark, catalog=mock_catalog)
        df = reader.read_dataset(
            topic="marketplace.orders",
            start_date="2026-11-01",
            end_date="2026-11-02",
            merge_schema=True,
            use_catalog=True,
        )

        assert df == mock_df
        mock_spark.read.option.assert_called_with("mergeSchema", "true")
        mock_spark.read.option.return_value.parquet.assert_called_once_with(*catalog_paths)

    def test_read_dataset_direct_path_when_catalog_disabled(self) -> None:
        """Verify use_catalog=False reads direct topic root path."""
        mock_df = MagicMock(spec=DataFrame)
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        mock_catalog = MagicMock(spec=BronzeCatalogManager)

        reader = BronzeDatasetReader(config=config, spark=mock_spark, catalog=mock_catalog)
        df = reader.read_dataset(
            topic="marketplace.payments",
            use_catalog=False,
            merge_schema=False,
        )

        assert df == mock_df
        mock_catalog.find_partitions.assert_not_called()
        mock_spark.read.option.assert_called_with("mergeSchema", "false")
        mock_spark.read.option.return_value.parquet.assert_called_once_with(
            "s3a://lakehouse-bronze/events/marketplace.payments"
        )

    def test_read_dataset_direct_path_date_range_filtering(self) -> None:
        """Verify direct mode applies date filter expressions on year, month, day columns."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.filter.return_value = mock_df
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        reader = BronzeDatasetReader(spark=mock_spark)

        with (
            patch("pyspark.sql.functions.make_date") as mock_make_date,
            patch("pyspark.sql.functions.col"),
            patch("pyspark.sql.functions.lit") as mock_lit,
        ):
            mock_make_date.return_value.__ge__.return_value = MagicMock()
            mock_make_date.return_value.__le__.return_value = MagicMock()
            df = reader.read_dataset(
                topic="marketplace.orders",
                start_date="2026-11-01",
                end_date="2026-11-03",
                use_catalog=False,
            )

            assert df == mock_df
            assert mock_df.filter.call_count == 2
            assert mock_make_date.call_count == 2
            mock_lit.assert_has_calls([call("2026-11-01"), call("2026-11-03")], any_order=True)

    def test_read_dataset_custom_partition_filter_and_projection(self) -> None:
        """Verify custom SQL partition_filter and column projections are applied."""
        mock_df = MagicMock(spec=DataFrame)
        mock_filtered = MagicMock(spec=DataFrame)
        mock_projected = MagicMock(spec=DataFrame)

        mock_df.filter.return_value = mock_filtered
        mock_filtered.select.return_value = mock_projected

        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.return_value = [
            "s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=02"
        ]

        reader = BronzeDatasetReader(spark=mock_spark, catalog=mock_catalog)
        result_df = reader.read_dataset(
            topic="marketplace.cart",
            partition_filter="year = 2026 AND month = 11",
            columns=["_bronze_id", "_source_topic", "cart_id"],
        )

        assert result_df == mock_projected
        mock_df.filter.assert_called_once_with("year = 2026 AND month = 11")
        mock_filtered.select.assert_called_once_with("_bronze_id", "_source_topic", "cart_id")

    def test_read_dataset_graceful_catalog_empty_fallback(self) -> None:
        """Verify fallback to direct root scan when catalog returns empty partitions."""
        mock_df = MagicMock(spec=DataFrame)
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.return_value = []

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        reader = BronzeDatasetReader(config=config, spark=mock_spark, catalog=mock_catalog)

        df = reader.read_dataset(topic="marketplace.inventory", use_catalog=True)
        assert df == mock_df
        mock_spark.read.option.return_value.parquet.assert_called_once_with(
            "s3a://lakehouse-bronze/events/marketplace.inventory"
        )

    def test_read_dataset_graceful_catalog_offline_fallback(self) -> None:
        """Verify fallback to direct root scan when catalog throws BronzePartitionCatalogError."""
        mock_df = MagicMock(spec=DataFrame)
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.side_effect = BronzePartitionCatalogError("Catalog offline")

        config = BronzeStorageConfig(bronze_root_path="s3a://lakehouse-bronze/events")
        reader = BronzeDatasetReader(config=config, spark=mock_spark, catalog=mock_catalog)

        df = reader.read_dataset(topic="marketplace.catalog", use_catalog=True)
        assert df == mock_df
        mock_spark.read.option.return_value.parquet.assert_called_once_with(
            "s3a://lakehouse-bronze/events/marketplace.catalog"
        )

    def test_read_multi_topic(self) -> None:
        """Verify read_multi_topic returns dictionary mapping topic names to DataFrames."""
        mock_df_1 = MagicMock(spec=DataFrame)
        mock_df_2 = MagicMock(spec=DataFrame)
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.read.option.return_value.parquet.side_effect = [mock_df_1, mock_df_2]

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.side_effect = [
            ["s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"],
            ["s3a://lakehouse-bronze/events/marketplace.payments/year=2026/month=11/day=02"],
        ]

        reader = BronzeDatasetReader(spark=mock_spark, catalog=mock_catalog)
        dfs = reader.read_multi_topic(topics=["marketplace.orders", "marketplace.payments"])

        assert set(dfs.keys()) == {"marketplace.orders", "marketplace.payments"}
        assert dfs["marketplace.orders"] == mock_df_1
        assert dfs["marketplace.payments"] == mock_df_2

    def test_get_partition_count(self) -> None:
        """Verify get_partition_count returns count of resolved partition URIs."""
        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.return_value = ["path1", "path2", "path3"]

        reader = BronzeDatasetReader(catalog=mock_catalog)
        count = reader.get_partition_count("marketplace.navigation")
        assert count == 3

    def test_context_manager_lifecycle(self) -> None:
        """Verify context manager closes catalog on exit when owned."""
        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        reader = BronzeDatasetReader(catalog=mock_catalog)
        reader._owns_catalog = True

        with reader as r:
            assert r == reader

        mock_catalog.close.assert_called_once()
        assert reader._catalog is None


@pytest.mark.skipif(not HAS_JAVA, reason="Java JVM runtime not available on host")
class TestBronzeDatasetReaderJVM:
    """Integration tests running against real SparkSession when Java is available."""

    def test_read_dataset_local_parquet_directory(
        self, spark: SparkSession, tmp_path: pytest.TempPathFactory
    ) -> None:
        """Verify read_dataset correctly loads and filters local Parquet directory."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("_bronze_id", StringType(), False),
            StructField("order_id", StringType(), False),
        ])

        data = [
            ("marketplace.orders", 2026, 11, 1, "uuid-1", "ord-101"),
            ("marketplace.orders", 2026, 11, 2, "uuid-2", "ord-102"),
            ("marketplace.orders", 2026, 11, 3, "uuid-3", "ord-103"),
        ]

        df = spark.createDataFrame(data, schema=schema)
        out_path = str(tmp_path / "events")
        df.write.partitionBy("topic", "year", "month", "day").parquet(out_path)

        config = BronzeStorageConfig(bronze_root_path=out_path)
        reader = BronzeDatasetReader(config=config, spark=spark)

        # Read direct without catalog
        read_df = reader.read_dataset(
            topic="marketplace.orders",
            start_date="2026-11-02",
            end_date="2026-11-03",
            use_catalog=False,
        )

        rows = read_df.collect()
        assert len(rows) == 2
        order_ids = {r["order_id"] for r in rows}
        assert order_ids == {"ord-102", "ord-103"}
