"""Unit test suite for Bronze technical metadata enrichment and partition derivation."""

import hashlib
import shutil
import uuid
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing.bronze.enrichment import (
    EMPTY_SHA256_HEX,
    BronzeMetadataEnricher,
    enrich_with_bronze_envelope,
)
from processing.bronze.schema import (
    BRONZE_METADATA_COLUMN_NAMES,
    BRONZE_REQUIRED_METADATA_COLUMNS,
    BronzeSchemaValidationError,
    build_bronze_enveloped_schema,
    validate_bronze_schema,
    validate_bronze_schema_or_raise,
)

HAS_JAVA = shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide local SparkSession for Bronze enrichment unit testing if Java is available."""
    if not HAS_JAVA:
        pytest.skip("Java runtime not available on host; SparkSession requires JVM")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("BronzeEnrichmentTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestBronzeEnrichmentMocked:
    """Test suite for BronzeMetadataEnricher using pure-Python and mocked DataFrame objects."""

    def test_enricher_init_defaults(self) -> None:
        """Verify default initialization parameters."""
        enricher = BronzeMetadataEnricher()
        assert enricher.batch_id is None
        assert enricher.topic is None
        assert enricher.processing_host is not None
        assert enricher.schema_version_lookup == {}
        assert enricher.domain_schema is None
        assert enricher.unpack_payload is False
        assert enricher.include_hour_partition is True
        assert enricher.validate_contract is True

    def test_empty_sha256_constant(self) -> None:
        """Verify empty SHA-256 fallback matches standard hashlib."""
        expected = hashlib.sha256(b"").hexdigest()
        assert expected == EMPTY_SHA256_HEX

    def test_enrich_invokes_column_transforms(self) -> None:
        """Verify enrich() chains withColumn calls for all 10 metadata columns and partition columns."""
        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = ["payload_json", "event_timestamp_ms", "partition", "offset", "schema_id"]

        # Chain withColumn calls
        mock_df.withColumn.return_value = mock_df

        mock_f = MagicMock()
        mock_col = MagicMock()
        mock_f.col.return_value = mock_col
        mock_f.lit.return_value = mock_col
        mock_f.expr.return_value = mock_col
        mock_f.current_timestamp.return_value = mock_col
        mock_f.sha2.return_value = mock_col
        mock_f.coalesce.return_value = mock_col
        mock_f.year.return_value = mock_col
        mock_f.month.return_value = mock_col
        mock_f.dayofmonth.return_value = mock_col
        mock_f.hour.return_value = mock_col
        mock_f.to_timestamp.return_value = mock_col
        mock_f.when.return_value = mock_f
        mock_f.otherwise.return_value = mock_col

        enricher = BronzeMetadataEnricher(
            batch_id="test-batch-uuid",
            topic="marketplace.orders",
            processing_host="worker-01",
            schema_version_lookup={1: 2},
            validate_contract=False,  # Skip Spark schema contract check on mock DataFrame
        )

        with patch("processing.bronze.enrichment.F", mock_f):
            result_df = enricher.enrich(mock_df)

        assert result_df == mock_df

        # Verify withColumn was invoked for all technical envelope columns
        with_col_names = [call.args[0] for call in mock_df.withColumn.call_args_list]
        for col_name in BRONZE_METADATA_COLUMN_NAMES:
            assert col_name in with_col_names, f"Expected {col_name} to be added via withColumn"

        # Verify partition columns added
        for part_col in ("year", "month", "day", "hour", "topic"):
            assert part_col in with_col_names, f"Expected {part_col} to be added via withColumn"

    def test_schema_contract_validation_pure_python(self) -> None:
        """Verify schema contract validation against valid and invalid StructTypes."""
        payload_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("amount", DoubleType(), False),
        ])
        enveloped_schema = build_bronze_enveloped_schema(payload_schema, include_partition_cols=True)

        is_valid, errors = validate_bronze_schema(enveloped_schema, require_partition_columns=True)
        assert is_valid is True
        assert errors == []

        validate_bronze_schema_or_raise(enveloped_schema, require_partition_columns=True)

    def test_schema_contract_validation_missing_columns(self) -> None:
        """Verify validation fails when mandatory metadata fields are omitted."""
        incomplete_schema = StructType([
            StructField("_bronze_id", StringType(), False),
            StructField("order_id", StringType(), False),
        ])
        is_valid, errors = validate_bronze_schema(incomplete_schema)
        assert is_valid is False
        assert len(errors) == len(BRONZE_REQUIRED_METADATA_COLUMNS) - 1

        with pytest.raises(BronzeSchemaValidationError) as exc_info:
            validate_bronze_schema_or_raise(incomplete_schema)
        assert len(exc_info.value.missing_fields) > 0


@pytest.mark.skipif(
    not HAS_JAVA,
    reason="Java not installed on host machine; PySpark execution runs inside Docker containers",
)
class TestBronzeEnrichmentLiveSpark:
    """Live Spark execution tests (runs when Java runtime is available)."""

    def test_metadata_envelope_injection(self, spark: SparkSession) -> None:
        """Verify all 10 canonical metadata columns and partition columns are injected in live Spark."""
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
            (
                "marketplace.orders",
                0,
                101,
                1,
                "evt-001",
                "ORDER_CREATED",
                1785000000000,
                '{"order_id": "ord-1", "total": 150.0}',
            )
        ]
        df = spark.createDataFrame(data, schema=schema)
        batch_id = "test-batch-101"

        enriched = enrich_with_bronze_envelope(
            df=df,
            batch_id=batch_id,
            processing_host="test-worker-01",
            schema_version_lookup={1: 3},
        )

        for col_name in BRONZE_METADATA_COLUMN_NAMES:
            assert col_name in enriched.columns

        for part_col in ("year", "month", "day", "hour"):
            assert part_col in enriched.columns

        row = enriched.collect()[0]
        parsed_uuid = uuid.UUID(row["_bronze_id"])
        assert str(parsed_uuid) == row["_bronze_id"]
        assert row["_source_topic"] == "marketplace.orders"
        assert row["_kafka_partition"] == 0
        assert row["_kafka_offset"] == 101
        assert row["_schema_id"] == 1
        assert row["_schema_version"] == 3
        assert row["_ingestion_batch_id"] == batch_id
        assert row["_processing_host"] == "test-worker-01"

        validate_bronze_schema_or_raise(enriched, require_partition_columns=True)

    def test_sha256_payload_determinism(self, spark: SparkSession) -> None:
        """Verify _payload_hash produces exact SHA-256 digest matching standard hashlib."""
        payload = '{"event_id": "test-sha", "amount": 250.75, "status": "APPROVED"}'
        expected_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("payload_json", StringType(), False),
            StructField("event_timestamp_ms", LongType(), False),
        ])
        df = spark.createDataFrame([("marketplace.payments", payload, 1785000000000)], schema=schema)

        enriched = enrich_with_bronze_envelope(df=df, batch_id="sha-batch")
        row = enriched.collect()[0]
        assert row["_payload_hash"] == expected_hash

    def test_partition_derivation_from_event_timestamp(self, spark: SparkSession) -> None:
        """Verify partition keys (year, month, day, hour) are derived deterministically in UTC."""
        epoch_ms = 1785000000000  # 2026-07-26 17:20:00 UTC
        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("payload_json", StringType(), False),
            StructField("event_timestamp_ms", LongType(), False),
        ])
        df = spark.createDataFrame([("marketplace.cart", "{}", epoch_ms)], schema=schema)

        enriched = enrich_with_bronze_envelope(df=df, batch_id="part-batch")
        row = enriched.collect()[0]
        assert row["year"] == 2026
        assert row["month"] == 7
        assert row["day"] == 26
        assert row["hour"] == 17

    def test_null_event_timestamp_fallback_to_ingested_at(self, spark: SparkSession) -> None:
        """Verify null event timestamp falls back to _ingested_at for partition keys."""
        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("payload_json", StringType(), False),
            StructField("event_timestamp_ms", LongType(), True),
        ])
        df = spark.createDataFrame([("marketplace.inventory", "{}", None)], schema=schema)

        enriched = enrich_with_bronze_envelope(df=df, batch_id="null-ts-batch")
        row = enriched.collect()[0]
        assert row["year"] is not None
        assert row["month"] is not None
        assert row["day"] is not None
        assert row["year"] >= 2026

    def test_domain_payload_unpacking(self, spark: SparkSession) -> None:
        """Verify optional domain payload unpacking retains payload_json while unpacking columns."""
        payload = '{"product_id": "prod-99", "price": 49.99, "in_stock": true}'
        domain_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("price", DoubleType(), True),
        ])
        schema = StructType([
            StructField("topic", StringType(), False),
            StructField("payload_json", StringType(), False),
            StructField("event_timestamp_ms", LongType(), False),
        ])
        df = spark.createDataFrame([("marketplace.catalog", payload, 1785000000000)], schema=schema)

        enricher = BronzeMetadataEnricher(
            batch_id="unpack-batch",
            domain_schema=domain_schema,
            unpack_payload=True,
        )
        enriched = enricher.enrich(df)
        assert "product_id" in enriched.columns
        assert "price" in enriched.columns
        assert "payload_json" in enriched.columns

        row = enriched.collect()[0]
        assert row["product_id"] == "prod-99"
        assert abs(row["price"] - 49.99) < 0.001
