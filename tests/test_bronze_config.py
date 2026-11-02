"""Unit tests for Bronze storage configuration, CLI parser, and PySpark schema definitions."""

import pytest
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from processing.bronze.config import (
    DEFAULT_BRONZE_CHECKPOINT_DIR,
    DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB,
    DEFAULT_BRONZE_COMPRESSION,
    DEFAULT_BRONZE_LANDING_PATH,
    DEFAULT_BRONZE_LANDING_TTL_HOURS,
    DEFAULT_BRONZE_PARTITION_KEYS,
    DEFAULT_BRONZE_ROOT_PATH,
    DEFAULT_TOPICS,
    BronzeStorageConfig,
    build_bronze_arg_parser,
    parse_bronze_config,
)
from processing.bronze.schema import (
    BRONZE_METADATA_COLUMN_NAMES,
    BRONZE_METADATA_FIELDS,
    BRONZE_REQUIRED_METADATA_COLUMNS,
    BronzeSchemaValidationError,
    build_bronze_enveloped_schema,
    get_bronze_metadata_schema,
    get_bronze_partition_schema,
    validate_bronze_schema,
    validate_bronze_schema_or_raise,
)


class TestBronzeStorageConfig:
    """Test suite for BronzeStorageConfig dataclass, validation, and helper methods."""

    def test_default_config(self) -> None:
        config = BronzeStorageConfig()
        assert config.app_name == "MarketplaceBronzeProcessing"
        assert config.landing_path == DEFAULT_BRONZE_LANDING_PATH
        assert config.bronze_root_path == DEFAULT_BRONZE_ROOT_PATH
        assert config.checkpoint_dir == DEFAULT_BRONZE_CHECKPOINT_DIR
        assert config.topics == list(DEFAULT_TOPICS)
        assert config.partition_keys == list(DEFAULT_BRONZE_PARTITION_KEYS)
        assert config.compression == DEFAULT_BRONZE_COMPRESSION
        assert config.target_file_size_mb == DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB
        assert config.target_file_size_bytes == 128 * 1024 * 1024
        assert config.landing_ttl_hours == DEFAULT_BRONZE_LANDING_TTL_HOURS
        assert config.dry_run is False

        # PostgreSQL URLs & Table Identifiers
        assert config.postgres_jdbc_url == "jdbc:postgresql://localhost:5432/marketplace"
        assert config.full_partition_table_name == "lakehouse_metadata.bronze_partition_catalog"
        assert config.full_audit_table_name == "lakehouse_metadata.bronze_ingestion_audit"
        assert config.postgres_connection_dict["host"] == "localhost"
        assert config.postgres_connection_dict["port"] == 5432
        assert config.postgres_connection_dict["dbname"] == "marketplace"

        # S3 / MinIO paths
        assert config.get_topic_root_path("marketplace.orders") == "s3a://lakehouse-bronze/events/marketplace.orders"
        expected_part = "s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"
        assert config.get_topic_partition_path("marketplace.orders", 2026, 11, 2) == expected_part
        assert config.get_compaction_staging_path("marketplace.orders", 2026, 11, 2, "batch_123") == (
            f"{expected_part}/_compacting_staging_batch_123"
        )

        # Spark conf
        spark_conf = config.to_spark_conf()
        assert spark_conf["spark.app.name"] == "MarketplaceBronzeProcessing"
        assert spark_conf["spark.sql.parquet.compression.codec"] == "snappy"
        assert spark_conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert spark_conf["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"

    def test_custom_topics_and_normalization(self) -> None:
        config_str = BronzeStorageConfig(topics="marketplace.orders, marketplace.payments ")
        assert config_str.topics == ["marketplace.orders", "marketplace.payments"]

        config_list = BronzeStorageConfig(topics=[" marketplace.cart ", "marketplace.inventory "])
        assert config_list.topics == ["marketplace.cart", "marketplace.inventory"]

    def test_custom_partition_keys_and_normalization(self) -> None:
        config = BronzeStorageConfig(partition_keys="topic, year, month ")
        assert config.partition_keys == ["topic", "year", "month"]

    def test_trailing_slashes_stripped(self) -> None:
        config = BronzeStorageConfig(
            landing_path="s3a://lakehouse-bronze/landing///",
            bronze_root_path="s3a://lakehouse-bronze/events///",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints///",
            minio_endpoint="http://localhost:9000///",
        )
        assert config.landing_path == "s3a://lakehouse-bronze/landing"
        assert config.bronze_root_path == "s3a://lakehouse-bronze/events"
        assert config.checkpoint_dir == "s3a://lakehouse-bronze/checkpoints"
        assert config.minio_endpoint == "http://localhost:9000"

    def test_validation_invalid_topics(self) -> None:
        with pytest.raises(ValueError, match="Topics list must not be empty"):
            BronzeStorageConfig(topics=[])

        with pytest.raises(ValueError, match="Topics list must not be empty"):
            BronzeStorageConfig(topics=[" ", ""])

        with pytest.raises(ValueError, match="Topics must be a list of strings"):
            BronzeStorageConfig(topics=12345)  # type: ignore[arg-type]

    def test_validation_invalid_partition_keys(self) -> None:
        with pytest.raises(ValueError, match="Partition keys list must not be empty"):
            BronzeStorageConfig(partition_keys=[])

    def test_validation_invalid_compression(self) -> None:
        with pytest.raises(ValueError, match="Invalid compression codec 'bzip2'"):
            BronzeStorageConfig(compression="bzip2")

    def test_validation_invalid_numeric_bounds(self) -> None:
        with pytest.raises(ValueError, match="target_file_size_mb must be > 0"):
            BronzeStorageConfig(target_file_size_mb=0)

        with pytest.raises(ValueError, match="compaction_threshold_file_count must be > 0"):
            BronzeStorageConfig(compaction_threshold_file_count=0)

        with pytest.raises(ValueError, match="landing_ttl_hours must be >= 0"):
            BronzeStorageConfig(landing_ttl_hours=-1)

        with pytest.raises(ValueError, match="postgres_port must be between 1 and 65535"):
            BronzeStorageConfig(postgres_port=70000)


class TestBronzeArgParser:
    """Test suite for CLI argument parser and config parsing."""

    def test_default_cli_args(self) -> None:
        parser = build_bronze_arg_parser()
        config = parse_bronze_config(parser=parser, args=[])

        assert config.topics == list(DEFAULT_TOPICS)
        assert config.target_file_size_mb == 128
        assert config.dry_run is False

    def test_custom_cli_args(self) -> None:
        parser = build_bronze_arg_parser()
        cli_args = [
            "--topics",
            "marketplace.orders",
            "marketplace.payments",
            "--landing-path",
            "s3a://custom-bucket/landing",
            "--bronze-path",
            "s3a://custom-bucket/events",
            "--checkpoint-dir",
            "s3a://custom-bucket/chk",
            "--compression",
            "zstd",
            "--target-file-size-mb",
            "256",
            "--compaction-threshold-files",
            "20",
            "--landing-ttl-hours",
            "72",
            "--minio-endpoint",
            "http://minio-host:9000",
            "--postgres-host",
            "pg-host",
            "--postgres-port",
            "5433",
            "--postgres-db",
            "custom_lakehouse",
            "--master",
            "spark://spark-master:7077",
            "--shuffle-partitions",
            "8",
            "--dry-run",
        ]
        config = parse_bronze_config(parser=parser, args=cli_args)

        assert config.topics == ["marketplace.orders", "marketplace.payments"]
        assert config.landing_path == "s3a://custom-bucket/landing"
        assert config.bronze_root_path == "s3a://custom-bucket/events"
        assert config.checkpoint_dir == "s3a://custom-bucket/chk"
        assert config.compression == "zstd"
        assert config.target_file_size_mb == 256
        assert config.target_file_size_bytes == 256 * 1024 * 1024
        assert config.compaction_threshold_file_count == 20
        assert config.landing_ttl_hours == 72
        assert config.minio_endpoint == "http://minio-host:9000"
        assert config.postgres_host == "pg-host"
        assert config.postgres_port == 5433
        assert config.postgres_db == "custom_lakehouse"
        assert config.master == "spark://spark-master:7077"
        assert config.shuffle_partitions == 8
        assert config.dry_run is True


class TestBronzeSchema:
    """Test suite for PySpark schema envelope definitions and validation functions."""

    def test_get_bronze_metadata_schema(self) -> None:
        schema = get_bronze_metadata_schema()
        assert len(schema.fields) == 10
        field_names = [f.name for f in schema.fields]
        assert field_names == BRONZE_METADATA_COLUMN_NAMES

        # Verify specific critical types
        assert schema["_bronze_id"].dataType == StringType()
        assert schema["_bronze_id"].nullable is False
        assert schema["_ingested_at"].dataType == TimestampType()
        assert schema["_ingested_at"].nullable is False
        assert schema["_kafka_partition"].dataType == IntegerType()
        assert schema["_kafka_offset"].dataType == LongType()
        assert schema["_schema_id"].dataType == IntegerType()
        assert schema["_schema_version"].dataType == IntegerType()
        assert schema["_schema_version"].nullable is True
        assert schema["_payload_hash"].dataType == StringType()

    def test_get_bronze_partition_schema(self) -> None:
        schema_with_hour = get_bronze_partition_schema(include_hour=True)
        assert len(schema_with_hour.fields) == 4
        assert [f.name for f in schema_with_hour.fields] == ["year", "month", "day", "hour"]

        schema_no_hour = get_bronze_partition_schema(include_hour=False)
        assert len(schema_no_hour.fields) == 3
        assert [f.name for f in schema_no_hour.fields] == ["year", "month", "day"]

    def test_build_bronze_enveloped_schema(self) -> None:
        payload_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("total_amount", StringType(), False),
        ])
        enveloped = build_bronze_enveloped_schema(payload_schema, include_partition_cols=True)

        names = [f.name for f in enveloped.fields]
        # Payload preserved
        assert "order_id" in names
        assert "customer_id" in names
        assert "total_amount" in names

        # Metadata envelope present
        for col in BRONZE_METADATA_COLUMN_NAMES:
            assert col in names

        # Partition columns present
        for col in ("year", "month", "day", "hour"):
            assert col in names

    def test_validate_bronze_schema_success(self) -> None:
        payload_schema = StructType([StructField("event_id", StringType(), False)])
        enveloped = build_bronze_enveloped_schema(payload_schema, include_partition_cols=True)

        is_valid, errors = validate_bronze_schema(enveloped, require_partition_columns=True)
        assert is_valid is True
        assert errors == []

        # Should not raise
        validate_bronze_schema_or_raise(enveloped, require_partition_columns=True)

    def test_validate_bronze_schema_missing_fields(self) -> None:
        incomplete_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("_bronze_id", StringType(), False),
        ])
        is_valid, errors = validate_bronze_schema(incomplete_schema)
        assert is_valid is False
        assert len(errors) == len(BRONZE_REQUIRED_METADATA_COLUMNS) - 1

        with pytest.raises(BronzeSchemaValidationError) as exc_info:
            validate_bronze_schema_or_raise(incomplete_schema)
        assert len(exc_info.value.missing_fields) > 0

    def test_validate_bronze_schema_type_mismatch(self) -> None:
        # Schema with _kafka_partition as StringType instead of IntegerType
        fields = [f for f in BRONZE_METADATA_FIELDS if f.name != "_kafka_partition"]
        fields.append(StructField("_kafka_partition", StringType(), False))
        bad_type_schema = StructType(fields)

        is_valid, errors = validate_bronze_schema(bad_type_schema, check_data_types=True)
        assert is_valid is False
        assert any("incorrect type" in e and "_kafka_partition" in e for e in errors)
