"""Unit tests for Spark Streaming Configuration and CLI argument parser."""

import pytest

from processing.streaming.config import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_STREAMING_TOPICS,
    SparkStreamingConfig,
    build_streaming_arg_parser,
    parse_streaming_config,
)


class TestSparkStreamingConfig:
    """Test suite for SparkStreamingConfig dataclass, validation, and helper methods."""

    def test_default_config(self) -> None:
        config = SparkStreamingConfig()
        assert config.app_name == "MarketplaceStreamingIngestion"
        assert config.topics == list(DEFAULT_STREAMING_TOPICS)
        assert config.dlq_topic == DEFAULT_DLQ_TOPIC
        assert config.starting_offsets == "earliest"
        assert config.max_offsets_per_trigger == 5000
        assert config.trigger_interval == "10 seconds"
        assert config.watermark_delay == "10 minutes"
        assert config.fail_on_data_loss is False
        assert config.once is False

        # Kafka read options
        kafka_opts = config.to_kafka_read_options()
        assert kafka_opts["kafka.bootstrap.servers"] == config.bootstrap_servers
        assert kafka_opts["subscribe"] == ",".join(DEFAULT_STREAMING_TOPICS)
        assert kafka_opts["startingOffsets"] == "earliest"
        assert kafka_opts["maxOffsetsPerTrigger"] == "5000"
        assert kafka_opts["failOnDataLoss"] == "false"

        # Spark conf
        spark_conf = config.to_spark_conf()
        assert spark_conf["spark.app.name"] == "MarketplaceStreamingIngestion"
        assert spark_conf["spark.streaming.stopGracefullyOnShutdown"] == "true"
        assert spark_conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
        assert spark_conf["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        assert spark_conf["spark.sql.session.timeZone"] == "UTC"

        # Checkpoint paths
        assert config.get_landing_checkpoint_path() == f"{config.checkpoint_dir}/landing"
        assert config.get_dlq_storage_checkpoint_path() == f"{config.checkpoint_dir}/dlq_storage"
        assert config.get_dlq_kafka_checkpoint_path() == f"{config.checkpoint_dir}/dlq_kafka"

    def test_custom_topics_and_string_normalization(self) -> None:
        # String topics input
        config_str = SparkStreamingConfig(topics="marketplace.orders, marketplace.payments ")
        assert config_str.topics == ["marketplace.orders", "marketplace.payments"]
        assert config_str.kafka_subscribe_pattern() == "marketplace.orders,marketplace.payments"

        # List with whitespace
        config_list = SparkStreamingConfig(topics=[" marketplace.cart ", " marketplace.inventory "])
        assert config_list.topics == ["marketplace.cart", "marketplace.inventory"]

    def test_trailing_slashes_stripped(self) -> None:
        config = SparkStreamingConfig(
            landing_path="s3a://lakehouse-bronze/landing///",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints///",
            dlq_storage_path="s3a://lakehouse-dlq/events///",
            schema_registry_url="http://localhost:8081///",
            minio_endpoint="http://localhost:9000///",
        )
        assert config.landing_path == "s3a://lakehouse-bronze/landing"
        assert config.checkpoint_dir == "s3a://lakehouse-bronze/checkpoints"
        assert config.dlq_storage_path == "s3a://lakehouse-dlq/events"
        assert config.schema_registry_url == "http://localhost:8081"
        assert config.minio_endpoint == "http://localhost:9000"

    def test_validation_empty_topics_raises(self) -> None:
        with pytest.raises(ValueError, match="Topics list must not be empty"):
            SparkStreamingConfig(topics=[])

        with pytest.raises(ValueError, match="Topics list must not be empty"):
            SparkStreamingConfig(topics=["  ", ""])

    def test_validation_invalid_topics_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Topics must be a list of strings or comma-separated string"):
            SparkStreamingConfig(topics=12345)  # type: ignore[arg-type]

    def test_validation_invalid_starting_offsets_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid starting_offsets 'beginning'"):
            SparkStreamingConfig(starting_offsets="beginning")

    def test_validation_valid_starting_offsets_json(self) -> None:
        json_offsets = '{"marketplace.orders": {"0": 100}}'
        config = SparkStreamingConfig(starting_offsets=json_offsets)
        assert config.starting_offsets == json_offsets

    def test_validation_invalid_numeric_bounds(self) -> None:
        with pytest.raises(ValueError, match="max_offsets_per_trigger must be > 0"):
            SparkStreamingConfig(max_offsets_per_trigger=0)

        with pytest.raises(ValueError, match="shuffle_partitions must be > 0"):
            SparkStreamingConfig(shuffle_partitions=0)

        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            SparkStreamingConfig(max_retries=-1)

        with pytest.raises(ValueError, match="initial_backoff_seconds must be > 0"):
            SparkStreamingConfig(initial_backoff_seconds=0.0)

        with pytest.raises(ValueError, match=r"max_backoff_seconds .* must be >= initial_backoff_seconds"):
            SparkStreamingConfig(initial_backoff_seconds=10.0, max_backoff_seconds=5.0)


class TestStreamingArgParser:
    """Test suite for CLI argument parser and config parsing."""

    def test_default_cli_args(self) -> None:
        parser = build_streaming_arg_parser()
        config = parse_streaming_config(parser=parser, args=[])

        assert config.topics == list(DEFAULT_STREAMING_TOPICS)
        assert config.once is False

    def test_custom_cli_args(self) -> None:
        parser = build_streaming_arg_parser()
        cli_args = [
            "--topics",
            "marketplace.orders",
            "marketplace.payments",
            "--bootstrap-servers",
            "broker1:9092,broker2:9092",
            "--schema-registry-url",
            "http://registry:8081",
            "--landing-path",
            "s3a://bronze/landing",
            "--checkpoint-dir",
            "s3a://bronze/chk",
            "--dlq-path",
            "s3a://dlq/events",
            "--dlq-topic",
            "custom.dlq",
            "--watermark-delay",
            "15 minutes",
            "--trigger-interval",
            "5 seconds",
            "--starting-offsets",
            "latest",
            "--max-offsets-per-trigger",
            "2500",
            "--master",
            "spark://spark-master:7077",
            "--shuffle-partitions",
            "8",
            "--fail-on-data-loss",
            "--once",
        ]
        config = parse_streaming_config(parser=parser, args=cli_args)

        assert config.topics == ["marketplace.orders", "marketplace.payments"]
        assert config.bootstrap_servers == "broker1:9092,broker2:9092"
        assert config.schema_registry_url == "http://registry:8081"
        assert config.landing_path == "s3a://bronze/landing"
        assert config.checkpoint_dir == "s3a://bronze/chk"
        assert config.dlq_storage_path == "s3a://dlq/events"
        assert config.dlq_topic == "custom.dlq"
        assert config.watermark_delay == "15 minutes"
        assert config.trigger_interval == "5 seconds"
        assert config.starting_offsets == "latest"
        assert config.max_offsets_per_trigger == 2500
        assert config.master == "spark://spark-master:7077"
        assert config.shuffle_partitions == 8
        assert config.fail_on_data_loss is True
        assert config.once is True

    def test_cli_comma_separated_topics(self) -> None:
        parser = build_streaming_arg_parser()
        cli_args = ["--topics", "marketplace.orders,marketplace.payments"]
        config = parse_streaming_config(parser=parser, args=cli_args)
        assert config.topics == ["marketplace.orders", "marketplace.payments"]

    def test_cli_continuous_flag(self) -> None:
        parser = build_streaming_arg_parser()
        cli_args = ["--continuous"]
        config = parse_streaming_config(parser=parser, args=cli_args)
        assert config.once is False
