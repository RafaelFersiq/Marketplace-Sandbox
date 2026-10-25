"""PySpark Structured Streaming Ingestion Pipeline Orchestrator.

Unifies multi-topic Kafka consumption (marketplace.orders, payments, cart, inventory, catalog, navigation),
Confluent Avro wire-format deserialization, bifurcated Dead Letter Queue (DLQ) routing, event-time
watermarking, stateful deduplication on [event_id, event_time], and fault-tolerant landing writes to MinIO
(s3a://lakehouse-bronze/landing/) using Write-Ahead Log (WAL) offset checkpointing.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from processing.streaming.config import SparkStreamingConfig
from processing.streaming.deserializer import deserialize_kafka_stream
from processing.streaming.dlq import DLQRouter
from processing.streaming.watermark import (
    apply_event_time_and_watermark,
    apply_stateful_deduplication,
)

logger = logging.getLogger("processing.streaming.pipeline")


class StreamingIngestionPipeline:
    """Orchestrates end-to-end PySpark Structured Streaming real-time event ingestion."""

    def __init__(
        self,
        config: SparkStreamingConfig,
        spark: SparkSession | None = None,
        metrics_listener: Any | None = None,
        enable_dlq_kafka: bool = True,
        enable_dlq_storage: bool = True,
    ) -> None:
        """Initialize StreamingIngestionPipeline.

        Args:
            config: SparkStreamingConfig defining Kafka, storage, triggers, and checkpoint settings.
            spark: Optional existing SparkSession instance (created via config if omitted).
            metrics_listener: Optional StreamingQueryListener instance for Prometheus metric tracking.
            enable_dlq_kafka: If True, writes malformed payloads to Kafka DLQ topic (marketplace.dlq).
            enable_dlq_storage: If True, writes malformed payloads to MinIO DLQ storage path.
        """
        self.config = config
        self.spark = spark
        self.metrics_listener = metrics_listener
        self.enable_dlq_kafka = enable_dlq_kafka
        self.enable_dlq_storage = enable_dlq_storage
        self.active_queries: list[StreamingQuery] = []

    def build_spark_session(self) -> SparkSession:
        """Build or retrieve the configured SparkSession with S3A / MinIO properties.

        Returns:
            Configured SparkSession instance.
        """
        if self.spark is not None:
            return self.spark

        logger.info(
            "Building SparkSession for app '%s' (master: %s)...",
            self.config.app_name,
            self.config.master,
        )

        builder = SparkSession.builder
        for key, val in self.config.to_spark_conf().items():
            builder = builder.config(key, val)

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        if self.metrics_listener is not None:
            try:
                self.spark.streams.addListener(self.metrics_listener)
                logger.info("Successfully attached StreamingQueryListener to SparkSession.")
            except Exception as exc:
                logger.warning("Failed to attach StreamingQueryListener: %s", exc)

        return self.spark

    def read_kafka_stream(self, spark: SparkSession) -> DataFrame:
        """Initialize streaming DataFrame reader subscribing to multi-topic Kafka feed.

        Args:
            spark: Active SparkSession instance.

        Returns:
            Streaming DataFrame containing raw Kafka message frames.
        """
        read_options = self.config.to_kafka_read_options()
        logger.info(
            "Configuring Kafka streaming source for topics: %s (bootstrap: %s)",
            read_options.get("subscribe"),
            read_options.get("kafka.bootstrap.servers"),
            extra={"kafka_options": read_options},
        )
        return spark.readStream.format("kafka").options(**read_options).load()

    def transform_stream(self, raw_kafka_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Apply wire-format deserialization, DLQ bifurcation, watermarking, and deduplication.

        Args:
            raw_kafka_df: Raw Kafka streaming DataFrame with 'value', 'topic', 'timestamp' columns.

        Returns:
            Tuple of (valid_dedup_df, dlq_df).
        """
        deserialized_df = deserialize_kafka_stream(
            df=raw_kafka_df,
            schema_registry_url=self.config.schema_registry_url,
        )

        valid_df, dlq_df = DLQRouter.bifurcate_stream(deserialized_df)

        watermarked_df = apply_event_time_and_watermark(
            valid_df=valid_df,
            watermark_delay=self.config.watermark_delay,
            timestamp_ms_col="event_timestamp_ms",
            event_time_col="event_time",
        )

        deduplicated_valid_df = apply_stateful_deduplication(
            watermarked_df=watermarked_df,
            dedup_columns=["event_id", "event_time"],
        )

        return deduplicated_valid_df, dlq_df

    def write_landing_stream(self, valid_df: DataFrame) -> StreamingQuery:
        """Write valid, deduplicated streaming events to MinIO bronze landing storage.

        Args:
            valid_df: Transformed and deduplicated streaming DataFrame.

        Returns:
            Active StreamingQuery handle for the Bronze landing sink.
        """
        landing_path = self.config.landing_path
        checkpoint_path = self.config.get_landing_checkpoint_path()
        landing_format = getattr(self.config, "landing_format", "parquet")

        logger.info(
            "Starting Bronze landing sink (path: %s, checkpoint: %s, trigger: %s)...",
            landing_path,
            checkpoint_path,
            "availableNow" if self.config.once else self.config.trigger_interval,
            extra={
                "landing_path": landing_path,
                "checkpoint_path": checkpoint_path,
                "format": landing_format,
            },
        )

        writer = (
            valid_df.writeStream.format(landing_format)
            .partitionBy("topic", "ingestion_date")
            .option("path", landing_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .queryName("bronze_landing_sink")
        )

        if self.config.once:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=self.config.trigger_interval)

        query = writer.start()
        logger.info("Bronze landing query started successfully (id: %s, runId: %s).", query.id, query.runId)
        return query

    def start_queries(self) -> list[StreamingQuery]:
        """Build session, read Kafka feed, transform stream, and start all configured sink queries.

        Returns:
            List of active StreamingQuery handles.
        """
        spark = self.build_spark_session()
        raw_kafka_df = self.read_kafka_stream(spark)
        valid_df, dlq_df = self.transform_stream(raw_kafka_df)

        landing_query = self.write_landing_stream(valid_df)
        self.active_queries.append(landing_query)

        if self.enable_dlq_storage:
            dlq_storage_query = DLQRouter.write_dlq_to_storage(dlq_df, self.config)
            self.active_queries.append(dlq_storage_query)
            logger.info(
                "DLQ storage query started successfully (id: %s, runId: %s).",
                dlq_storage_query.id,
                dlq_storage_query.runId,
            )

        if self.enable_dlq_kafka:
            dlq_kafka_query = DLQRouter.write_dlq_to_kafka(dlq_df, self.config)
            self.active_queries.append(dlq_kafka_query)
            logger.info(
                "DLQ Kafka publisher query started successfully (id: %s, runId: %s).",
                dlq_kafka_query.id,
                dlq_kafka_query.runId,
            )

        logger.info("All streaming queries initialized (total: %d queries).", len(self.active_queries))
        return self.active_queries

    def stop_queries(self) -> None:
        """Gracefully terminate all active streaming queries."""
        logger.info("Terminating all active streaming queries (%d queries)...", len(self.active_queries))
        for query in self.active_queries:
            try:
                is_active = (
                    query.isActive()
                    if callable(getattr(query, "isActive", None))
                    else getattr(query, "isActive", False)
                )
                if is_active:
                    logger.info("Stopping query: %s (name: %s)...", query.id, query.name)
                    query.stop()
            except Exception as exc:
                logger.warning("Error stopping query %s: %s", getattr(query, "id", "unknown"), exc)

        self.active_queries.clear()
        logger.info("All streaming queries terminated successfully.")

    def await_termination(self, timeout_seconds: float | None = None) -> None:
        """Wait for all active streaming queries to finish or until timeout is reached."""
        for query in self.active_queries:
            is_active = (
                query.isActive() if callable(getattr(query, "isActive", None)) else getattr(query, "isActive", False)
            )
            if is_active:
                query.awaitTermination(timeout=timeout_seconds)
