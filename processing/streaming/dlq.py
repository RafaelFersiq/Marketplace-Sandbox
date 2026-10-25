"""Dead Letter Queue (DLQ) routing, stream bifurcation, and diagnostic error isolation.

Provides utilities for filtering corrupt, malformed, or schema-violating events from PySpark
Structured Streaming pipelines, enriching them with standardized diagnostics (_error_type,
_error_message, _raw_payload_base64, _kafka_topic, _kafka_partition, _kafka_offset, _failed_at),
and dispatching them to Kafka DLQ topic (marketplace.dlq) or MinIO object storage (s3a://lakehouse-dlq/).
"""

import base64
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.streaming import StreamingQuery

if TYPE_CHECKING:
    from processing.streaming.config import SparkStreamingConfig

logger = logging.getLogger("processing.streaming.dlq")


@dataclass(frozen=True)
class DLQRecord:
    """Diagnostic dead letter queue record representing an unparseable or rejected payload."""

    _error_type: str
    _error_message: str
    _raw_payload_base64: str
    _kafka_topic: str
    _kafka_partition: int
    _kafka_offset: int
    _failed_at: str
    schema_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert DLQ diagnostic record to dictionary.

        Returns:
            Dictionary containing error classification, payload base64, Kafka coordinates, and failure timestamp.
        """
        return {
            "_error_type": self._error_type,
            "_error_message": self._error_message,
            "_raw_payload_base64": self._raw_payload_base64,
            "_kafka_topic": self._kafka_topic,
            "_kafka_partition": self._kafka_partition,
            "_kafka_offset": self._kafka_offset,
            "_failed_at": self._failed_at,
            "schema_id": self.schema_id,
        }

    def to_json(self) -> str:
        """Serialize DLQ diagnostic record to compact JSON string.

        Returns:
            JSON string formatted for Kafka DLQ publishing or MinIO archiving.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)


class DLQRouter:
    """Dead Letter Queue routing and stream bifurcation orchestrator for corrupt event payloads."""

    @staticmethod
    def create_dlq_record(
        raw_payload: bytes | str | None,
        error_type: str,
        error_message: str,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
        schema_id: int | None = None,
        failed_at: datetime | str | None = None,
    ) -> DLQRecord:
        """Construct a structured DLQRecord with normalized base64 payload and failure timestamp.

        Args:
            raw_payload: Raw payload bytes, base64-encoded string, or None.
            error_type: High-level failure categorization (e.g. 'CORRUPT_FRAME', 'AVRO_DECODE_ERROR').
            error_message: Detailed diagnostic error message or rejection reason.
            kafka_topic: Origin Kafka topic name.
            kafka_partition: Origin Kafka partition integer.
            kafka_offset: Origin Kafka offset integer.
            schema_id: Confluent schema registry identifier, if detected.
            failed_at: Optional failure datetime or ISO-8601 string (defaults to current UTC).

        Returns:
            Populated DLQRecord instance.
        """
        if isinstance(raw_payload, bytes):
            raw_b64 = base64.b64encode(raw_payload).decode("ascii")
        elif isinstance(raw_payload, str):
            try:
                base64.b64decode(raw_payload, validate=True)
                raw_b64 = raw_payload
            except Exception:
                raw_b64 = base64.b64encode(raw_payload.encode("utf-8")).decode("ascii")
        else:
            raw_b64 = ""

        if failed_at is None:
            failed_at_str = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        elif isinstance(failed_at, datetime):
            dt = failed_at if failed_at.tzinfo is not None else failed_at.replace(tzinfo=UTC)
            failed_at_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            failed_at_str = str(failed_at)

        return DLQRecord(
            _error_type=error_type,
            _error_message=error_message,
            _raw_payload_base64=raw_b64,
            _kafka_topic=kafka_topic,
            _kafka_partition=kafka_partition,
            _kafka_offset=kafka_offset,
            _failed_at=failed_at_str,
            schema_id=schema_id,
        )

    @staticmethod
    def bifurcate_stream(deserialized_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Split a deserialized Kafka stream into valid events and enriched DLQ diagnostics.

        Args:
            deserialized_df: DataFrame resulting from deserialize_kafka_stream containing 'is_valid'.

        Returns:
            Tuple of (valid_df, dlq_df).
            - valid_df contains rows where is_valid == True with business attributes and Kafka coordinates.
            - dlq_df contains rows where is_valid == False enriched with standardized DLQ diagnostic columns.
        """
        # Valid events projection
        valid_df = deserialized_df.filter(F.col("is_valid") == F.lit(True)).select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("event_timestamp_ms"),
            F.col("schema_id"),
            F.col("payload_json"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_timestamp"),
        )

        # DLQ records projection with coalesce fallbacks and diagnostic metadata
        dlq_raw_col = F.col("value") if "value" in deserialized_df.columns else F.lit(None).cast("binary")
        raw_b64_fallback = F.base64(dlq_raw_col)

        dlq_df = (
            deserialized_df.filter(F.col("is_valid") == F.lit(False))
            .withColumn(
                "_error_type",
                F.coalesce(F.col("_error_type"), F.lit("UNKNOWN_DESERIALIZATION_ERROR")),
            )
            .withColumn(
                "_error_message",
                F.coalesce(F.col("_error_message"), F.lit("Unspecified deserialization failure")),
            )
            .withColumn(
                "_raw_payload_base64",
                F.coalesce(F.col("_raw_payload_base64"), raw_b64_fallback, F.lit("")),
            )
            .withColumn("_kafka_topic", F.coalesce(F.col("topic"), F.lit("unknown")))
            .withColumn("_kafka_partition", F.coalesce(F.col("partition"), F.lit(0)))
            .withColumn("_kafka_offset", F.coalesce(F.col("offset"), F.lit(0).cast("long")))
            .withColumn(
                "_failed_at",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            )
            .select(
                F.col("_error_type"),
                F.col("_error_message"),
                F.col("_raw_payload_base64"),
                F.col("_kafka_topic"),
                F.col("_kafka_partition"),
                F.col("_kafka_offset"),
                F.col("_failed_at"),
                F.col("schema_id"),
            )
        )

        return valid_df, dlq_df

    @staticmethod
    def format_for_kafka_dlq(dlq_df: DataFrame) -> DataFrame:
        """Format DLQ DataFrame into Kafka-compatible key and value columns.

        Args:
            dlq_df: Bifurcated DLQ DataFrame containing diagnostic columns.

        Returns:
            DataFrame with 'key' (string topic) and 'value' (JSON-encoded diagnostic payload).
        """
        return dlq_df.select(
            F.coalesce(F.col("_kafka_topic"), F.lit("unknown")).cast("string").alias("key"),
            F.to_json(
                F.struct(
                    F.col("_error_type"),
                    F.col("_error_message"),
                    F.col("_raw_payload_base64"),
                    F.col("_kafka_topic"),
                    F.col("_kafka_partition"),
                    F.col("_kafka_offset"),
                    F.col("_failed_at"),
                    F.col("schema_id"),
                )
            )
            .cast("string")
            .alias("value"),
        )

    @staticmethod
    def write_dlq_to_kafka(
        dlq_df: DataFrame,
        config: "SparkStreamingConfig | Any",
    ) -> StreamingQuery:
        """Publish bifurcated DLQ records to Kafka dead letter topic (marketplace.dlq).

        Args:
            dlq_df: Bifurcated DLQ DataFrame.
            config: Streaming configuration providing bootstrap servers, DLQ topic, and checkpoints.

        Returns:
            Active StreamingQuery handle for the Kafka sink.
        """
        kafka_formatted_df = DLQRouter.format_for_kafka_dlq(dlq_df)

        bootstrap_servers = (
            getattr(config, "kafka_bootstrap_servers", None)
            or getattr(config, "bootstrap_servers", None)
            or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        )
        dlq_topic = getattr(config, "dlq_topic", None) or os.getenv("KAFKA_DLQ_TOPIC", "marketplace.dlq")
        if hasattr(config, "get_dlq_kafka_checkpoint_path"):
            checkpoint_dir = config.get_dlq_kafka_checkpoint_path()
        else:
            checkpoint_dir = (
                getattr(config, "dlq_kafka_checkpoint_path", None)
                or getattr(config, "dlq_checkpoint_path", None)
                or getattr(config, "checkpoint_dir", None)
                or os.getenv("SPARK_DLQ_CHECKPOINT_PATH", "s3a://lakehouse-bronze/checkpoints/dlq_kafka")
            )
        trigger_interval = getattr(config, "trigger_interval", "10 seconds")
        once = getattr(config, "once", False)

        logger.info(
            "Starting DLQ Kafka streaming writer (topic: %s, bootstrap: %s)...",
            dlq_topic,
            bootstrap_servers,
            extra={"dlq_topic": dlq_topic, "bootstrap_servers": bootstrap_servers},
        )

        writer = (
            kafka_formatted_df.writeStream.queryName("dlq_kafka_sink")
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("topic", dlq_topic)
            .option("checkpointLocation", checkpoint_dir)
            .outputMode("append")
        )

        if once:
            writer = writer.trigger(availableNow=True)
        elif trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)

        return writer.start()

    @staticmethod
    def write_dlq_to_storage(
        dlq_df: DataFrame,
        config: "SparkStreamingConfig | Any",
    ) -> StreamingQuery:
        """Persist bifurcated DLQ records to MinIO / S3 object storage for forensic auditing.

        Args:
            dlq_df: Bifurcated DLQ DataFrame.
            config: Streaming configuration providing storage path, storage format, and checkpoints.

        Returns:
            Active StreamingQuery handle for the object storage sink.
        """
        storage_path = (
            getattr(config, "dlq_storage_path", None)
            or getattr(config, "dlq_landing_path", None)
            or os.getenv("SPARK_DLQ_STORAGE_PATH", "s3a://lakehouse-dlq/events")
        )
        if hasattr(config, "get_dlq_storage_checkpoint_path"):
            checkpoint_dir = config.get_dlq_storage_checkpoint_path()
        else:
            checkpoint_dir = (
                getattr(config, "dlq_storage_checkpoint_path", None)
                or getattr(config, "dlq_checkpoint_path", None)
                or getattr(config, "checkpoint_dir", None)
                or os.getenv("SPARK_DLQ_STORAGE_CHECKPOINT_PATH", "s3a://lakehouse-bronze/checkpoints/dlq_storage")
            )
        storage_format = getattr(config, "dlq_storage_format", "json")
        trigger_interval = getattr(config, "trigger_interval", "10 seconds")
        once = getattr(config, "once", False)

        logger.info(
            "Starting DLQ Storage streaming writer (path: %s, format: %s)...",
            storage_path,
            storage_format,
            extra={"storage_path": storage_path, "storage_format": storage_format},
        )

        writer = (
            dlq_df.writeStream.queryName("dlq_storage_sink")
            .format(storage_format)
            .option("path", storage_path)
            .option("checkpointLocation", checkpoint_dir)
            .outputMode("append")
        )

        if once:
            writer = writer.trigger(availableNow=True)
        elif trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)

        return writer.start()
