"""Marketplace Structured Streaming Ingestion package.

Provides Spark Structured Streaming configuration, wire-format deserialization,
Dead Letter Queue (DLQ) routing, watermarking, fault tolerance, and pipeline orchestration.
"""

from processing.streaming.config import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_SPARK_PACKAGES,
    DEFAULT_STREAMING_TOPICS,
    SparkStreamingConfig,
    build_streaming_arg_parser,
    parse_streaming_config,
)
from processing.streaming.deserializer import (
    ConfluentWireDeserializer,
    DeserializedRecord,
    WireFormatError,
    deserialize_kafka_stream,
    get_deserialized_struct_type,
)
from processing.streaming.dlq import DLQRecord, DLQRouter
from processing.streaming.metrics import (
    PrometheusMetricsRegistry,
    QueryProgressSnapshot,
    StreamingMetricsListener,
)
from processing.streaming.pipeline import StreamingIngestionPipeline
from processing.streaming.recovery import (
    RecoveryMetrics,
    StreamingRecoveryError,
    StreamingRecoveryExhaustedError,
    StreamingRecoverySupervisor,
)
from processing.streaming.watermark import (
    InMemoryWatermarkDeduplicator,
    apply_event_time_and_watermark,
    apply_stateful_deduplication,
    ms_to_utc_datetime,
    parse_watermark_duration_ms,
)

__all__ = [
    "DEFAULT_DLQ_TOPIC",
    "DEFAULT_SPARK_PACKAGES",
    "DEFAULT_STREAMING_TOPICS",
    "ConfluentWireDeserializer",
    "DLQRecord",
    "DLQRouter",
    "DeserializedRecord",
    "InMemoryWatermarkDeduplicator",
    "PrometheusMetricsRegistry",
    "QueryProgressSnapshot",
    "RecoveryMetrics",
    "SparkStreamingConfig",
    "StreamingIngestionPipeline",
    "StreamingMetricsListener",
    "StreamingRecoveryError",
    "StreamingRecoveryExhaustedError",
    "StreamingRecoverySupervisor",
    "WireFormatError",
    "apply_event_time_and_watermark",
    "apply_stateful_deduplication",
    "build_streaming_arg_parser",
    "deserialize_kafka_stream",
    "get_deserialized_struct_type",
    "ms_to_utc_datetime",
    "parse_streaming_config",
    "parse_watermark_duration_ms",
]
