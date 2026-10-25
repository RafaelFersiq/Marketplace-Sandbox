"""Unit tests for Spark Structured Streaming metrics, Prometheus registry, and listener."""

import json
import logging
from dataclasses import dataclass
from typing import Any

import pytest

from processing.streaming.metrics import (
    PrometheusMetricsRegistry,
    QueryProgressSnapshot,
    StreamingMetricsListener,
)


@dataclass
class MockQueryStartedEvent:
    id: str
    runId: str  # noqa: N815
    name: str
    timestamp: str


@dataclass
class MockQueryProgressEvent:
    progress: dict[str, Any]


@dataclass
class MockQueryIdleEvent:
    id: str
    runId: str  # noqa: N815
    timestamp: str


@dataclass
class MockQueryTerminatedEvent:
    id: str
    runId: str  # noqa: N815
    exception: str | None = None


class TestQueryProgressSnapshot:
    """Test suite for QueryProgressSnapshot dataclass."""

    def test_snapshot_creation_and_serialization(self) -> None:
        snapshot = QueryProgressSnapshot(
            query_id="qid-123",
            run_id="run-456",
            query_name="orders_stream",
            batch_id=42,
            timestamp="2026-10-25T01:00:00.000Z",
            num_input_rows=150,
            input_rows_per_second=30.0,
            processed_rows_per_second=28.5,
            batch_duration_ms=520,
            event_time_watermark="2026-10-25T00:50:00.000Z",
            state_rows_total=1200,
            state_memory_bytes=8192,
        )

        data = snapshot.to_dict()
        assert data["query_id"] == "qid-123"
        assert data["batch_id"] == 42
        assert data["num_input_rows"] == 150
        assert data["event_time_watermark"] == "2026-10-25T00:50:00.000Z"

        json_str = snapshot.to_json()
        parsed = json.loads(json_str)
        assert parsed["query_name"] == "orders_stream"
        assert parsed["state_rows_total"] == 1200


class TestPrometheusMetricsRegistry:
    """Test suite for in-memory Prometheus metrics registry."""

    def test_initial_state_empty(self) -> None:
        registry = PrometheusMetricsRegistry()
        assert registry.format_prometheus_text() == ""

    def test_record_progress_and_gauges(self) -> None:
        registry = PrometheusMetricsRegistry()
        snapshot = QueryProgressSnapshot(
            query_id="qid-1",
            run_id="run-1",
            query_name="marketplace.orders",
            batch_id=1,
            timestamp="2026-10-25T01:00:00.000Z",
            num_input_rows=100,
            input_rows_per_second=10.0,
            processed_rows_per_second=9.5,
            batch_duration_ms=450,
            state_rows_total=500,
            state_memory_bytes=4096,
        )
        registry.record_progress(snapshot)

        assert registry.get_metric_value("spark_streaming_input_rows_total", "marketplace.orders") == 100
        assert registry.get_metric_value("spark_streaming_batches_completed_total", "marketplace.orders") == 1
        assert registry.get_metric_value("spark_streaming_batch_duration_ms", "marketplace.orders") == 450
        assert registry.get_metric_value("spark_streaming_state_rows_total", "marketplace.orders") == 500

        snapshot2 = QueryProgressSnapshot(
            query_id="qid-1",
            run_id="run-1",
            query_name="marketplace.orders",
            batch_id=2,
            timestamp="2026-10-25T01:00:10.000Z",
            num_input_rows=200,
            input_rows_per_second=20.0,
            processed_rows_per_second=19.0,
            batch_duration_ms=300,
            state_rows_total=650,
            state_memory_bytes=5120,
        )
        registry.record_progress(snapshot2)

        assert registry.get_metric_value("spark_streaming_input_rows_total", "marketplace.orders") == 300
        assert registry.get_metric_value("spark_streaming_batches_completed_total", "marketplace.orders") == 2
        assert registry.get_metric_value("spark_streaming_batch_duration_ms", "marketplace.orders") == 300
        assert registry.get_metric_value("spark_streaming_state_rows_total", "marketplace.orders") == 650

    def test_record_error(self) -> None:
        registry = PrometheusMetricsRegistry()
        registry.record_error("marketplace.payments", "Kafka connection timeout")

        assert registry.get_metric_value("spark_streaming_query_errors_total", "marketplace.payments") == 1
        assert registry.get_latest_error("marketplace.payments") == "Kafka connection timeout"

    def test_prometheus_exposition_format(self) -> None:
        registry = PrometheusMetricsRegistry()
        snapshot = QueryProgressSnapshot(
            query_id="qid-1",
            run_id="run-1",
            query_name="marketplace.orders",
            batch_id=1,
            timestamp="2026-10-25T01:00:00.000Z",
            num_input_rows=50,
            input_rows_per_second=5.0,
            processed_rows_per_second=4.8,
            batch_duration_ms=250,
        )
        registry.record_progress(snapshot)
        text = registry.format_prometheus_text()

        assert "# HELP spark_streaming_input_rows_total" in text
        assert "# TYPE spark_streaming_input_rows_total counter" in text
        assert 'spark_streaming_input_rows_total{query_name="marketplace.orders"} 50' in text
        assert 'spark_streaming_batch_duration_ms{query_name="marketplace.orders"} 250' in text
        assert text.endswith("\n")


class TestStreamingMetricsListener:
    """Test suite for StreamingMetricsListener."""

    def test_listener_lifecycle_events(self, caplog: pytest.LogCaptureFixture) -> None:
        registry = PrometheusMetricsRegistry()
        logger_instance = logging.getLogger("test_listener")
        listener = StreamingMetricsListener(metrics_registry=registry, logger=logger_instance)

        with caplog.at_level(logging.INFO):
            start_event = MockQueryStartedEvent(
                id="qid-999",
                runId="run-888",
                name="marketplace.cart",
                timestamp="2026-10-25T01:00:00.000Z",
            )
            listener.onQueryStarted(start_event)
            assert "streaming_query_started" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.INFO):
            progress_data = {
                "id": "qid-999",
                "runId": "run-888",
                "name": "marketplace.cart",
                "batchId": 0,
                "timestamp": "2026-10-25T01:00:05.000Z",
                "numInputRows": 75,
                "inputRowsPerSecond": 15.0,
                "processedRowsPerSecond": 14.2,
                "durationMs": {"triggerExecution": 410},
                "eventTime": {"watermark": "2026-10-25T00:55:00.000Z"},
                "stateOperators": [{"numRowsTotal": 30, "memoryUsedBytes": 1024}],
            }
            progress_event = MockQueryProgressEvent(progress=progress_data)
            listener.onQueryProgress(progress_event)

            assert "streaming_query_progress" in caplog.text
            assert registry.get_metric_value("spark_streaming_input_rows_total", "marketplace.cart") == 75
            assert registry.get_metric_value("spark_streaming_batch_duration_ms", "marketplace.cart") == 410
            assert registry.get_metric_value("spark_streaming_state_rows_total", "marketplace.cart") == 30

        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            idle_event = MockQueryIdleEvent(id="qid-999", runId="run-888", timestamp="2026-10-25T01:00:10.000Z")
            listener.onQueryIdle(idle_event)
            assert "streaming_query_idle" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.INFO):
            term_event = MockQueryTerminatedEvent(id="qid-999", runId="run-888", exception=None)
            listener.onQueryTerminated(term_event)
            assert "streaming_query_terminated" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.ERROR):
            err_event = MockQueryTerminatedEvent(
                id="qid-999",
                runId="run-888",
                exception="SchemaRegistryUnavailableException: HTTP 503",
            )
            listener.onQueryTerminated(err_event)
            assert "streaming_query_terminated_with_error" in caplog.text
            assert registry.get_metric_value("spark_streaming_query_errors_total", "marketplace.cart") == 1
