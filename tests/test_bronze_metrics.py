"""Comprehensive unit test suite for Bronze layer Prometheus metric instrumentation.

Verifies thread-safe counters, gauges, histograms, snapshot generation, Prometheus 0.0.4 text
formatting, reset routines, and Pushgateway mock publication.
"""

from __future__ import annotations

import concurrent.futures
from unittest.mock import MagicMock, patch

import pytest

from processing.bronze.metrics import (
    METRIC_DEFINITIONS,
    BronzeMetricsRegistry,
    _format_labels,
    _format_metric_value,
    get_bronze_metrics_registry,
    reset_bronze_metrics_registry,
)


@pytest.fixture
def registry() -> BronzeMetricsRegistry:
    """Provide a fresh isolated BronzeMetricsRegistry instance for each test."""
    return BronzeMetricsRegistry()


class TestBronzeMetricsRegistration:
    """Test suite validating default metric registration and schema metadata."""

    def test_default_metric_definitions_exist(self) -> None:
        """Verify all 8 counters, 3 gauges, and 3 histograms are defined by default."""
        expected_counters = [
            "bronze_ingestion_records_total",
            "bronze_ingestion_bytes_total",
            "bronze_ingestion_batches_total",
            "bronze_compaction_runs_total",
            "bronze_compaction_files_reduced_total",
            "bronze_compaction_bytes_saved_total",
            "bronze_landing_pruned_objects_total",
            "bronze_landing_pruned_bytes_total",
        ]
        expected_gauges = [
            "bronze_partition_file_count",
            "bronze_partition_bytes",
            "bronze_uncompacted_partitions_count",
        ]
        expected_histograms = [
            "bronze_ingestion_duration_seconds",
            "bronze_compaction_duration_seconds",
            "bronze_retention_prune_duration_seconds",
        ]

        for c in expected_counters:
            assert c in METRIC_DEFINITIONS
            assert METRIC_DEFINITIONS[c][0] == "counter"

        for g in expected_gauges:
            assert g in METRIC_DEFINITIONS
            assert METRIC_DEFINITIONS[g][0] == "gauge"

        for h in expected_histograms:
            assert h in METRIC_DEFINITIONS
            assert METRIC_DEFINITIONS[h][0] == "histogram"

    def test_custom_metric_registration(self, registry: BronzeMetricsRegistry) -> None:
        """Verify dynamic registration of custom counters, gauges, and histograms."""
        registry.register_metric("custom_counter", "counter", "Custom test counter")
        registry.register_metric("custom_gauge", "gauge", "Custom test gauge")
        registry.register_metric(
            "custom_histogram",
            "histogram",
            "Custom test histogram",
            buckets=(0.1, 0.5, 1.0),
        )

        registry.inc_counter("custom_counter", 5.0)
        registry.set_gauge("custom_gauge", 42.0)
        registry.observe_histogram("custom_histogram", 0.3)

        snap = registry.get_snapshot()
        assert snap.get_counter("custom_counter") == 5.0
        assert snap.get_gauge("custom_gauge") == 42.0
        assert snap.get_histogram_count("custom_histogram") == 1

    def test_register_invalid_metric_type_raises(self, registry: BronzeMetricsRegistry) -> None:
        """Verify ValueError is raised if an invalid metric type is registered."""
        with pytest.raises(ValueError, match="Unsupported metric type"):
            registry.register_metric("invalid_m", "summary", "Unsupported summary metric")


class TestBronzeMetricsCounters:
    """Test suite validating counter increments and validation rules."""

    def test_counter_increments(self, registry: BronzeMetricsRegistry) -> None:
        """Verify basic counter increments and multi-label support."""
        success_lbl = {"topic": "marketplace.orders", "status": "SUCCESS"}
        failed_lbl = {"topic": "marketplace.orders", "status": "FAILED"}
        payment_lbl = {"topic": "marketplace.payments", "status": "SUCCESS"}

        registry.inc_counter("bronze_ingestion_records_total", 100, labels=success_lbl)
        registry.inc_counter("bronze_ingestion_records_total", 50, labels=success_lbl)
        registry.inc_counter("bronze_ingestion_records_total", 10, labels=failed_lbl)

        snap = registry.get_snapshot()
        assert snap.get_counter("bronze_ingestion_records_total", labels=success_lbl) == 150.0
        assert snap.get_counter("bronze_ingestion_records_total", labels=failed_lbl) == 10.0
        assert snap.get_counter("bronze_ingestion_records_total", labels=payment_lbl) == 0.0

    def test_negative_increment_raises(self, registry: BronzeMetricsRegistry) -> None:
        """Verify negative increment raises ValueError according to Prometheus standard."""
        with pytest.raises(ValueError, match="Counter increment must be non-negative"):
            registry.inc_counter("bronze_ingestion_records_total", -5.0)

    def test_pre_initialized_zero_label_counters(self, registry: BronzeMetricsRegistry) -> None:
        """Verify counters without labels start at 0.0."""
        snap = registry.get_snapshot()
        assert snap.get_counter("bronze_landing_pruned_objects_total") == 0.0
        assert snap.get_counter("bronze_landing_pruned_bytes_total") == 0.0


class TestBronzeMetricsGauges:
    """Test suite validating gauge set, inc, and dec operations."""

    def test_gauge_set_inc_dec(self, registry: BronzeMetricsRegistry) -> None:
        """Verify setting, incrementing, and decrementing gauge values."""
        labels = {"topic": "marketplace.orders", "partition": "year=2026/month=11/day=02"}
        registry.set_gauge("bronze_partition_file_count", 15.0, labels=labels)
        snap = registry.get_snapshot()
        assert snap.get_gauge("bronze_partition_file_count", labels=labels) == 15.0

        registry.inc_gauge("bronze_partition_file_count", 5.0, labels=labels)
        snap = registry.get_snapshot()
        assert snap.get_gauge("bronze_partition_file_count", labels=labels) == 20.0

        registry.dec_gauge("bronze_partition_file_count", 8.0, labels=labels)
        snap = registry.get_snapshot()
        assert snap.get_gauge("bronze_partition_file_count", labels=labels) == 12.0


class TestBronzeMetricsHistograms:
    """Test suite validating histogram observations and cumulative bucket distributions."""

    def test_histogram_observation_distribution(self, registry: BronzeMetricsRegistry) -> None:
        """Verify observation distribution across cumulative buckets."""
        topic_labels = {"topic": "marketplace.orders"}
        # DEFAULT_DURATION_BUCKETS includes 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, ...
        registry.observe_histogram("bronze_ingestion_duration_seconds", 0.02, labels=topic_labels)
        registry.observe_histogram("bronze_ingestion_duration_seconds", 0.08, labels=topic_labels)
        registry.observe_histogram("bronze_ingestion_duration_seconds", 0.40, labels=topic_labels)

        snap = registry.get_snapshot()
        assert snap.get_histogram_count("bronze_ingestion_duration_seconds", labels=topic_labels) == 3
        hist_sum = snap.get_histogram_sum("bronze_ingestion_duration_seconds", labels=topic_labels)
        assert pytest.approx(hist_sum, 0.001) == 0.50

        # Validate internal cumulative buckets
        hist_data = registry._histograms["bronze_ingestion_duration_seconds"][tuple(sorted(topic_labels.items()))]
        # 0.02 is > 0.01 and <= 0.025
        assert hist_data.counts[0.01] == 0
        assert hist_data.counts[0.025] == 1
        assert hist_data.counts[0.05] == 1
        assert hist_data.counts[0.1] == 2
        assert hist_data.counts[0.5] == 3
        assert hist_data.inf_count == 3

    def test_measure_duration_context_manager(self, registry: BronzeMetricsRegistry) -> None:
        """Verify measure_duration context manager observes positive elapsed duration."""
        labels = {"topic": "marketplace.cart"}
        with registry.measure_duration("bronze_ingestion_duration_seconds", labels=labels):
            pass

        snap = registry.get_snapshot()
        assert snap.get_histogram_count("bronze_ingestion_duration_seconds", labels=labels) == 1
        assert snap.get_histogram_sum("bronze_ingestion_duration_seconds", labels=labels) >= 0.0


class TestBronzeDomainMethods:
    """Test suite validating high-level domain telemetry recording methods."""

    def test_record_ingestion(self, registry: BronzeMetricsRegistry) -> None:
        """Verify record_ingestion updates records, bytes, batches, and duration."""
        registry.record_ingestion(
            topic="marketplace.orders",
            status="SUCCESS",
            record_count=1200,
            bytes_written=524288,
            duration_seconds=0.45,
            batch_count=1,
        )

        snap = registry.get_snapshot()
        order_lbl = {"topic": "marketplace.orders", "status": "SUCCESS"}
        order_topic = {"topic": "marketplace.orders"}
        assert snap.get_counter("bronze_ingestion_records_total", labels=order_lbl) == 1200.0
        assert snap.get_counter("bronze_ingestion_bytes_total", labels=order_topic) == 524288.0
        assert snap.get_counter("bronze_ingestion_batches_total", labels=order_lbl) == 1.0
        assert snap.get_histogram_count("bronze_ingestion_duration_seconds", labels=order_topic) == 1

    def test_record_compaction(self, registry: BronzeMetricsRegistry) -> None:
        """Verify record_compaction updates runs, files reduced, bytes saved, and duration."""
        registry.record_compaction(
            topic="marketplace.payments",
            status="SUCCESS",
            files_reduced=8,
            bytes_saved=1048576,
            duration_seconds=1.25,
            run_count=1,
        )

        snap = registry.get_snapshot()
        pay_lbl = {"topic": "marketplace.payments", "status": "SUCCESS"}
        pay_topic = {"topic": "marketplace.payments"}
        assert snap.get_counter("bronze_compaction_runs_total", labels=pay_lbl) == 1.0
        assert snap.get_counter("bronze_compaction_files_reduced_total", labels=pay_topic) == 8.0
        assert snap.get_counter("bronze_compaction_bytes_saved_total", labels=pay_topic) == 1048576.0
        assert snap.get_histogram_count("bronze_compaction_duration_seconds", labels=pay_topic) == 1

    def test_record_partition_gauge_and_uncompacted(self, registry: BronzeMetricsRegistry) -> None:
        """Verify partition gauges and uncompacted partitions count updates."""
        registry.record_partition_gauge(
            topic="marketplace.inventory",
            partition="year=2026/month=11/day=02",
            file_count=12,
            bytes_total=20971520,
            uncompacted_partitions_count=3,
        )

        snap = registry.get_snapshot()
        part_labels = {"topic": "marketplace.inventory", "partition": "year=2026/month=11/day=02"}
        assert snap.get_gauge("bronze_partition_file_count", labels=part_labels) == 12.0
        assert snap.get_gauge("bronze_partition_bytes", labels=part_labels) == 20971520.0
        assert snap.get_gauge("bronze_uncompacted_partitions_count", labels={"topic": "marketplace.inventory"}) == 3.0

        registry.record_uncompacted_partitions("marketplace.inventory", 0)
        snap = registry.get_snapshot()
        assert snap.get_gauge("bronze_uncompacted_partitions_count", labels={"topic": "marketplace.inventory"}) == 0.0

    def test_record_retention(self, registry: BronzeMetricsRegistry) -> None:
        """Verify retention records pruned objects, pruned bytes, and prune duration."""
        registry.record_retention(
            pruned_objects=42,
            pruned_bytes=104857600,
            duration_seconds=0.75,
        )

        snap = registry.get_snapshot()
        assert snap.get_counter("bronze_landing_pruned_objects_total") == 42.0
        assert snap.get_counter("bronze_landing_pruned_bytes_total") == 104857600.0
        assert snap.get_histogram_count("bronze_retention_prune_duration_seconds") == 1


class TestBronzeMetricSnapshot:
    """Test suite validating BronzeMetricSnapshot immutability and serialization."""

    def test_snapshot_immutability(self, registry: BronzeMetricsRegistry) -> None:
        """Verify snapshot remains unchanged when registry receives subsequent updates."""
        cat_lbl = {"topic": "marketplace.catalog", "status": "SUCCESS"}
        registry.inc_counter("bronze_ingestion_records_total", 50, labels=cat_lbl)
        snap1 = registry.get_snapshot()

        registry.inc_counter("bronze_ingestion_records_total", 100, labels=cat_lbl)
        snap2 = registry.get_snapshot()

        assert snap1.get_counter("bronze_ingestion_records_total", labels=cat_lbl) == 50.0
        assert snap2.get_counter("bronze_ingestion_records_total", labels=cat_lbl) == 150.0

    def test_snapshot_to_dict_serialization(self, registry: BronzeMetricsRegistry) -> None:
        """Verify snapshot.to_dict() produces valid serializable dictionary."""
        registry.record_ingestion(
            topic="marketplace.navigation",
            status="SUCCESS",
            record_count=500,
            bytes_written=10240,
            duration_seconds=0.15,
        )
        snap = registry.get_snapshot()
        data = snap.to_dict()

        assert "timestamp" in data
        assert "counters" in data
        assert "gauges" in data
        assert "histograms" in data
        assert any("marketplace.navigation" in k for k in data["counters"])


class TestPrometheusTextExposition:
    """Test suite validating standard Prometheus text exposition format (0.0.4)."""

    def test_prometheus_exposition_format(self, registry: BronzeMetricsRegistry) -> None:
        """Verify exposition contains # HELP, # TYPE, sorted labels, and trailing newline."""
        registry.record_ingestion(
            topic="marketplace.orders",
            status="SUCCESS",
            record_count=10,
            bytes_written=1024,
            duration_seconds=0.05,
        )

        text = registry.generate_prometheus_text()
        assert text.endswith("\n")

        # Verify Help and Type headers
        assert "# HELP bronze_ingestion_records_total" in text
        assert "# TYPE bronze_ingestion_records_total counter" in text
        assert "# HELP bronze_ingestion_duration_seconds" in text
        assert "# TYPE bronze_ingestion_duration_seconds histogram" in text

        # Verify sorted labels
        assert 'bronze_ingestion_records_total{status="SUCCESS",topic="marketplace.orders"} 10' in text
        assert 'bronze_ingestion_bytes_total{topic="marketplace.orders"} 1024' in text

        # Verify histogram bucket formatting
        assert 'bronze_ingestion_duration_seconds_bucket{le="+Inf",topic="marketplace.orders"} 1' in text
        assert 'bronze_ingestion_duration_seconds_count{topic="marketplace.orders"} 1' in text
        assert 'bronze_ingestion_duration_seconds_sum{topic="marketplace.orders"} 0.05' in text

    def test_format_labels_and_values(self) -> None:
        """Verify label formatting escapes special characters and formats numbers properly."""
        assert _format_labels({}) == ""
        assert _format_labels(None) == ""
        assert _format_labels({"b": "val2", "a": 'quote"test'}) == '{a="quote\\"test",b="val2"}'

        assert _format_metric_value(10) == "10"
        assert _format_metric_value(10.0) == "10"
        assert _format_metric_value(0.025) == "0.025"
        assert _format_metric_value(float("inf")) == "+Inf"
        assert _format_metric_value(float("-inf")) == "-Inf"
        assert _format_metric_value(float("nan")) == "NaN"


class TestRegistryReset:
    """Test suite validating registry reset functionality."""

    def test_reset_clears_metrics(self, registry: BronzeMetricsRegistry) -> None:
        """Verify reset clears recorded metrics and re-initializes clean state."""
        registry.record_ingestion(
            topic="marketplace.orders",
            status="SUCCESS",
            record_count=100,
            bytes_written=2048,
            duration_seconds=0.1,
        )
        registry.reset()

        snap = registry.get_snapshot()
        order_lbl = {"topic": "marketplace.orders", "status": "SUCCESS"}
        order_topic = {"topic": "marketplace.orders"}
        assert snap.get_counter("bronze_ingestion_records_total", labels=order_lbl) == 0.0
        assert snap.get_counter("bronze_ingestion_bytes_total", labels=order_topic) == 0.0
        assert snap.get_histogram_count("bronze_ingestion_duration_seconds", labels=order_topic) == 0


class TestPushgatewayIntegration:
    """Test suite validating Prometheus Pushgateway communication with mocks."""

    @patch("requests.request")
    def test_push_to_gateway_requests_success(self, mock_request: MagicMock, registry: BronzeMetricsRegistry) -> None:
        """Verify successful HTTP POST push to Pushgateway using requests."""
        mock_resp = MagicMock()
        mock_resp.status_code = 202
        mock_request.return_value = mock_resp

        registry.record_ingestion(
            topic="marketplace.orders",
            status="SUCCESS",
            record_count=10,
            bytes_written=100,
            duration_seconds=0.01,
        )

        status = registry.push_to_gateway(
            pushgateway_url="http://localhost:9091",
            job_name="bronze_processing",
            grouping_key={"instance": "worker-1"},
            method="POST",
        )

        assert status == 202
        mock_request.assert_called_once()
        call_kwargs = mock_request.call_args[1]
        assert call_kwargs["method"] == "POST"
        assert "http://localhost:9091/metrics/job/bronze_processing/instance/worker-1" in call_kwargs["url"]
        assert call_kwargs["headers"]["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"

    @patch("requests.request")
    def test_push_to_gateway_failure_raises(self, mock_request: MagicMock, registry: BronzeMetricsRegistry) -> None:
        """Verify Pushgateway failure raises exception and logs error."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = RuntimeError("500 Server Error")
        mock_request.return_value = mock_resp

        with pytest.raises(RuntimeError, match="500 Server Error"):
            registry.push_to_gateway(
                pushgateway_url="http://localhost:9091",
                job_name="bronze_processing",
            )


class TestConcurrencyAndThreadSafety:
    """Test suite validating thread-safety under concurrent load."""

    def test_concurrent_metric_recording(self, registry: BronzeMetricsRegistry) -> None:
        """Verify multi-threaded increments and observations execute safely without races."""
        def worker(topic: str, worker_id: int) -> None:
            for i in range(50):
                registry.record_ingestion(
                    topic=topic,
                    status="SUCCESS",
                    record_count=10,
                    bytes_written=100,
                    duration_seconds=0.01,
                )
                registry.record_partition_gauge(
                    topic=topic,
                    partition=f"p-{worker_id}",
                    file_count=i,
                    bytes_total=i * 100,
                )

        topics = ["marketplace.orders", "marketplace.payments", "marketplace.inventory"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(worker, topics[i % len(topics)], i)
                for i in range(6)
            ]
            concurrent.futures.wait(futures)

        snap = registry.get_snapshot()
        # 6 workers each did 50 iterations = 300 total batches across 3 topics (100 per topic)
        for t in topics:
            records = snap.get_counter("bronze_ingestion_records_total", labels={"topic": t, "status": "SUCCESS"})
            assert records == 1000.0  # 100 batches * 10 records


class TestGlobalRegistrySingleton:
    """Test suite validating global default registry singleton."""

    def test_global_singleton_access(self) -> None:
        """Verify get_bronze_metrics_registry() and reset_bronze_metrics_registry()."""
        reg1 = get_bronze_metrics_registry()
        reg2 = get_bronze_metrics_registry()
        assert reg1 is reg2

        order_lbl = {"topic": "marketplace.orders", "status": "SUCCESS"}
        reg1.inc_counter("bronze_ingestion_records_total", 25, labels=order_lbl)
        reset_bronze_metrics_registry()

        snap = reg1.get_snapshot()
        assert snap.get_counter("bronze_ingestion_records_total", labels=order_lbl) == 0.0
