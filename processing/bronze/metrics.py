"""Prometheus metric instrumentation and telemetry registry for Medallion Bronze layer.

Manages thread-safe in-memory counters, gauges, and histograms tracking batch ingestion
throughput, storage byte volumes, small-file compaction efficiency, and raw landing retention
lifecycles conforming to ADR-0008. Formats metrics into standard Prometheus text exposition
format (0.0.4) and provides Prometheus Pushgateway publication utilities.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]

logger = logging.getLogger("processing.bronze.metrics")

# Default latency and runtime histogram buckets in seconds
DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

# Canonical Bronze telemetry metric definitions: metric_name -> (type, help_description)
METRIC_DEFINITIONS: dict[str, tuple[str, str]] = {
    # Counters
    "bronze_ingestion_records_total": (
        "counter",
        "Total number of raw Kafka records ingested into Bronze layer",
    ),
    "bronze_ingestion_bytes_total": (
        "counter",
        "Total volume of data in bytes written to Bronze storage",
    ),
    "bronze_ingestion_batches_total": (
        "counter",
        "Total number of Bronze ingestion micro-batches executed",
    ),
    "bronze_compaction_runs_total": (
        "counter",
        "Total number of Bronze partition compaction jobs executed",
    ),
    "bronze_compaction_files_reduced_total": (
        "counter",
        "Total number of small files eliminated by Bronze compaction",
    ),
    "bronze_compaction_bytes_saved_total": (
        "counter",
        "Total storage bytes saved through Bronze compaction",
    ),
    "bronze_landing_pruned_objects_total": (
        "counter",
        "Total number of expired objects pruned from landing storage",
    ),
    "bronze_landing_pruned_bytes_total": (
        "counter",
        "Total volume of expired data in bytes purged from landing storage",
    ),
    # Gauges
    "bronze_partition_file_count": (
        "gauge",
        "Current count of Parquet files in a Bronze partition",
    ),
    "bronze_partition_bytes": (
        "gauge",
        "Current total size in bytes of a Bronze partition",
    ),
    "bronze_uncompacted_partitions_count": (
        "gauge",
        "Number of Bronze partitions currently pending compaction",
    ),
    # Histograms / Timings
    "bronze_ingestion_duration_seconds": (
        "histogram",
        "Duration of Bronze micro-batch ingestion runs in seconds",
    ),
    "bronze_compaction_duration_seconds": (
        "histogram",
        "Duration of Bronze partition compaction runs in seconds",
    ),
    "bronze_retention_prune_duration_seconds": (
        "histogram",
        "Duration of Bronze landing storage prune runs in seconds",
    ),
}


def _escape_label_value(val: str) -> str:
    """Escape special characters in Prometheus label values according to exposition format.

    Args:
        val: Raw string value.

    Returns:
        Escaped string safe for Prometheus label format.
    """
    return val.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels_to_key(labels: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    """Convert a label dictionary into a deterministic, hashable tuple of sorted key-value pairs.

    Args:
        labels: Optional label dictionary.

    Returns:
        Sorted tuple of (label_name, label_value) pairs.
    """
    if not labels:
        return ()
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def _format_labels(labels: dict[str, str] | None) -> str:
    """Format label dictionary into Prometheus exposition syntax '{key="value",...}'.

    Args:
        labels: Optional label dictionary.

    Returns:
        Formatted label bracket string, or empty string if no labels provided.
    """
    if not labels:
        return ""
    sorted_pairs = sorted(labels.items(), key=lambda kv: kv[0])
    formatted = ",".join(f'{k}="{_escape_label_value(str(v))}"' for k, v in sorted_pairs)
    return f"{{{formatted}}}"


def _format_metric_value(val: float | int) -> str:
    """Format numeric value for standard Prometheus text exposition format.

    Args:
        val: Numeric integer or float value.

    Returns:
        String representation formatted for Prometheus text.
    """
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if math.isnan(val):
            return "NaN"
        if math.isinf(val):
            return "+Inf" if val > 0 else "-Inf"
        if val.is_integer():
            return str(int(val))
        return f"{val:g}"
    return str(val)


@dataclass
class _HistogramData:
    """Internal cumulative histogram data container for a single time-series."""

    buckets: tuple[float, ...]
    counts: dict[float, int] = field(default_factory=dict)
    inf_count: int = 0
    total_sum: float = 0.0

    def __post_init__(self) -> None:
        """Initialize empty bucket counts if not already present."""
        if not self.counts:
            self.counts = dict.fromkeys(self.buckets, 0)

    def observe(self, value: float) -> None:
        """Record an observation, updating total sum, total count, and cumulative bucket counts.

        Args:
            value: Observed metric sample (e.g. execution duration in seconds).
        """
        self.inf_count += 1
        self.total_sum += value
        for b in self.buckets:
            if value <= b:
                self.counts[b] += 1

    def to_dict(self) -> dict[str, Any]:
        """Convert histogram series into a dictionary.

        Returns:
            Dictionary containing count, sum, and bucket counts.
        """
        return {
            "count": self.inf_count,
            "sum": self.total_sum,
            "buckets": {str(b): self.counts[b] for b in self.buckets},
        }


@dataclass(frozen=True)
class BronzeMetricSnapshot:
    """Immutable point-in-time snapshot of Bronze layer telemetry metrics."""

    timestamp: float
    counters: dict[str, dict[tuple[tuple[str, str], ...], float]]
    gauges: dict[str, dict[tuple[tuple[str, str], ...], float]]
    histograms: dict[str, dict[tuple[tuple[str, str], ...], dict[str, Any]]]

    def get_counter(self, name: str, labels: dict[str, str] | None = None) -> float:
        """Retrieve counter value by metric name and label dictionary.

        Args:
            name: Metric name.
            labels: Optional metric labels.

        Returns:
            Counter value, or 0.0 if not found.
        """
        key = _labels_to_key(labels)
        return self.counters.get(name, {}).get(key, 0.0)

    def get_gauge(self, name: str, labels: dict[str, str] | None = None) -> float:
        """Retrieve gauge value by metric name and label dictionary.

        Args:
            name: Metric name.
            labels: Optional metric labels.

        Returns:
            Gauge value, or 0.0 if not found.
        """
        key = _labels_to_key(labels)
        return self.gauges.get(name, {}).get(key, 0.0)

    def get_histogram_count(self, name: str, labels: dict[str, str] | None = None) -> int:
        """Retrieve total observation count for a histogram.

        Args:
            name: Metric name.
            labels: Optional metric labels.

        Returns:
            Observation count, or 0 if not found.
        """
        key = _labels_to_key(labels)
        hist = self.histograms.get(name, {}).get(key)
        return int(hist["count"]) if hist else 0

    def get_histogram_sum(self, name: str, labels: dict[str, str] | None = None) -> float:
        """Retrieve total observation sum for a histogram.

        Args:
            name: Metric name.
            labels: Optional metric labels.

        Returns:
            Total sum of observed values, or 0.0 if not found.
        """
        key = _labels_to_key(labels)
        hist = self.histograms.get(name, {}).get(key)
        return float(hist["sum"]) if hist else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot into a human-readable and JSON-serializable dictionary.

        Returns:
            Dictionary containing timestamp, counters, gauges, and histograms.
        """
        return {
            "timestamp": self.timestamp,
            "counters": {
                f"{name}{_format_labels(dict(label_tuples))}": val
                for name, series in self.counters.items()
                for label_tuples, val in series.items()
            },
            "gauges": {
                f"{name}{_format_labels(dict(label_tuples))}": val
                for name, series in self.gauges.items()
                for label_tuples, val in series.items()
            },
            "histograms": {
                f"{name}{_format_labels(dict(label_tuples))}": {
                    "count": data["count"],
                    "sum": data["sum"],
                }
                for name, series in self.histograms.items()
                for label_tuples, data in series.items()
            },
        }


class BronzeMetricsRegistry:
    """Thread-safe Prometheus metrics registry for Bronze layer telemetry.

    Manages counters, gauges, and histograms tracking data ingestion rates, storage byte
    footprints, small-file compaction performance, and raw landing retention cycles.
    Provides standard Prometheus exposition text formatting and Pushgateway transmission.
    """

    def __init__(self, custom_buckets: tuple[float, ...] | None = None) -> None:
        """Initialize BronzeMetricsRegistry with default metrics and histograms.

        Args:
            custom_buckets: Optional custom bucket boundaries for duration histograms.
        """
        self._lock = threading.RLock()
        self._duration_buckets = custom_buckets or DEFAULT_DURATION_BUCKETS
        self._metric_metadata: dict[str, tuple[str, str]] = dict(METRIC_DEFINITIONS)
        self._histogram_buckets: dict[str, tuple[float, ...]] = {}

        # Storage structures keyed by metric name and sorted label tuples
        self._counters: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
        self._gauges: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
        self._histograms: dict[str, dict[tuple[tuple[str, str], ...], _HistogramData]] = {}

        self._initialize_registered_metrics()

    def _initialize_registered_metrics(self) -> None:
        """Initialize metric containers according to registered definitions."""
        with self._lock:
            for name, (m_type, _desc) in self._metric_metadata.items():
                if m_type == "counter":
                    self._counters.setdefault(name, {})
                elif m_type == "gauge":
                    self._gauges.setdefault(name, {})
                elif m_type == "histogram":
                    self._histograms.setdefault(name, {})
                    self._histogram_buckets.setdefault(name, self._duration_buckets)

            # Pre-initialize zero-label counters to 0.0
            self._counters["bronze_landing_pruned_objects_total"].setdefault((), 0.0)
            self._counters["bronze_landing_pruned_bytes_total"].setdefault((), 0.0)

    def reset(self) -> None:
        """Reset all counters, gauges, and histograms to their initial clean zero state."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._initialize_registered_metrics()
            logger.info("BronzeMetricsRegistry reset to initial clean state")

    def register_metric(
        self,
        name: str,
        metric_type: str,
        description: str,
        buckets: tuple[float, ...] | None = None,
    ) -> None:
        """Register a new metric definition.

        Args:
            name: Metric identifier string.
            metric_type: One of 'counter', 'gauge', or 'histogram'.
            description: Help docstring for the metric.
            buckets: Upper bounds if metric_type is 'histogram'.

        Raises:
            ValueError: If metric_type is unsupported.
        """
        if metric_type not in ("counter", "gauge", "histogram"):
            raise ValueError(
                f"Unsupported metric type: '{metric_type}'. Must be 'counter', 'gauge', or 'histogram'."
            )
        with self._lock:
            self._metric_metadata[name] = (metric_type, description)
            if metric_type == "counter":
                self._counters.setdefault(name, {})
            elif metric_type == "gauge":
                self._gauges.setdefault(name, {})
            elif metric_type == "histogram":
                self._histograms.setdefault(name, {})
                self._histogram_buckets[name] = buckets or self._duration_buckets

    def inc_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment a counter metric by a non-negative value.

        Args:
            name: Counter metric name.
            value: Increment value (must be >= 0).
            labels: Optional label dictionary.

        Raises:
            ValueError: If value is negative.
        """
        if value < 0:
            raise ValueError(f"Counter increment must be non-negative, got {value}")
        with self._lock:
            if name not in self._counters:
                self.register_metric(name, "counter", f"Custom counter {name}")
            key = _labels_to_key(labels)
            self._counters[name][key] = self._counters[name].get(key, 0.0) + float(value)

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Set a gauge metric to an arbitrary value.

        Args:
            name: Gauge metric name.
            value: Metric value.
            labels: Optional label dictionary.
        """
        with self._lock:
            if name not in self._gauges:
                self.register_metric(name, "gauge", f"Custom gauge {name}")
            key = _labels_to_key(labels)
            self._gauges[name][key] = float(value)

    def inc_gauge(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment a gauge metric by value.

        Args:
            name: Gauge metric name.
            value: Increment amount.
            labels: Optional label dictionary.
        """
        with self._lock:
            if name not in self._gauges:
                self.register_metric(name, "gauge", f"Custom gauge {name}")
            key = _labels_to_key(labels)
            self._gauges[name][key] = self._gauges[name].get(key, 0.0) + float(value)

    def dec_gauge(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Decrement a gauge metric by value.

        Args:
            name: Gauge metric name.
            value: Decrement amount.
            labels: Optional label dictionary.
        """
        self.inc_gauge(name, -value, labels=labels)

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record an observation in a histogram metric.

        Args:
            name: Histogram metric name.
            value: Observed value (typically duration in seconds).
            labels: Optional label dictionary.
        """
        with self._lock:
            if name not in self._histograms:
                self.register_metric(name, "histogram", f"Custom histogram {name}")
            key = _labels_to_key(labels)
            series = self._histograms[name]
            if key not in series:
                buckets = self._histogram_buckets.get(name, self._duration_buckets)
                series[key] = _HistogramData(buckets=buckets)
            series[key].observe(float(value))

    @contextmanager
    def measure_duration(
        self,
        name: str,
        labels: dict[str, str] | None = None,
    ) -> Generator[None, None, None]:
        """Context manager to measure and observe code block duration in seconds.

        Args:
            name: Histogram metric name.
            labels: Optional label dictionary.
        """
        start = time.monotonic()
        try:
            yield
        finally:
            duration = time.monotonic() - start
            self.observe_histogram(name, duration, labels=labels)

    # -------------------------------------------------------------------------
    # High-level domain methods
    # -------------------------------------------------------------------------
    def record_ingestion(
        self,
        topic: str,
        status: str,
        record_count: int,
        bytes_written: int,
        duration_seconds: float,
        batch_count: int = 1,
    ) -> None:
        """Record telemetry for a Bronze micro-batch ingestion run.

        Args:
            topic: Kafka topic or domain dataset name.
            status: Ingestion status ("SUCCESS", "FAILED", "SKIPPED_DRY_RUN", etc.).
            record_count: Number of records ingested.
            bytes_written: Volume of data written in bytes.
            duration_seconds: Processing runtime duration in seconds.
            batch_count: Number of micro-batches (defaults to 1).
        """
        with self._lock:
            self.inc_counter(
                "bronze_ingestion_records_total",
                value=float(record_count),
                labels={"topic": topic, "status": status},
            )
            self.inc_counter(
                "bronze_ingestion_bytes_total",
                value=float(bytes_written),
                labels={"topic": topic},
            )
            self.inc_counter(
                "bronze_ingestion_batches_total",
                value=float(batch_count),
                labels={"topic": topic, "status": status},
            )
            self.observe_histogram(
                "bronze_ingestion_duration_seconds",
                value=float(duration_seconds),
                labels={"topic": topic},
            )

        logger.info(
            "Bronze ingestion metric recorded: %s",
            json.dumps({
                "event": "bronze_metric_ingestion",
                "topic": topic,
                "status": status,
                "record_count": record_count,
                "bytes_written": bytes_written,
                "duration_seconds": duration_seconds,
                "batch_count": batch_count,
            }),
        )

    def record_compaction(
        self,
        topic: str,
        status: str,
        files_reduced: int,
        bytes_saved: int,
        duration_seconds: float,
        run_count: int = 1,
    ) -> None:
        """Record telemetry for a Bronze small-file compaction run.

        Args:
            topic: Kafka topic or domain dataset name.
            status: Compaction execution status ("SUCCESS", "FAILED", "SKIPPED_DRY_RUN", etc.).
            files_reduced: Net reduction in Parquet file count.
            bytes_saved: Storage space saved in bytes.
            duration_seconds: Compaction execution duration in seconds.
            run_count: Number of compaction runs (defaults to 1).
        """
        with self._lock:
            self.inc_counter(
                "bronze_compaction_runs_total",
                value=float(run_count),
                labels={"topic": topic, "status": status},
            )
            self.inc_counter(
                "bronze_compaction_files_reduced_total",
                value=float(max(0, files_reduced)),
                labels={"topic": topic},
            )
            self.inc_counter(
                "bronze_compaction_bytes_saved_total",
                value=float(max(0, bytes_saved)),
                labels={"topic": topic},
            )
            self.observe_histogram(
                "bronze_compaction_duration_seconds",
                value=float(duration_seconds),
                labels={"topic": topic},
            )

        logger.info(
            "Bronze compaction metric recorded: %s",
            json.dumps({
                "event": "bronze_metric_compaction",
                "topic": topic,
                "status": status,
                "files_reduced": files_reduced,
                "bytes_saved": bytes_saved,
                "duration_seconds": duration_seconds,
                "run_count": run_count,
            }),
        )

    def record_partition_gauge(
        self,
        topic: str,
        partition: str,
        file_count: int,
        bytes_total: int,
        uncompacted_partitions_count: int | None = None,
    ) -> None:
        """Record point-in-time gauge metrics for a specific Bronze partition.

        Args:
            topic: Kafka topic or domain dataset name.
            partition: Partition path or date key (e.g. 'year=2026/month=11/day=02').
            file_count: Current number of Parquet files in the partition.
            bytes_total: Total byte size of Parquet files in the partition.
            uncompacted_partitions_count: Optional count of uncompacted partitions for the topic.
        """
        with self._lock:
            self.set_gauge(
                "bronze_partition_file_count",
                value=float(file_count),
                labels={"topic": topic, "partition": partition},
            )
            self.set_gauge(
                "bronze_partition_bytes",
                value=float(bytes_total),
                labels={"topic": topic, "partition": partition},
            )
            if uncompacted_partitions_count is not None:
                self.set_gauge(
                    "bronze_uncompacted_partitions_count",
                    value=float(uncompacted_partitions_count),
                    labels={"topic": topic},
                )

        logger.debug(
            "Bronze partition gauge recorded: %s",
            json.dumps({
                "event": "bronze_metric_partition_gauge",
                "topic": topic,
                "partition": partition,
                "file_count": file_count,
                "bytes_total": bytes_total,
                "uncompacted_partitions_count": uncompacted_partitions_count,
            }),
        )

    def record_uncompacted_partitions(self, topic: str, count: int) -> None:
        """Set the count of uncompacted partitions for a topic.

        Args:
            topic: Kafka topic or domain dataset name.
            count: Number of uncompacted partitions.
        """
        self.set_gauge(
            "bronze_uncompacted_partitions_count",
            value=float(count),
            labels={"topic": topic},
        )

    def record_retention(
        self,
        pruned_objects: int,
        pruned_bytes: int,
        duration_seconds: float,
    ) -> None:
        """Record telemetry for raw landing zone retention and cleanup purge.

        Args:
            pruned_objects: Number of expired landing objects deleted/pruned.
            pruned_bytes: Total bytes freed in MinIO / S3.
            duration_seconds: Retention execution duration in seconds.
        """
        with self._lock:
            self.inc_counter(
                "bronze_landing_pruned_objects_total",
                value=float(pruned_objects),
            )
            self.inc_counter(
                "bronze_landing_pruned_bytes_total",
                value=float(pruned_bytes),
            )
            self.observe_histogram(
                "bronze_retention_prune_duration_seconds",
                value=float(duration_seconds),
            )

        logger.info(
            "Bronze retention metric recorded: %s",
            json.dumps({
                "event": "bronze_metric_retention",
                "pruned_objects": pruned_objects,
                "pruned_bytes": pruned_bytes,
                "duration_seconds": duration_seconds,
            }),
        )

    def get_snapshot(self) -> BronzeMetricSnapshot:
        """Capture an immutable point-in-time snapshot of all Bronze metrics.

        Returns:
            BronzeMetricSnapshot instance.
        """
        with self._lock:
            counters_copy = {name: dict(series) for name, series in self._counters.items()}
            gauges_copy = {name: dict(series) for name, series in self._gauges.items()}
            histograms_copy = {
                name: {key: series_data.to_dict() for key, series_data in series.items()}
                for name, series in self._histograms.items()
            }
            return BronzeMetricSnapshot(
                timestamp=time.time(),
                counters=counters_copy,
                gauges=gauges_copy,
                histograms=histograms_copy,
            )

    def generate_prometheus_text(self) -> str:
        """Format all registered metrics into standard Prometheus text exposition format (0.0.4).

        Returns:
            Multi-line string containing # HELP, # TYPE, and metric samples with trailing newline.
        """
        lines: list[str] = []
        with self._lock:
            for name, (m_type, desc) in self._metric_metadata.items():
                family_lines: list[str] = []

                if m_type == "counter":
                    series = self._counters.get(name, {})
                    if series:
                        family_lines.append(f"# HELP {name} {desc}")
                        family_lines.append(f"# TYPE {name} {m_type}")
                        for label_tuples in sorted(series.keys()):
                            labels_dict = dict(label_tuples)
                            labels_str = _format_labels(labels_dict)
                            val = series[label_tuples]
                            family_lines.append(f"{name}{labels_str} {_format_metric_value(val)}")

                elif m_type == "gauge":
                    series = self._gauges.get(name, {})
                    if series:
                        family_lines.append(f"# HELP {name} {desc}")
                        family_lines.append(f"# TYPE {name} {m_type}")
                        for label_tuples in sorted(series.keys()):
                            labels_dict = dict(label_tuples)
                            labels_str = _format_labels(labels_dict)
                            val = series[label_tuples]
                            family_lines.append(f"{name}{labels_str} {_format_metric_value(val)}")

                elif m_type == "histogram":
                    series = self._histograms.get(name, {})
                    if series:
                        family_lines.append(f"# HELP {name} {desc}")
                        family_lines.append(f"# TYPE {name} {m_type}")
                        for label_tuples in sorted(series.keys()):
                            labels_dict = dict(label_tuples)
                            h_data = series[label_tuples]
                            for b in h_data.buckets:
                                bucket_labels = {**labels_dict, "le": _format_metric_value(b)}
                                labels_str = _format_labels(bucket_labels)
                                count = h_data.counts[b]
                                family_lines.append(f"{name}_bucket{labels_str} {count}")
                            inf_labels = {**labels_dict, "le": "+Inf"}
                            family_lines.append(
                                f"{name}_bucket{_format_labels(inf_labels)} {h_data.inf_count}"
                            )
                            base_labels_str = _format_labels(labels_dict)
                            family_lines.append(
                                f"{name}_sum{base_labels_str} {_format_metric_value(h_data.total_sum)}"
                            )
                            family_lines.append(f"{name}_count{base_labels_str} {h_data.inf_count}")

                if family_lines:
                    lines.extend(family_lines)
                    lines.append("")

        return "\n".join(lines).strip() + "\n" if lines else ""

    def push_to_gateway(
        self,
        pushgateway_url: str,
        job_name: str = "bronze_processing",
        grouping_key: dict[str, str] | None = None,
        timeout: float = 10.0,
        method: str = "POST",
    ) -> int:
        """Push current metrics exposition to Prometheus Pushgateway.

        Args:
            pushgateway_url: Base URL of Prometheus Pushgateway (e.g. 'http://localhost:9091').
            job_name: Job name label assigned in Pushgateway (defaults to 'bronze_processing').
            grouping_key: Optional additional grouping labels (e.g. {'instance': 'worker-1'}).
            timeout: HTTP request timeout in seconds.
            method: HTTP method ('POST' to append/replace metrics, 'PUT' to overwrite job group).

        Returns:
            HTTP response status code.

        Raises:
            Exception: If Pushgateway transmission fails or responds with error status.
        """
        base_url = pushgateway_url.rstrip("/")
        url_path = f"{base_url}/metrics/job/{urllib.parse.quote(job_name, safe='')}"
        if grouping_key:
            for k, v in sorted(grouping_key.items()):
                url_path += f"/{urllib.parse.quote(k, safe='')}/{urllib.parse.quote(v, safe='')}"

        payload = self.generate_prometheus_text().encode("utf-8")
        headers = {"Content-Type": "text/plain; version=0.0.4; charset=utf-8"}
        effective_method = method.upper().strip()

        try:
            if requests is not None:
                resp = requests.request(
                    method=effective_method,
                    url=url_path,
                    data=payload,
                    headers=headers,
                    timeout=timeout,
                )
                resp.raise_for_status()
                status_code = resp.status_code
            else:
                req = urllib.request.Request(
                    url=url_path,
                    data=payload,
                    headers=headers,
                    method=effective_method,
                )
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    status_code = response.status

            logger.info(
                "Pushed Bronze metrics to Prometheus Pushgateway: %s",
                json.dumps({
                    "event": "pushgateway_push_success",
                    "url": url_path,
                    "job": job_name,
                    "status_code": status_code,
                    "bytes_sent": len(payload),
                }),
            )
            return status_code
        except Exception as err:
            logger.error(
                "Failed to push Bronze metrics to Prometheus Pushgateway: %s",
                json.dumps({
                    "event": "pushgateway_push_error",
                    "url": url_path,
                    "job": job_name,
                    "error": str(err),
                }),
            )
            raise


_default_registry = BronzeMetricsRegistry()


def get_bronze_metrics_registry() -> BronzeMetricsRegistry:
    """Return the global default BronzeMetricsRegistry instance."""
    return _default_registry


def reset_bronze_metrics_registry() -> None:
    """Reset the global default BronzeMetricsRegistry instance."""
    _default_registry.reset()
