"""PySpark Structured Streaming metrics collection, Prometheus exposition, and listener.

Provides QueryProgressSnapshot dataclass, thread-safe in-memory PrometheusMetricsRegistry,
and StreamingMetricsListener subclassing PySpark's StreamingQueryListener to capture
micro-batch throughput, trigger latencies, state store footprints, and query terminations
with structured JSON audit logging and standard Prometheus text-format exposition.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
from dataclasses import asdict, dataclass
from typing import Any

try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError:  # pragma: no cover

    class StreamingQueryListener:  # type: ignore[no-redef]
        """Fallback base class when PySpark is not installed."""

        def onQueryStarted(self, event: Any) -> None:  # noqa: N802
            """Handle query started event."""

        def onQueryProgress(self, event: Any) -> None:  # noqa: N802
            """Handle query progress event."""

        def onQueryIdle(self, event: Any) -> None:  # noqa: N802
            """Handle query idle event."""

        def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
            """Handle query terminated event."""


logger = logging.getLogger("processing.streaming.metrics")


@dataclass(frozen=True)
class QueryProgressSnapshot:
    """Immutable snapshot of a single micro-batch execution progress."""

    query_id: str
    run_id: str
    query_name: str
    batch_id: int
    timestamp: str
    num_input_rows: int
    input_rows_per_second: float
    processed_rows_per_second: float
    batch_duration_ms: int
    event_time_watermark: str | None = None
    state_rows_total: int = 0
    state_memory_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot into a serializable dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize snapshot into a JSON-formatted string."""
        return json.dumps(self.to_dict(), default=str)


METRIC_SPECIFICATIONS: list[dict[str, str]] = [
    {
        "name": "spark_streaming_input_rows_total",
        "type": "counter",
        "help": "Total number of input rows read by the streaming query.",
    },
    {
        "name": "spark_streaming_input_rows_per_second",
        "type": "gauge",
        "help": "Current input rows per second ingested by the streaming query.",
    },
    {
        "name": "spark_streaming_processed_rows_per_second",
        "type": "gauge",
        "help": "Current processed rows per second processed by the streaming query.",
    },
    {
        "name": "spark_streaming_batch_duration_ms",
        "type": "gauge",
        "help": "Total duration of the most recent micro-batch trigger execution in milliseconds.",
    },
    {
        "name": "spark_streaming_batches_completed_total",
        "type": "counter",
        "help": "Total number of micro-batches completed by the streaming query.",
    },
    {
        "name": "spark_streaming_query_errors_total",
        "type": "counter",
        "help": "Total number of query errors or terminations with failure.",
    },
    {
        "name": "spark_streaming_state_rows_total",
        "type": "gauge",
        "help": "Total number of active state rows retained across stateful operators.",
    },
    {
        "name": "spark_streaming_state_memory_bytes",
        "type": "gauge",
        "help": "Total memory consumed by stateful operators in bytes.",
    },
]


class PrometheusMetricsRegistry:
    """Thread-safe in-memory Prometheus metrics collector and text exposition exporter."""

    def __init__(self) -> None:
        """Initialize in-memory metrics storage and synchronization lock."""
        self._lock = threading.Lock()
        self._known_queries: set[str] = set()

        self._input_rows_total: dict[str, int] = {}
        self._batches_completed_total: dict[str, int] = {}
        self._query_errors_total: dict[str, int] = {}

        self._input_rows_per_second: dict[str, float] = {}
        self._processed_rows_per_second: dict[str, float] = {}
        self._batch_duration_ms: dict[str, int] = {}
        self._state_rows_total: dict[str, int] = {}
        self._state_memory_bytes: dict[str, int] = {}

        self._latest_snapshots: dict[str, QueryProgressSnapshot] = {}
        self._latest_errors: dict[str, str] = {}

    def record_progress(self, snapshot: QueryProgressSnapshot) -> None:
        """Record micro-batch progress metrics for a streaming query."""
        with self._lock:
            qn = snapshot.query_name or "default"
            self._known_queries.add(qn)

            self._input_rows_total[qn] = self._input_rows_total.get(qn, 0) + snapshot.num_input_rows
            self._batches_completed_total[qn] = self._batches_completed_total.get(qn, 0) + 1

            self._input_rows_per_second[qn] = float(snapshot.input_rows_per_second)
            self._processed_rows_per_second[qn] = float(snapshot.processed_rows_per_second)
            self._batch_duration_ms[qn] = int(snapshot.batch_duration_ms)
            self._state_rows_total[qn] = int(snapshot.state_rows_total)
            self._state_memory_bytes[qn] = int(snapshot.state_memory_bytes)

            self._latest_snapshots[qn] = snapshot

    def record_error(self, query_name: str, error_message: str) -> None:
        """Record query termination failure or processing exception."""
        with self._lock:
            qn = query_name or "default"
            self._known_queries.add(qn)
            self._query_errors_total[qn] = self._query_errors_total.get(qn, 0) + 1
            self._latest_errors[qn] = error_message

    def get_metric_value(self, metric_name: str, query_name: str) -> int | float | None:
        """Retrieve current value for a given metric and query name."""
        with self._lock:
            qn = query_name or "default"
            match metric_name:
                case "spark_streaming_input_rows_total":
                    return self._input_rows_total.get(qn, 0)
                case "spark_streaming_batches_completed_total":
                    return self._batches_completed_total.get(qn, 0)
                case "spark_streaming_query_errors_total":
                    return self._query_errors_total.get(qn, 0)
                case "spark_streaming_input_rows_per_second":
                    return self._input_rows_per_second.get(qn, 0.0)
                case "spark_streaming_processed_rows_per_second":
                    return self._processed_rows_per_second.get(qn, 0.0)
                case "spark_streaming_batch_duration_ms":
                    return self._batch_duration_ms.get(qn, 0)
                case "spark_streaming_state_rows_total":
                    return self._state_rows_total.get(qn, 0)
                case "spark_streaming_state_memory_bytes":
                    return self._state_memory_bytes.get(qn, 0)
                case _:
                    return None

    def get_snapshot(self, query_name: str) -> QueryProgressSnapshot | None:
        """Retrieve most recent progress snapshot for a query."""
        with self._lock:
            return self._latest_snapshots.get(query_name)

    def get_latest_error(self, query_name: str) -> str | None:
        """Retrieve most recent error message recorded for a query."""
        with self._lock:
            return self._latest_errors.get(query_name)

    def reset(self) -> None:
        """Clear all registered metrics and snapshots."""
        with self._lock:
            self._known_queries.clear()
            self._input_rows_total.clear()
            self._batches_completed_total.clear()
            self._query_errors_total.clear()
            self._input_rows_per_second.clear()
            self._processed_rows_per_second.clear()
            self._batch_duration_ms.clear()
            self._state_rows_total.clear()
            self._state_memory_bytes.clear()
            self._latest_snapshots.clear()
            self._latest_errors.clear()

    def format_prometheus_text(self) -> str:
        """Render all recorded metrics into standard Prometheus exposition format."""
        with self._lock:
            if not self._known_queries:
                return ""

            sorted_queries = sorted(self._known_queries)
            lines: list[str] = []

            for spec in METRIC_SPECIFICATIONS:
                metric_name = spec["name"]
                metric_type = spec["type"]
                metric_help = spec["help"]

                lines.append(f"# HELP {metric_name} {metric_help}")
                lines.append(f"# TYPE {metric_name} {metric_type}")

                for qn in sorted_queries:
                    raw_val = self._resolve_raw_metric(metric_name, qn)
                    escaped_qn = qn.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
                    lines.append(f'{metric_name}{{query_name="{escaped_qn}"}} {raw_val}')

            return "\n".join(lines) + "\n"

    def _resolve_raw_metric(self, metric_name: str, query_name: str) -> str:
        """Resolve metric value formatted for Prometheus sample line."""
        match metric_name:
            case "spark_streaming_input_rows_total":
                return str(self._input_rows_total.get(query_name, 0))
            case "spark_streaming_batches_completed_total":
                return str(self._batches_completed_total.get(query_name, 0))
            case "spark_streaming_query_errors_total":
                return str(self._query_errors_total.get(query_name, 0))
            case "spark_streaming_input_rows_per_second":
                return str(float(self._input_rows_per_second.get(query_name, 0.0)))
            case "spark_streaming_processed_rows_per_second":
                return str(float(self._processed_rows_per_second.get(query_name, 0.0)))
            case "spark_streaming_batch_duration_ms":
                return str(self._batch_duration_ms.get(query_name, 0))
            case "spark_streaming_state_rows_total":
                return str(self._state_rows_total.get(query_name, 0))
            case "spark_streaming_state_memory_bytes":
                return str(self._state_memory_bytes.get(query_name, 0))
            case _:
                return "0"


def _get_field(obj: Any, field_name: str, default: Any = None) -> Any:
    """Safely extract field from an object attribute or dictionary mapping."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        val = obj.get(field_name, default)
        return val if val is not None else default
    if hasattr(obj, field_name):
        val = getattr(obj, field_name)
        return val if val is not None else default
    if hasattr(obj, "get") and callable(obj.get):
        try:
            val = obj.get(field_name, default)
            return val if val is not None else default
        except (TypeError, KeyError, AttributeError):
            return default
    return default


def _extract_batch_duration_ms(progress: Any) -> int:
    """Extract batch duration in milliseconds from progress object or dictionary."""
    direct_duration = _get_field(progress, "batchDuration", None)
    if direct_duration is not None:
        try:
            return int(direct_duration)
        except (ValueError, TypeError):
            pass

    duration_ms = _get_field(progress, "durationMs", None)
    if isinstance(duration_ms, dict):
        if "triggerExecution" in duration_ms:
            try:
                return int(duration_ms["triggerExecution"])
            except (ValueError, TypeError):
                pass
        total_ms = 0
        for v in duration_ms.values():
            if isinstance(v, (int, float)):
                total_ms += int(v)
        return total_ms

    return 0


def _extract_watermark(progress: Any) -> str | None:
    """Extract watermark timestamp string from progress eventTime structure."""
    event_time = _get_field(progress, "eventTime", None)
    if isinstance(event_time, dict):
        wm = event_time.get("watermark")
        if wm is not None and str(wm).strip():
            return str(wm).strip()
    elif event_time is not None and hasattr(event_time, "watermark"):
        wm = event_time.watermark
        if wm is not None and str(wm).strip():
            return str(wm).strip()
    return None


def _extract_state_metrics(progress: Any) -> tuple[int, int]:
    """Extract total state rows and memory bytes across all state operators."""
    state_operators = _get_field(progress, "stateOperators", [])
    total_rows = 0
    total_memory = 0

    if isinstance(state_operators, (list, tuple)):
        for op in state_operators:
            num_rows = _get_field(op, "numRowsTotal", 0)
            memory_bytes = _get_field(op, "memoryUsedBytes", 0)
            with contextlib.suppress(ValueError, TypeError):
                total_rows += int(num_rows)
            with contextlib.suppress(ValueError, TypeError):
                total_memory += int(memory_bytes)

    return total_rows, total_memory


class StreamingMetricsListener(StreamingQueryListener):
    """PySpark StreamingQueryListener capturing progress, idle, and termination events."""

    def __init__(
        self,
        metrics_registry: PrometheusMetricsRegistry | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize listener with Prometheus metrics registry and logger."""
        super().__init__()
        self.metrics_registry: PrometheusMetricsRegistry = (
            metrics_registry if metrics_registry is not None else PrometheusMetricsRegistry()
        )
        self.logger: logging.Logger = (
            logger if logger is not None else logging.getLogger("processing.streaming.metrics")
        )
        self._query_names: dict[str, str] = {}
        self._lock = threading.Lock()

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802
        """Handle query started lifecycle event."""
        query_id = str(_get_field(event, "id", "") or "")
        run_id = str(_get_field(event, "runId", "") or "")
        query_name = str(_get_field(event, "name", "") or "unnamed")
        timestamp = str(_get_field(event, "timestamp", "") or "")

        with self._lock:
            if query_id:
                self._query_names[query_id] = query_name
            if run_id:
                self._query_names[run_id] = query_name

        payload = {
            "event": "streaming_query_started",
            "id": query_id,
            "runId": run_id,
            "name": query_name,
            "timestamp": timestamp,
        }
        self.logger.info(json.dumps(payload))

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        """Handle micro-batch progress event and update Prometheus metrics."""
        progress = _get_field(event, "progress", event)

        query_id = str(_get_field(progress, "id", "") or "")
        run_id = str(_get_field(progress, "runId", "") or "")
        query_name_raw = _get_field(progress, "name", None)

        with self._lock:
            cached_name = self._query_names.get(query_id) or self._query_names.get(run_id)
            query_name = str(query_name_raw or cached_name or "unnamed")
            if query_id:
                self._query_names[query_id] = query_name
            if run_id:
                self._query_names[run_id] = query_name

        batch_id_raw = _get_field(progress, "batchId", 0)
        try:
            batch_id = int(batch_id_raw)
        except (ValueError, TypeError):
            batch_id = 0

        timestamp = str(_get_field(progress, "timestamp", "") or "")

        num_input_rows_raw = _get_field(progress, "numInputRows", 0)
        try:
            num_input_rows = int(num_input_rows_raw)
        except (ValueError, TypeError):
            num_input_rows = 0

        input_rows_per_sec_raw = _get_field(progress, "inputRowsPerSecond", 0.0)
        try:
            input_rows_per_second = float(input_rows_per_sec_raw)
        except (ValueError, TypeError):
            input_rows_per_second = 0.0

        processed_rows_per_sec_raw = _get_field(progress, "processedRowsPerSecond", 0.0)
        try:
            processed_rows_per_second = float(processed_rows_per_sec_raw)
        except (ValueError, TypeError):
            processed_rows_per_second = 0.0

        batch_duration_ms = _extract_batch_duration_ms(progress)
        watermark = _extract_watermark(progress)
        state_rows, state_mem = _extract_state_metrics(progress)

        snapshot = QueryProgressSnapshot(
            query_id=query_id,
            run_id=run_id,
            query_name=query_name,
            batch_id=batch_id,
            timestamp=timestamp,
            num_input_rows=num_input_rows,
            input_rows_per_second=input_rows_per_second,
            processed_rows_per_second=processed_rows_per_second,
            batch_duration_ms=batch_duration_ms,
            event_time_watermark=watermark,
            state_rows_total=state_rows,
            state_memory_bytes=state_mem,
        )

        if self.metrics_registry is not None:
            self.metrics_registry.record_progress(snapshot)

        payload = {
            "event": "streaming_query_progress",
            **snapshot.to_dict(),
        }
        self.logger.info(json.dumps(payload))

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        """Handle query idle lifecycle event."""
        query_id = str(_get_field(event, "id", "") or "")
        run_id = str(_get_field(event, "runId", "") or "")
        timestamp = str(_get_field(event, "timestamp", "") or "")

        with self._lock:
            query_name = self._query_names.get(query_id) or self._query_names.get(run_id) or "unnamed"

        payload = {
            "event": "streaming_query_idle",
            "id": query_id,
            "runId": run_id,
            "name": query_name,
            "timestamp": timestamp,
        }
        self.logger.debug(json.dumps(payload))

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        """Handle query termination lifecycle event, capturing failure diagnostics."""
        query_id = str(_get_field(event, "id", "") or "")
        run_id = str(_get_field(event, "runId", "") or "")

        with self._lock:
            query_name = self._query_names.get(query_id) or self._query_names.get(run_id) or "unnamed"

        exception = _get_field(event, "exception", None)
        has_exception = (
            exception is not None and str(exception).strip() != "" and str(exception).strip().lower() != "none"
        )

        if has_exception:
            error_message = str(exception)
            if self.metrics_registry is not None:
                self.metrics_registry.record_error(query_name, error_message)

            payload = {
                "event": "streaming_query_terminated_with_error",
                "id": query_id,
                "runId": run_id,
                "name": query_name,
                "error": error_message,
            }
            self.logger.error(json.dumps(payload))
        else:
            payload = {
                "event": "streaming_query_terminated",
                "id": query_id,
                "runId": run_id,
                "name": query_name,
            }
            self.logger.info(json.dumps(payload))
