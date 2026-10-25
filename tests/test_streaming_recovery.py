"""Unit tests for Spark Structured Streaming fault-tolerant recovery supervisor."""

from typing import Any

import pytest

from processing.streaming.config import SparkStreamingConfig
from processing.streaming.recovery import (
    RecoveryMetrics,
    StreamingRecoveryExhaustedError,
    StreamingRecoverySupervisor,
)


class MockStreamingQuery:
    """Mock PySpark StreamingQuery for supervisor unit testing."""

    def __init__(
        self,
        name: str = "mock_query",
        is_active: bool = True,
        exception: Exception | None = None,
    ) -> None:
        self.name = name
        self._is_active = is_active
        self._exception = exception
        self.stop_called = False

    def isActive(self) -> bool:  # noqa: N802
        return self._is_active and not self.stop_called

    def exception(self) -> Exception | None:
        return self._exception

    def stop(self) -> None:
        self.stop_called = True
        self._is_active = False


class TestStreamingRecoverySupervisor:
    """Test suite for StreamingRecoverySupervisor backoff, signals, and restart loops."""

    def test_init_defaults(self) -> None:
        supervisor = StreamingRecoverySupervisor()
        assert supervisor.max_retries == 5
        assert supervisor.initial_backoff_seconds == 2.0
        assert supervisor.max_backoff_seconds == 60.0
        assert supervisor.backoff_multiplier == 2.0
        assert supervisor.jitter_factor == 0.1
        assert supervisor.is_shutdown_requested is False

    def test_init_from_spark_streaming_config(self) -> None:
        config = SparkStreamingConfig(
            max_retries=10,
            initial_backoff_seconds=3.0,
            max_backoff_seconds=45.0,
        )
        supervisor = StreamingRecoverySupervisor(config=config)
        assert supervisor.max_retries == 10
        assert supervisor.initial_backoff_seconds == 3.0
        assert supervisor.max_backoff_seconds == 45.0

    def test_init_parameter_validation_raises(self) -> None:
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            StreamingRecoverySupervisor(max_retries=-1)

        with pytest.raises(ValueError, match="initial_backoff_seconds must be > 0"):
            StreamingRecoverySupervisor(initial_backoff_seconds=0.0)

        with pytest.raises(ValueError, match=r"max_backoff_seconds .* must be >= initial_backoff_seconds"):
            StreamingRecoverySupervisor(initial_backoff_seconds=10.0, max_backoff_seconds=5.0)

        with pytest.raises(ValueError, match=r"backoff_multiplier must be >= 1\.0"):
            StreamingRecoverySupervisor(backoff_multiplier=0.5)

        with pytest.raises(ValueError, match=r"jitter_factor must be between 0\.0 and 1\.0"):
            StreamingRecoverySupervisor(jitter_factor=1.5)

    def test_compute_backoff_delay_exponential_growth(self) -> None:
        supervisor = StreamingRecoverySupervisor(
            initial_backoff_seconds=2.0,
            max_backoff_seconds=60.0,
            backoff_multiplier=2.0,
            jitter_factor=0.0,
        )
        assert supervisor.compute_backoff_delay(1, with_jitter=False) == 2.0
        assert supervisor.compute_backoff_delay(2, with_jitter=False) == 4.0
        assert supervisor.compute_backoff_delay(3, with_jitter=False) == 8.0
        assert supervisor.compute_backoff_delay(4, with_jitter=False) == 16.0
        assert supervisor.compute_backoff_delay(5, with_jitter=False) == 32.0
        assert supervisor.compute_backoff_delay(6, with_jitter=False) == 60.0

    def test_compute_backoff_delay_with_jitter_bounds(self) -> None:
        supervisor = StreamingRecoverySupervisor(
            initial_backoff_seconds=10.0,
            max_backoff_seconds=100.0,
            jitter_factor=0.2,
        )
        for _ in range(20):
            delay = supervisor.compute_backoff_delay(1, with_jitter=True)
            assert 8.0 <= delay <= 12.0

    def test_compute_backoff_delay_invalid_attempt_raises(self) -> None:
        supervisor = StreamingRecoverySupervisor()
        with pytest.raises(ValueError, match="attempt must be >= 1"):
            supervisor.compute_backoff_delay(0)

    def test_run_with_recovery_clean_termination_no_errors(self) -> None:
        supervisor = StreamingRecoverySupervisor()
        mock_query = MockStreamingQuery(name="clean_query", is_active=False, exception=None)

        result = supervisor.run_with_recovery(
            start_queries_fn=lambda: [mock_query],
            register_signal_handlers=False,
        )
        assert result == 0
        assert supervisor.metrics.successful_runs == 1
        assert supervisor.metrics.failed_runs == 0
        assert supervisor.metrics.total_restarts == 0

    def test_run_with_recovery_empty_queries_clean_exit(self) -> None:
        supervisor = StreamingRecoverySupervisor()
        result = supervisor.run_with_recovery(
            start_queries_fn=lambda: [],
            register_signal_handlers=False,
        )
        assert result == 0
        assert supervisor.metrics.successful_runs == 1

    def test_run_with_recovery_recovers_after_transient_failure(self) -> None:
        supervisor = StreamingRecoverySupervisor(max_retries=3, initial_backoff_seconds=1.0)
        attempt_counter = 0
        slept_intervals: list[float] = []

        query_fail = MockStreamingQuery(name="failing_query", is_active=True, exception=RuntimeError("Kafka timeout"))
        query_ok = MockStreamingQuery(name="healthy_query", is_active=False, exception=None)

        def mock_start_queries() -> list[Any]:
            nonlocal attempt_counter
            attempt_counter += 1
            if attempt_counter == 1:
                return [query_fail]
            return [query_ok]

        result = supervisor.run_with_recovery(
            start_queries_fn=mock_start_queries,
            sleep_fn=slept_intervals.append,
            register_signal_handlers=False,
        )

        assert result == 0
        assert attempt_counter == 2
        assert supervisor.metrics.total_restarts == 1
        assert supervisor.metrics.successful_runs == 1
        assert len(slept_intervals) == 1
        assert query_fail.stop_called is True

    def test_run_with_recovery_exhausted_raises_exception(self) -> None:
        supervisor = StreamingRecoverySupervisor(max_retries=3, initial_backoff_seconds=1.0)
        slept_intervals: list[float] = []
        failing_query = MockStreamingQuery(
            name="always_fail",
            is_active=False,
            exception=ConnectionError("Broker down"),
        )

        with pytest.raises(StreamingRecoveryExhaustedError) as exc_info:
            supervisor.run_with_recovery(
                start_queries_fn=lambda: [failing_query],
                sleep_fn=slept_intervals.append,
                register_signal_handlers=False,
            )

        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, ConnectionError)
        assert supervisor.metrics.total_restarts == 2
        assert supervisor.metrics.failed_runs == 3
        assert len(slept_intervals) == 2

    def test_run_with_recovery_start_fn_raises_directly(self) -> None:
        supervisor = StreamingRecoverySupervisor(max_retries=2, initial_backoff_seconds=0.5)
        call_count = 0

        def failing_starter() -> list[Any]:
            nonlocal call_count
            call_count += 1
            raise OSError("Schema Registry unreachable")

        with pytest.raises(StreamingRecoveryExhaustedError):
            supervisor.run_with_recovery(
                start_queries_fn=failing_starter,
                sleep_fn=lambda _: None,
                register_signal_handlers=False,
            )

        assert call_count == 2
        assert supervisor.metrics.failed_runs == 2

    def test_request_shutdown_stops_active_queries_and_exits(self) -> None:
        supervisor = StreamingRecoverySupervisor(poll_interval_seconds=0.01)
        mock_query = MockStreamingQuery(name="running_query", is_active=True, exception=None)

        def starter() -> list[Any]:
            supervisor.request_shutdown()
            return [mock_query]

        result = supervisor.run_with_recovery(
            start_queries_fn=starter,
            register_signal_handlers=False,
        )
        assert result == 0
        assert mock_query.stop_called is True
        assert supervisor.is_shutdown_requested is True

    def test_signal_handling_triggers_shutdown(self) -> None:
        supervisor = StreamingRecoverySupervisor()
        mock_query = MockStreamingQuery(name="signal_query", is_active=True)
        supervisor._active_queries = [mock_query]

        supervisor._handle_signal(signum=15, _frame=None)

        assert supervisor.is_shutdown_requested is True
        assert mock_query.stop_called is True

    def test_metrics_elapsed_time(self) -> None:
        metrics = RecoveryMetrics(start_time=100.0, end_time=115.5)
        assert metrics.elapsed_seconds == 15.5
