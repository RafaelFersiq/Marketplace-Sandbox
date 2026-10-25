"""Fault-tolerant supervisor and automatic recovery for PySpark Structured Streaming.

Provides StreamingRecoverySupervisor for managing resilient lifecycle execution of
streaming queries, automatic restart with exponential backoff on transient failures,
responsive signal handling (SIGINT/SIGTERM), and clean offset checkpoint commits.
"""

import logging
import random
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from types import FrameType
from typing import Any

from processing.streaming.config import SparkStreamingConfig

logger = logging.getLogger("streaming.recovery")


class StreamingRecoveryError(Exception):
    """Base exception for streaming query recovery failures."""


class StreamingRecoveryExhaustedError(StreamingRecoveryError):
    """Raised when maximum recovery restart retries have been exhausted."""

    def __init__(self, attempts: int, last_error: Exception | None = None) -> None:
        """Initialize StreamingRecoveryExhaustedError.

        Args:
            attempts: Total number of failed attempts made.
            last_error: Last caught exception that triggered failure.
        """
        self.attempts = attempts
        self.last_error = last_error
        msg = f"Streaming query recovery exhausted after {attempts} attempts. Last error: {last_error}"
        super().__init__(msg)


@dataclass
class RecoveryMetrics:
    """Telemetry and operational metrics for streaming query recovery supervisor."""

    total_restarts: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    last_error: str | None = None
    start_time: float = 0.0
    end_time: float | None = None
    attempt_history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def elapsed_seconds(self) -> float:
        """Calculate total runtime in seconds."""
        if self.start_time == 0.0:
            return 0.0
        current = self.end_time if self.end_time is not None else time.monotonic()
        return max(0.0, current - self.start_time)


class StreamingRecoverySupervisor:
    """Supervises PySpark Structured Streaming queries with automatic restart and backoff."""

    def __init__(
        self,
        config: SparkStreamingConfig | None = None,
        max_retries: int | None = None,
        initial_backoff_seconds: float | None = None,
        max_backoff_seconds: float | None = None,
        backoff_multiplier: float = 2.0,
        jitter_factor: float = 0.1,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        """Initialize StreamingRecoverySupervisor.

        Args:
            config: Optional SparkStreamingConfig instance providing defaults.
            max_retries: Maximum number of restart attempts before giving up.
            initial_backoff_seconds: Initial backoff delay in seconds.
            max_backoff_seconds: Upper ceiling on backoff delay.
            backoff_multiplier: Growth factor applied per retry attempt.
            jitter_factor: Random jitter coefficient in [0.0, 1.0].
            poll_interval_seconds: Polling sleep interval when checking query status.
        """
        self.config = config
        self.max_retries = max_retries if max_retries is not None else (config.max_retries if config is not None else 5)
        self.initial_backoff_seconds = (
            initial_backoff_seconds
            if initial_backoff_seconds is not None
            else (config.initial_backoff_seconds if config is not None else 2.0)
        )
        self.max_backoff_seconds = (
            max_backoff_seconds
            if max_backoff_seconds is not None
            else (config.max_backoff_seconds if config is not None else 60.0)
        )
        self.backoff_multiplier = backoff_multiplier
        self.jitter_factor = jitter_factor
        self.poll_interval_seconds = poll_interval_seconds

        # Validate parameters
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.initial_backoff_seconds <= 0:
            raise ValueError(f"initial_backoff_seconds must be > 0, got {self.initial_backoff_seconds}")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                f"max_backoff_seconds ({self.max_backoff_seconds}) must be >= "
                f"initial_backoff_seconds ({self.initial_backoff_seconds})"
            )
        if self.backoff_multiplier < 1.0:
            raise ValueError(f"backoff_multiplier must be >= 1.0, got {self.backoff_multiplier}")
        if not (0.0 <= self.jitter_factor <= 1.0):
            raise ValueError(f"jitter_factor must be between 0.0 and 1.0, got {self.jitter_factor}")

        self._shutdown_requested = threading.Event()
        self._active_queries: list[Any] = []
        self.metrics = RecoveryMetrics()

    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been signaled."""
        return self._shutdown_requested.is_set()

    def request_shutdown(self) -> None:
        """Set shutdown flag and stop active streaming queries immediately."""
        self._shutdown_requested.set()
        self.stop_active_queries()

    def reset(self) -> None:
        """Reset internal shutdown flags and active query lists for reuse."""
        self._shutdown_requested.clear()
        self._active_queries.clear()
        self.metrics = RecoveryMetrics()

    def compute_backoff_delay(self, attempt: int, with_jitter: bool = True) -> float:
        """Calculate exponential backoff delay for a given retry attempt.

        Formula:
            min(max_backoff_seconds, initial_backoff_seconds * (backoff_multiplier ** (attempt - 1)))

        Args:
            attempt: 1-indexed attempt number (must be >= 1).
            with_jitter: If True and jitter_factor > 0, apply bounded random jitter.

        Returns:
            Calculated delay in seconds.
        """
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt}")

        base_delay = min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (self.backoff_multiplier ** (attempt - 1)),
        )

        if not with_jitter or self.jitter_factor == 0.0:
            return float(base_delay)

        jitter_range = self.jitter_factor * base_delay
        jitter_delta = random.uniform(-jitter_range, jitter_range)
        delay = max(0.0, base_delay + jitter_delta)
        return float(min(self.max_backoff_seconds, delay))

    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        """Handle incoming OS termination signal (SIGINT, SIGTERM)."""
        signame = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info(
            "Termination signal received: %s (%d). Initiating graceful query shutdown...",
            signame,
            signum,
            extra={"event": "shutdown_signal_received", "signal": signame, "signal_number": signum},
        )
        self.request_shutdown()

    def register_signals(self) -> None:
        """Attach SIGINT and SIGTERM signal handlers if running in the main thread."""
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
            logger.debug("Successfully registered SIGINT and SIGTERM handlers.")
        except (ValueError, AttributeError):
            logger.debug("Signal handlers could not be registered (not running in main thread).")

    def stop_active_queries(self) -> None:
        """Gracefully stop all currently active streaming queries to commit checkpoints."""
        for query in self._active_queries:
            try:
                is_active = (
                    query.isActive()
                    if callable(getattr(query, "isActive", None))
                    else getattr(query, "isActive", False)
                )
                if is_active:
                    query_name = getattr(query, "name", None) or str(query)
                    logger.info(
                        "Stopping active streaming query: %s",
                        query_name,
                        extra={"event": "query_stop", "query": query_name},
                    )
                    query.stop()
            except Exception as exc:
                query_name = getattr(query, "name", None) or str(query)
                logger.warning(
                    "Error stopping query %s: %s",
                    query_name,
                    exc,
                    extra={"event": "query_stop_error", "query": query_name, "error": str(exc)},
                )

    def _sleep(self, duration: float, sleep_fn: Callable[[float], None]) -> None:
        """Sleep responsive to shutdown requests."""
        if sleep_fn is not time.sleep:
            sleep_fn(duration)
            return

        deadline = time.monotonic() + duration
        while time.monotonic() < deadline and not self._shutdown_requested.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.2, remaining))

    def run_with_recovery(
        self,
        start_queries_fn: Callable[[], list[Any]],
        sleep_fn: Callable[[float], None] = time.sleep,
        register_signal_handlers: bool = True,
    ) -> int:
        """Execute and monitor streaming queries with automatic recovery and exponential backoff.

        Args:
            start_queries_fn: Callable that starts and returns a list of active StreamingQuery objects.
            sleep_fn: Injected sleep function for backoff delays (useful for mocking).
            register_signal_handlers: Whether to attach SIGINT/SIGTERM signal handlers.

        Returns:
            Process exit code (0 for clean termination).

        Raises:
            StreamingRecoveryExhaustedError: If retries exceed max_retries limit.
        """
        if register_signal_handlers:
            self.register_signals()

        self.metrics.start_time = time.monotonic()
        attempt = 1

        logger.info(
            "Starting streaming recovery supervisor (max_retries=%d, initial_backoff=%.1fs, max_backoff=%.1fs)",
            self.max_retries,
            self.initial_backoff_seconds,
            self.max_backoff_seconds,
            extra={
                "event": "supervisor_started",
                "max_retries": self.max_retries,
                "initial_backoff_seconds": self.initial_backoff_seconds,
                "max_backoff_seconds": self.max_backoff_seconds,
            },
        )

        while not self._shutdown_requested.is_set():
            try:
                logger.info(
                    "Starting streaming queries (attempt %d/%d)...",
                    attempt,
                    self.max_retries,
                    extra={"event": "queries_starting", "attempt": attempt, "max_retries": self.max_retries},
                )
                raw_queries = start_queries_fn()
                self._active_queries = raw_queries if isinstance(raw_queries, list) else [raw_queries]

                if not self._active_queries:
                    logger.info("No queries returned by start_queries_fn. Exiting cleanly.")
                    self.metrics.successful_runs += 1
                    self.metrics.end_time = time.monotonic()
                    return 0

                # Monitor active queries
                query_failed = False
                failure_exc: Exception | None = None

                while not self._shutdown_requested.is_set():
                    all_terminated = True

                    for query in self._active_queries:
                        # Check for query failure
                        exc: Any = None
                        if hasattr(query, "exception"):
                            exc_attr = query.exception
                            exc = exc_attr() if callable(exc_attr) else exc_attr

                        if exc is not None:
                            query_failed = True
                            failure_exc = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
                            break

                        # Check if query is still running
                        is_active = False
                        if hasattr(query, "isActive"):
                            active_attr = query.isActive
                            is_active = active_attr() if callable(active_attr) else bool(active_attr)

                        if is_active:
                            all_terminated = False

                    if query_failed:
                        raise failure_exc or RuntimeError("Streaming query failed with unspecified exception")

                    if all_terminated:
                        logger.info(
                            "All streaming queries terminated cleanly without exception.",
                            extra={"event": "queries_terminated_cleanly", "attempt": attempt},
                        )
                        self.metrics.successful_runs += 1
                        self.metrics.end_time = time.monotonic()
                        return 0

                    if self._shutdown_requested.is_set():
                        break

                    self._sleep(self.poll_interval_seconds, sleep_fn)

                if self._shutdown_requested.is_set():
                    logger.info(
                        "Shutdown requested during streaming execution. Stopping queries...",
                        extra={"event": "shutdown_during_execution"},
                    )
                    self.stop_active_queries()
                    self.metrics.end_time = time.monotonic()
                    return 0

            except Exception as exc:
                if self._shutdown_requested.is_set():
                    logger.info(
                        "Shutdown requested while handling query error. Stopping queries and exiting.",
                        extra={"event": "shutdown_during_error"},
                    )
                    self.stop_active_queries()
                    self.metrics.end_time = time.monotonic()
                    return 0

                self.metrics.last_error = str(exc)
                self.metrics.failed_runs += 1
                self.stop_active_queries()

                attempt_record = {
                    "attempt": attempt,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                    "timestamp": time.monotonic(),
                }
                self.metrics.attempt_history.append(attempt_record)

                if attempt >= self.max_retries:
                    self.metrics.end_time = time.monotonic()
                    logger.critical(
                        "Streaming recovery retries exhausted (%d/%d). Terminating with error: %s",
                        attempt,
                        self.max_retries,
                        exc,
                        extra={
                            "event": "recovery_exhausted",
                            "attempt": attempt,
                            "max_retries": self.max_retries,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc),
                        },
                    )
                    raise StreamingRecoveryExhaustedError(attempts=attempt, last_error=exc) from exc

                backoff_delay = self.compute_backoff_delay(attempt)
                logger.warning(
                    "Streaming query failure on attempt %d/%d: %s. Retrying in %.2f seconds...",
                    attempt,
                    self.max_retries,
                    exc,
                    backoff_delay,
                    extra={
                        "event": "recovery_retry",
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "backoff_seconds": backoff_delay,
                        "error_type": exc.__class__.__name__,
                        "error_message": str(exc),
                    },
                )

                self.metrics.total_restarts += 1
                self._sleep(backoff_delay, sleep_fn)
                attempt += 1

        self.stop_active_queries()
        self.metrics.end_time = time.monotonic()
        return 0
