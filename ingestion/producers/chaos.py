"""Chaos and resilience simulation engine for streaming event pipelines.

Provides ChaosEngine, ChaosConfig, ChaosMetrics, and SchemaViolationType for simulating
traffic bursts, late-arriving/out-of-order data, message duplication, Dead Letter Queue (DLQ)
schema violations, binary wire format corruption, and transient Kafka network retry handling.
"""

from __future__ import annotations

import copy
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

import confluent_kafka

logger = logging.getLogger("producers.chaos")


class SchemaViolationType(StrEnum):
    """Supported payload corruption types for Dead Letter Queue (DLQ) simulation."""

    MISSING_REQUIRED = "missing_required"
    INVALID_TYPE = "invalid_type"
    NEGATIVE_AMOUNT = "negative_amount"
    CORRUPTED_STRUCTURE = "corrupted_structure"


@dataclass
class ChaosConfig:
    """Configuration parameters for chaos scenario injection."""

    duplicate_rate: float = 0.0
    delay_rate: float = 0.0
    max_delay_seconds: int = 3600
    invalid_payload_rate: float = 0.0
    burst_probability: float = 0.0
    burst_multiplier: float = 5.0
    burst_period_seconds: float = 60.0
    burst_duration_seconds: float = 10.0
    drop_rate: float = 0.0
    seed: int | None = None

    def __post_init__(self) -> None:
        """Validate chaos parameters and bounds."""
        for rate_name in ("duplicate_rate", "delay_rate", "invalid_payload_rate", "burst_probability", "drop_rate"):
            val = getattr(self, rate_name)
            if not isinstance(val, (int, float)) or not (0.0 <= float(val) <= 1.0):
                raise ValueError(f"Rate '{rate_name}' must be a float between 0.0 and 1.0, got: {val}")

        if self.max_delay_seconds < 0:
            raise ValueError(f"max_delay_seconds must be >= 0, got: {self.max_delay_seconds}")

        if self.burst_multiplier <= 0.0:
            raise ValueError(f"burst_multiplier must be > 0.0, got: {self.burst_multiplier}")

        if self.burst_period_seconds < 0.0:
            raise ValueError(f"burst_period_seconds must be >= 0.0, got: {self.burst_period_seconds}")

        if self.burst_duration_seconds < 0.0:
            raise ValueError(f"burst_duration_seconds must be >= 0.0, got: {self.burst_duration_seconds}")


@dataclass
class ChaosMetrics:
    """Telemetry counters for injected chaos events."""

    evaluated_count: int = 0
    delayed_count: int = 0
    duplicated_count: int = 0
    violated_count: int = 0
    dropped_count: int = 0


class ChaosEngine:
    """Simulation engine for stream resilience, data quality failures, and traffic bursts."""

    def __init__(
        self,
        config: ChaosConfig | None = None,
        duplicate_rate: float | None = None,
        delay_rate: float | None = None,
        max_delay_seconds: int | None = None,
        invalid_payload_rate: float | None = None,
        burst_probability: float | None = None,
        burst_multiplier: float | None = None,
        burst_period_seconds: float | None = None,
        burst_duration_seconds: float | None = None,
        drop_rate: float | None = None,
        seed: int | None = None,
    ) -> None:
        """Initialize ChaosEngine with configuration or explicit keyword parameters.

        Args:
            config: Optional pre-configured ChaosConfig instance.
            duplicate_rate: Probability of duplicating an event [0.0, 1.0].
            delay_rate: Probability of altering an event timestamp backward [0.0, 1.0].
            max_delay_seconds: Upper bound for randomly generated timestamp delay.
            invalid_payload_rate: Probability of corrupting schema structure [0.0, 1.0].
            burst_probability: Probability of entering a traffic spike [0.0, 1.0].
            burst_multiplier: Throughput multiplier applied during a burst.
            burst_period_seconds: Periodic cycle interval for flash-sale spikes.
            burst_duration_seconds: Duration in seconds of each periodic spike window.
            drop_rate: Probability of dropping an event altogether [0.0, 1.0].
            seed: Optional integer seed for isolated reproducible random sequences.
        """
        if config is not None:
            self.config = ChaosConfig(
                duplicate_rate=duplicate_rate if duplicate_rate is not None else config.duplicate_rate,
                delay_rate=delay_rate if delay_rate is not None else config.delay_rate,
                max_delay_seconds=max_delay_seconds if max_delay_seconds is not None else config.max_delay_seconds,
                invalid_payload_rate=invalid_payload_rate
                if invalid_payload_rate is not None
                else config.invalid_payload_rate,
                burst_probability=burst_probability if burst_probability is not None else config.burst_probability,
                burst_multiplier=burst_multiplier if burst_multiplier is not None else config.burst_multiplier,
                burst_period_seconds=burst_period_seconds
                if burst_period_seconds is not None
                else config.burst_period_seconds,
                burst_duration_seconds=burst_duration_seconds
                if burst_duration_seconds is not None
                else config.burst_duration_seconds,
                drop_rate=drop_rate if drop_rate is not None else config.drop_rate,
                seed=seed if seed is not None else config.seed,
            )
        else:
            self.config = ChaosConfig(
                duplicate_rate=duplicate_rate if duplicate_rate is not None else 0.0,
                delay_rate=delay_rate if delay_rate is not None else 0.0,
                max_delay_seconds=max_delay_seconds if max_delay_seconds is not None else 3600,
                invalid_payload_rate=invalid_payload_rate if invalid_payload_rate is not None else 0.0,
                burst_probability=burst_probability if burst_probability is not None else 0.0,
                burst_multiplier=burst_multiplier if burst_multiplier is not None else 5.0,
                burst_period_seconds=burst_period_seconds if burst_period_seconds is not None else 60.0,
                burst_duration_seconds=burst_duration_seconds if burst_duration_seconds is not None else 10.0,
                drop_rate=drop_rate if drop_rate is not None else 0.0,
                seed=seed,
            )

        self._random = random.Random(self.config.seed)
        self.metrics = ChaosMetrics()

    def inject_delay(self, event: dict[str, Any], delay_seconds: float | None = None) -> dict[str, Any]:
        """Alter event timestamp backward to simulate late-arriving / out-of-order data.

        Args:
            event: Source event payload dictionary.
            delay_seconds: Explicit delay in seconds to subtract. If None, chosen randomly
                in range [1.0, max_delay_seconds].

        Returns:
            New event dictionary with altered timestamp.
        """
        if delay_seconds is None:
            delay_seconds = self._random.uniform(1.0, max(1.0, float(self.config.max_delay_seconds)))

        corrupted = copy.deepcopy(event)
        timestamp_keys = ("timestamp", "event_timestamp", "created_at", "occurred_at")
        target_key: str | None = None

        for key in timestamp_keys:
            if key in corrupted:
                target_key = key
                break

        if target_key is None:
            target_key = "timestamp"
            current_time = datetime.now(UTC)
            delayed_dt = current_time - timedelta(seconds=delay_seconds)
            corrupted[target_key] = delayed_dt
        else:
            val = corrupted[target_key]
            if isinstance(val, datetime):
                corrupted[target_key] = val - timedelta(seconds=delay_seconds)
            elif isinstance(val, (int, float)):
                # Milliseconds Unix epoch format (Avro timestamp-millis)
                corrupted[target_key] = int(val - (delay_seconds * 1000))
            elif isinstance(val, str):
                try:
                    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    corrupted[target_key] = (dt - timedelta(seconds=delay_seconds)).isoformat()
                except ValueError:
                    corrupted[target_key] = datetime.now(UTC) - timedelta(seconds=delay_seconds)
            else:
                corrupted[target_key] = datetime.now(UTC) - timedelta(seconds=delay_seconds)

        self.metrics.delayed_count += 1
        logger.debug(
            "Injected delay of %.2fs into event '%s' field '%s'",
            delay_seconds,
            corrupted.get("event_id", "unknown"),
            target_key,
            extra={"delay_seconds": delay_seconds, "event_id": str(corrupted.get("event_id"))},
        )
        return corrupted

    def inject_duplicate(self, event: dict[str, Any], count: int = 1) -> list[dict[str, Any]]:
        """Duplicate an event with identical event_id and content for deduplication testing.

        Args:
            event: Source event payload dictionary.
            count: Number of duplicate copies to generate (must be >= 1).

        Returns:
            List containing the original event plus requested duplicates (total count + 1 items).

        Raises:
            ValueError: If count is less than 1.
        """
        if count < 1:
            raise ValueError(f"Duplicate count must be at least 1, got: {count}")

        duplicates = [copy.deepcopy(event) for _ in range(count + 1)]
        self.metrics.duplicated_count += count
        logger.debug(
            "Duplicated event '%s' %d time(s) (total: %d)",
            event.get("event_id", "unknown"),
            count,
            len(duplicates),
            extra={"event_id": str(event.get("event_id")), "duplicate_count": count},
        )
        return duplicates

    def inject_schema_violation(
        self,
        event: dict[str, Any],
        violation_type: str | SchemaViolationType = SchemaViolationType.MISSING_REQUIRED,
    ) -> dict[str, Any]:
        """Corrupt payload structure or data types for Dead Letter Queue (DLQ) testing.

        Supported violation types:
        - 'missing_required': Deletes required schema fields (e.g., event_id, order_id).
        - 'invalid_type': Sets numeric/decimal field to an invalid string.
        - 'negative_amount': Sets negative values for amounts or quantities.
        - 'corrupted_structure': Removes nested records (e.g., shipping_address, items).

        Args:
            event: Source event payload dictionary.
            violation_type: Violation category to inject.

        Returns:
            New corrupted event dictionary.

        Raises:
            ValueError: If violation_type is not a recognized SchemaViolationType.
        """
        if isinstance(violation_type, SchemaViolationType):
            v_type = violation_type
        else:
            try:
                v_type = SchemaViolationType(str(violation_type).lower())
            except ValueError as exc:
                supported = [t.value for t in SchemaViolationType]
                raise ValueError(f"Unsupported violation_type '{violation_type}'. Must be one of {supported}") from exc

        corrupted = copy.deepcopy(event)

        if v_type == SchemaViolationType.MISSING_REQUIRED:
            required_candidates = (
                "event_id",
                "order_id",
                "payment_id",
                "product_id",
                "cart_id",
                "session_id",
                "customer_id",
                "items",
            )
            deleted_field: str | None = None
            for field_name in required_candidates:
                if field_name in corrupted:
                    del corrupted[field_name]
                    deleted_field = field_name
                    break

            if deleted_field is None and corrupted:
                deleted_field = next(iter(corrupted.keys()))
                del corrupted[deleted_field]

            logger.info(
                "Injected missing_required violation by removing '%s' from event '%s'",
                deleted_field,
                corrupted.get("event_id", "unknown"),
                extra={"violation": v_type.value, "deleted_field": deleted_field},
            )

        elif v_type == SchemaViolationType.INVALID_TYPE:
            numeric_candidates = (
                "total_amount",
                "subtotal_amount",
                "shipping_fee",
                "discount_amount",
                "amount",
                "price",
                "unit_price",
                "subtotal",
                "stock_delta",
                "available_stock",
                "reserved_stock",
                "quantity",
            )
            corrupted_field: str | None = None
            for field_name in numeric_candidates:
                if field_name in corrupted:
                    corrupted[field_name] = "INVALID_NON_NUMERIC_STRING_VALUE"
                    corrupted_field = field_name
                    break

            if corrupted_field is None:
                for k, v in corrupted.items():
                    if isinstance(v, (int, float, Decimal)):
                        corrupted[k] = "NOT_A_VALID_NUMBER"
                        corrupted_field = k
                        break

            if corrupted_field is None:
                if "timestamp" in corrupted:
                    corrupted["timestamp"] = {"corrupted": "not_a_timestamp"}
                    corrupted_field = "timestamp"
                elif corrupted:
                    first_k = next(iter(corrupted.keys()))
                    corrupted[first_k] = ["invalid", "type"]
                    corrupted_field = first_k

            logger.info(
                "Injected invalid_type violation in field '%s' of event '%s'",
                corrupted_field,
                corrupted.get("event_id", "unknown"),
                extra={"violation": v_type.value, "corrupted_field": corrupted_field},
            )

        elif v_type == SchemaViolationType.NEGATIVE_AMOUNT:
            amount_candidates = (
                "total_amount",
                "subtotal_amount",
                "amount",
                "price",
                "unit_price",
                "subtotal",
                "quantity",
                "available_stock",
                "reserved_stock",
            )
            modified_field: str | None = None
            for field_name in amount_candidates:
                if field_name in corrupted:
                    if field_name == "quantity":
                        corrupted[field_name] = -50
                    elif isinstance(corrupted[field_name], Decimal):
                        corrupted[field_name] = Decimal("-999.9900")
                    elif isinstance(corrupted[field_name], float):
                        corrupted[field_name] = -999.99
                    elif isinstance(corrupted[field_name], int):
                        corrupted[field_name] = -100
                    else:
                        corrupted[field_name] = Decimal("-999.9900")
                    modified_field = field_name
                    break

            if modified_field is None:
                corrupted["total_amount"] = Decimal("-999.9900")
                modified_field = "total_amount"

            logger.info(
                "Injected negative_amount violation in field '%s' of event '%s'",
                modified_field,
                corrupted.get("event_id", "unknown"),
                extra={"violation": v_type.value, "modified_field": modified_field},
            )

        elif v_type == SchemaViolationType.CORRUPTED_STRUCTURE:
            nested_candidates = ("shipping_address", "items", "location", "device_info", "changes")
            removed_nested: str | None = None
            for field_name in nested_candidates:
                if field_name in corrupted:
                    del corrupted[field_name]
                    removed_nested = field_name
                    break

            if removed_nested is None:
                for k, v in corrupted.items():
                    if isinstance(v, (dict, list)):
                        del corrupted[k]
                        removed_nested = k
                        break

            if removed_nested is None:
                corrupted["shipping_address"] = "INVALID_SCALAR_INSTEAD_OF_RECORD"
                removed_nested = "shipping_address"

            logger.info(
                "Injected corrupted_structure violation by removing '%s' from event '%s'",
                removed_nested,
                corrupted.get("event_id", "unknown"),
                extra={"violation": v_type.value, "removed_nested": removed_nested},
            )

        self.metrics.violated_count += 1
        return corrupted

    @staticmethod
    def corrupt_wire_format(payload_bytes: bytes, corruption_type: str = "magic_byte") -> bytes:
        """Corrupt Avro binary wire format to simulate serialization/deserialization failures.

        Args:
            payload_bytes: Confluent wire-format encoded binary bytes.
            corruption_type: Corruption strategy:
                - 'magic_byte': Flips the initial magic byte (b'\\x00' -> b'\\xff').
                - 'truncate': Truncates bytes so header or payload is incomplete.

        Returns:
            Corrupted byte sequence.
        """
        if not payload_bytes:
            return b"\xff\x00\x00\x00\x01"

        if corruption_type == "truncate":
            truncate_len = min(3, max(1, len(payload_bytes) // 2))
            return payload_bytes[:truncate_len]

        # Default: flip magic byte
        altered_byte = b"\xff" if payload_bytes[0:1] == b"\x00" else b"\x00"
        return altered_byte + payload_bytes[1:]

    def calculate_burst_rate(self, base_rate: float, elapsed_seconds: float) -> float:
        """Compute dynamic event rate incorporating periodic traffic spikes or flash sales.

        Args:
            base_rate: Baseline rate in events per second.
            elapsed_seconds: Total elapsed simulation runtime in seconds.

        Returns:
            Adjusted rate in events per second.
        """
        if base_rate <= 0.0:
            return 0.0

        if self.config.burst_multiplier <= 1.0 or self.config.burst_probability <= 0.0:
            return base_rate

        # Periodic flash sale window evaluation
        if self.config.burst_period_seconds > 0.0:
            cycle_time = elapsed_seconds % self.config.burst_period_seconds
            in_burst_window = cycle_time < self.config.burst_duration_seconds
            if in_burst_window and self._random.random() < self.config.burst_probability:
                return base_rate * self.config.burst_multiplier
            return base_rate

        # Continuous stochastic bursts if period is 0
        if self._random.random() < self.config.burst_probability:
            return base_rate * self.config.burst_multiplier

        return base_rate

    def apply_chaos(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Evaluate configured chaos probabilities and return resulting event list.

        Evaluates:
        - drop_rate: returns empty list [] if dropped.
        - delay_rate: alters timestamp backward if triggered.
        - invalid_payload_rate: corrupts schema fields for DLQ testing if triggered.
        - duplicate_rate: returns duplicated events list if triggered.

        Args:
            event: Input domain event payload dictionary.

        Returns:
            List of events:
            - 0 events if dropped ([])
            - 1 event if clean or altered ([event])
            - >1 events if duplicated ([event, copy, ...])
        """
        self.metrics.evaluated_count += 1

        # 1. Drop evaluation
        if self.config.drop_rate > 0.0 and self._random.random() < self.config.drop_rate:
            self.metrics.dropped_count += 1
            logger.info(
                "Event '%s' dropped by chaos engine (drop_rate=%.2f)",
                event.get("event_id", "unknown"),
                self.config.drop_rate,
                extra={"event_id": str(event.get("event_id")), "drop_rate": self.config.drop_rate},
            )
            return []

        # Work on deep copy to avoid mutating original source
        current_event = copy.deepcopy(event)

        # 2. Delay evaluation
        if self.config.delay_rate > 0.0 and self._random.random() < self.config.delay_rate:
            current_event = self.inject_delay(current_event)

        # 3. Invalid payload / DLQ evaluation
        if self.config.invalid_payload_rate > 0.0 and self._random.random() < self.config.invalid_payload_rate:
            chosen_violation = self._random.choice(list(SchemaViolationType))
            current_event = self.inject_schema_violation(current_event, violation_type=chosen_violation)

        # 4. Duplicate evaluation
        if self.config.duplicate_rate > 0.0 and self._random.random() < self.config.duplicate_rate:
            return self.inject_duplicate(current_event, count=1)

        return [current_event]

    @staticmethod
    def simulate_retry_connection(
        produce_func: Callable[..., Any],
        *args: Any,
        max_retries: int = 3,
        backoff_base: float = 0.1,
        retryable_exceptions: tuple[type[Exception], ...] | None = None,
        retry_on_false: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Helper for resilient retry on transient Kafka produce errors.

        Retries with exponential backoff on specified transient errors.

        Args:
            produce_func: Callable executing the Kafka produce or network operation.
            *args: Positional arguments forwarded to produce_func.
            max_retries: Maximum number of retry attempts before raising (default 3).
            backoff_base: Initial backoff duration in seconds (doubles each attempt).
            retryable_exceptions: Tuple of Exception classes considered transient.
                Defaults to (KafkaException, BufferError, TimeoutError, ConnectionError, OSError).
            retry_on_false: If True and produce_func returns False, treat as transient failure.
            **kwargs: Keyword arguments forwarded to produce_func.

        Returns:
            Result returned by produce_func.

        Raises:
            Exception: The last encountered exception if all retry attempts are exhausted,
                or immediately if a non-retryable exception is encountered.
        """
        if retryable_exceptions is None:
            retryable_exceptions = (
                confluent_kafka.KafkaException,
                BufferError,
                TimeoutError,
                ConnectionError,
                OSError,
            )

        last_exception: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                result = produce_func(*args, **kwargs)
                if retry_on_false and result is False:
                    if attempt < max_retries:
                        backoff = backoff_base * (2**attempt)
                        logger.warning(
                            "Produce operation returned False on attempt %d/%d. Retrying in %.3fs...",
                            attempt + 1,
                            max_retries,
                            backoff,
                            extra={"attempt": attempt + 1, "max_retries": max_retries, "backoff": backoff},
                        )
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error(
                            "Produce operation returned False after %d retries.",
                            max_retries,
                            extra={"max_retries": max_retries},
                        )
                        return False
                return result
            except retryable_exceptions as exc:
                last_exception = exc
                if attempt < max_retries:
                    backoff = backoff_base * (2**attempt)
                    logger.warning(
                        "Transient error during produce attempt %d/%d: %s. Retrying in %.3fs...",
                        attempt + 1,
                        max_retries,
                        exc,
                        backoff,
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "backoff": backoff,
                            "error": str(exc),
                        },
                    )
                    time.sleep(backoff)
                else:
                    logger.error(
                        "Produce retries exhausted after %d attempts: %s",
                        max_retries + 1,
                        exc,
                        extra={"max_retries": max_retries, "error": str(exc)},
                    )
                    raise exc

        if last_exception is not None:
            raise last_exception
