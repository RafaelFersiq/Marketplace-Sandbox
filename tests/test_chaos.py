"""Unit tests for chaos and resilience simulation engine."""

import copy
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import confluent_kafka
import pytest

from ingestion.producers.chaos import (
    ChaosConfig,
    ChaosEngine,
    SchemaViolationType,
)
from ingestion.producers.generator import SyntheticDataGenerator
from ingestion.serialization import (
    AvroSerializerDeserializer,
    DeserializationError,
    SchemaValidationError,
)


@pytest.fixture
def generator() -> SyntheticDataGenerator:
    """Fixture providing deterministic synthetic data generator."""
    return SyntheticDataGenerator(seed=42)


@pytest.fixture
def serializer() -> AvroSerializerDeserializer:
    """Fixture providing Avro serializer/deserializer."""
    return AvroSerializerDeserializer()


@pytest.fixture
def order_schema() -> dict[str, Any]:
    """Fixture loading order_creation.avsc schema."""
    schema_path = (
        Path(__file__).resolve().parent.parent
        / "ingestion"
        / "schemas"
        / "avro"
        / "order_creation.avsc"
    )
    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


class TestChaosConfig:
    """Test suite for ChaosConfig parameters and validations."""

    def test_default_configuration(self) -> None:
        config = ChaosConfig()
        assert config.duplicate_rate == 0.0
        assert config.delay_rate == 0.0
        assert config.max_delay_seconds == 3600
        assert config.invalid_payload_rate == 0.0
        assert config.burst_probability == 0.0
        assert config.burst_multiplier == 5.0
        assert config.burst_period_seconds == 60.0
        assert config.burst_duration_seconds == 10.0
        assert config.drop_rate == 0.0
        assert config.seed is None

    def test_custom_configuration(self) -> None:
        config = ChaosConfig(
            duplicate_rate=0.1,
            delay_rate=0.2,
            max_delay_seconds=1800,
            invalid_payload_rate=0.05,
            burst_probability=0.3,
            burst_multiplier=3.0,
            burst_period_seconds=30.0,
            burst_duration_seconds=5.0,
            drop_rate=0.01,
            seed=123,
        )
        assert config.duplicate_rate == 0.1
        assert config.delay_rate == 0.2
        assert config.max_delay_seconds == 1800
        assert config.invalid_payload_rate == 0.05
        assert config.burst_probability == 0.3
        assert config.burst_multiplier == 3.0
        assert config.burst_period_seconds == 30.0
        assert config.burst_duration_seconds == 5.0
        assert config.drop_rate == 0.01
        assert config.seed == 123

    @pytest.mark.parametrize(
        "rate_param",
        ["duplicate_rate", "delay_rate", "invalid_payload_rate", "burst_probability", "drop_rate"],
    )
    def test_invalid_probability_parameters(self, rate_param: str) -> None:
        with pytest.raises(ValueError, match=r"must be a float between 0\.0 and 1\.0"):
            ChaosConfig(**{rate_param: -0.1})

        with pytest.raises(ValueError, match=r"must be a float between 0\.0 and 1\.0"):
            ChaosConfig(**{rate_param: 1.1})

    def test_invalid_timing_parameters(self) -> None:
        with pytest.raises(ValueError, match=r"max_delay_seconds must be >= 0"):
            ChaosConfig(max_delay_seconds=-1)

        with pytest.raises(ValueError, match=r"burst_multiplier must be > 0\.0"):
            ChaosConfig(burst_multiplier=0.0)

        with pytest.raises(ValueError, match=r"burst_period_seconds must be >= 0\.0"):
            ChaosConfig(burst_period_seconds=-5.0)

        with pytest.raises(ValueError, match=r"burst_duration_seconds must be >= 0\.0"):
            ChaosConfig(burst_duration_seconds=-1.0)


class TestInjectDelay:
    """Test suite for timestamp delay and out-of-order data injection."""

    def test_inject_delay_datetime(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()
        original_time = event["timestamp"]
        assert isinstance(original_time, datetime)

        delayed = engine.inject_delay(event, delay_seconds=120.0)
        assert isinstance(delayed["timestamp"], datetime)
        diff_seconds = (original_time - delayed["timestamp"]).total_seconds()
        assert pytest.approx(diff_seconds, rel=1e-2) == 120.0
        assert engine.metrics.delayed_count == 1

    def test_inject_delay_millis(self) -> None:
        engine = ChaosEngine()
        base_millis = 1700000000000
        event = {"event_id": "test-id", "timestamp": base_millis}

        delayed = engine.inject_delay(event, delay_seconds=60.0)
        expected_millis = base_millis - 60000
        assert delayed["timestamp"] == expected_millis

    def test_inject_delay_iso_string(self) -> None:
        engine = ChaosEngine()
        event = {"event_id": "test-id", "timestamp": "2026-10-24T12:00:00+00:00"}

        delayed = engine.inject_delay(event, delay_seconds=300.0)
        assert isinstance(delayed["timestamp"], str)
        dt_delayed = datetime.fromisoformat(delayed["timestamp"])
        expected_dt = datetime.fromisoformat("2026-10-24T12:00:00+00:00") - timedelta(seconds=300.0)
        assert dt_delayed == expected_dt

    def test_inject_delay_fallback_new_timestamp(self) -> None:
        engine = ChaosEngine()
        event = {"event_id": "test-id", "name": "Event Without Timestamp"}

        delayed = engine.inject_delay(event, delay_seconds=50.0)
        assert "timestamp" in delayed
        assert isinstance(delayed["timestamp"], datetime)

    def test_inject_delay_random_bounds(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine(max_delay_seconds=60, seed=42)
        event = generator.generate_order_creation()
        original_time = event["timestamp"]

        delayed = engine.inject_delay(event, delay_seconds=None)
        diff_seconds = (original_time - delayed["timestamp"]).total_seconds()
        assert 1.0 <= diff_seconds <= 60.0

    def test_inject_delay_preserves_input(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()
        original_snapshot = copy.deepcopy(event)

        _ = engine.inject_delay(event, delay_seconds=100.0)
        assert event == original_snapshot


class TestInjectDuplicate:
    """Test suite for event duplication for deduplication pipeline testing."""

    def test_inject_duplicate_default_count(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        duplicates = engine.inject_duplicate(event, count=1)
        assert len(duplicates) == 2
        assert duplicates[0]["event_id"] == event["event_id"]
        assert duplicates[1]["event_id"] == event["event_id"]
        assert duplicates[0] == duplicates[1]
        assert engine.metrics.duplicated_count == 1

    def test_inject_duplicate_custom_count(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        duplicates = engine.inject_duplicate(event, count=4)
        assert len(duplicates) == 5
        assert all(d["event_id"] == event["event_id"] for d in duplicates)
        assert engine.metrics.duplicated_count == 4

    def test_inject_duplicate_invalid_count(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        with pytest.raises(ValueError, match="Duplicate count must be at least 1"):
            engine.inject_duplicate(event, count=0)

        with pytest.raises(ValueError, match="Duplicate count must be at least 1"):
            engine.inject_duplicate(event, count=-2)

    def test_inject_duplicate_preserves_input(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()
        original_snapshot = copy.deepcopy(event)

        duplicates = engine.inject_duplicate(event, count=2)
        duplicates[0]["order_id"] = "MUTATED"
        assert event == original_snapshot
        assert duplicates[1]["order_id"] == original_snapshot["order_id"]


class TestInjectSchemaViolation:
    """Test suite for schema violations and Dead Letter Queue (DLQ) injection."""

    def test_violation_missing_required(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()
        assert serializer.validate(event, order_schema) is True

        corrupted = engine.inject_schema_violation(event, SchemaViolationType.MISSING_REQUIRED)
        assert "event_id" not in corrupted

        with pytest.raises(SchemaValidationError):
            serializer.validate(corrupted, order_schema)
        assert engine.metrics.violated_count == 1

    def test_violation_invalid_type(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        corrupted = engine.inject_schema_violation(event, SchemaViolationType.INVALID_TYPE)
        assert isinstance(corrupted["total_amount"], str)
        assert corrupted["total_amount"] == "INVALID_NON_NUMERIC_STRING_VALUE"

        with pytest.raises(SchemaValidationError):
            serializer.validate(corrupted, order_schema)

    def test_violation_negative_amount(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        corrupted = engine.inject_schema_violation(event, SchemaViolationType.NEGATIVE_AMOUNT)
        assert corrupted["total_amount"] < Decimal("0.0")

    def test_violation_corrupted_structure(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        corrupted = engine.inject_schema_violation(event, SchemaViolationType.CORRUPTED_STRUCTURE)
        assert "shipping_address" not in corrupted

        with pytest.raises(SchemaValidationError):
            serializer.validate(corrupted, order_schema)

    def test_violation_string_and_enum_types(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        c1 = engine.inject_schema_violation(event, "missing_required")
        assert "event_id" not in c1

        c2 = engine.inject_schema_violation(event, "MISSING_REQUIRED")
        assert "event_id" not in c2

    def test_violation_unsupported_type_raises(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        with pytest.raises(ValueError, match="Unsupported violation_type 'unsupported_mode'"):
            engine.inject_schema_violation(event, "unsupported_mode")


class TestCorruptWireFormat:
    """Test suite for binary wire format header and payload corruption."""

    def test_corrupt_wire_format_magic_byte(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        event = generator.generate_order_creation()
        wire_bytes = serializer.serialize(event, order_schema, schema_id=42, include_wire_header=True)
        assert wire_bytes.startswith(b"\x00")

        corrupted = ChaosEngine.corrupt_wire_format(wire_bytes, "magic_byte")
        assert corrupted.startswith(b"\xff")

        with pytest.raises(DeserializationError, match="Invalid magic byte"):
            serializer.deserialize(corrupted, reader_schema=order_schema, has_wire_header=True)

    def test_corrupt_wire_format_truncate(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        event = generator.generate_order_creation()
        wire_bytes = serializer.serialize(event, order_schema, schema_id=42, include_wire_header=True)

        truncated = ChaosEngine.corrupt_wire_format(wire_bytes, "truncate")
        assert len(truncated) < 5

        with pytest.raises(DeserializationError, match=r"shorter than header size"):
            serializer.deserialize(truncated, reader_schema=order_schema, has_wire_header=True)

    def test_corrupt_wire_format_empty_bytes(self) -> None:
        corrupted = ChaosEngine.corrupt_wire_format(b"")
        assert len(corrupted) == 5
        assert corrupted.startswith(b"\xff")


class TestCalculateBurstRate:
    """Test suite for dynamic burst rate and periodic flash sale spikes."""

    def test_burst_rate_zero_and_negative_base(self) -> None:
        engine = ChaosEngine(burst_probability=1.0, burst_multiplier=5.0)
        assert engine.calculate_burst_rate(0.0, elapsed_seconds=5.0) == 0.0
        assert engine.calculate_burst_rate(-10.0, elapsed_seconds=5.0) == 0.0

    def test_burst_rate_disabled_by_default(self) -> None:
        engine = ChaosEngine()
        assert engine.calculate_burst_rate(10.0, elapsed_seconds=5.0) == 10.0

    def test_burst_rate_multiplier_one(self) -> None:
        engine = ChaosEngine(burst_probability=1.0, burst_multiplier=1.0)
        assert engine.calculate_burst_rate(15.0, elapsed_seconds=2.0) == 15.0

    def test_burst_rate_periodic_flash_sale(self) -> None:
        engine = ChaosEngine(
            burst_probability=1.0,
            burst_multiplier=4.0,
            burst_period_seconds=60.0,
            burst_duration_seconds=10.0,
        )
        base = 20.0
        # Inside first window [0, 10)
        assert engine.calculate_burst_rate(base, elapsed_seconds=0.0) == 80.0
        assert engine.calculate_burst_rate(base, elapsed_seconds=5.0) == 80.0
        assert engine.calculate_burst_rate(base, elapsed_seconds=9.9) == 80.0

        # Outside window [10, 60)
        assert engine.calculate_burst_rate(base, elapsed_seconds=10.0) == 20.0
        assert engine.calculate_burst_rate(base, elapsed_seconds=30.0) == 20.0
        assert engine.calculate_burst_rate(base, elapsed_seconds=59.9) == 20.0

        # Inside second window [60, 70)
        assert engine.calculate_burst_rate(base, elapsed_seconds=62.0) == 80.0
        assert engine.calculate_burst_rate(base, elapsed_seconds=75.0) == 20.0

    def test_burst_rate_continuous_stochastic(self) -> None:
        engine_burst = ChaosEngine(
            burst_probability=1.0,
            burst_multiplier=3.0,
            burst_period_seconds=0.0,
        )
        assert engine_burst.calculate_burst_rate(10.0, elapsed_seconds=123.0) == 30.0

        engine_no_burst = ChaosEngine(
            burst_probability=0.0,
            burst_multiplier=3.0,
            burst_period_seconds=0.0,
        )
        assert engine_no_burst.calculate_burst_rate(10.0, elapsed_seconds=123.0) == 10.0


class TestApplyChaos:
    """Test suite for probabilistic pipeline chaos evaluation."""

    def test_apply_chaos_clean_passthrough(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine()
        event = generator.generate_order_creation()

        events = engine.apply_chaos(event)
        assert len(events) == 1
        assert events[0] == event
        assert engine.metrics.evaluated_count == 1
        assert engine.metrics.dropped_count == 0

    def test_apply_chaos_drop(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine(drop_rate=1.0)
        event = generator.generate_order_creation()

        events = engine.apply_chaos(event)
        assert len(events) == 0
        assert engine.metrics.dropped_count == 1
        assert engine.metrics.evaluated_count == 1

    def test_apply_chaos_delay(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine(delay_rate=1.0, max_delay_seconds=100, seed=42)
        event = generator.generate_order_creation()
        original_ts = event["timestamp"]

        events = engine.apply_chaos(event)
        assert len(events) == 1
        assert events[0]["timestamp"] < original_ts
        assert engine.metrics.delayed_count == 1

    def test_apply_chaos_invalid_payload(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine(invalid_payload_rate=1.0, seed=42)
        event = generator.generate_order_creation()

        events = engine.apply_chaos(event)
        assert len(events) == 1
        assert engine.metrics.violated_count == 1

    def test_apply_chaos_duplicate(self, generator: SyntheticDataGenerator) -> None:
        engine = ChaosEngine(duplicate_rate=1.0)
        event = generator.generate_order_creation()

        events = engine.apply_chaos(event)
        assert len(events) == 2
        assert events[0]["event_id"] == events[1]["event_id"]
        assert engine.metrics.duplicated_count == 1

    def test_apply_chaos_seed_reproducibility(self, generator: SyntheticDataGenerator) -> None:
        config = ChaosConfig(
            duplicate_rate=0.3,
            delay_rate=0.3,
            invalid_payload_rate=0.3,
            seed=999,
        )
        engine1 = ChaosEngine(config=config)
        engine2 = ChaosEngine(config=config)

        event = generator.generate_order_creation()
        results1 = [len(engine1.apply_chaos(copy.deepcopy(event))) for _ in range(20)]
        results2 = [len(engine2.apply_chaos(copy.deepcopy(event))) for _ in range(20)]
        assert results1 == results2


class TestSimulateRetryConnection:
    """Test suite for transient Kafka error retries and exponential backoff."""

    def test_retry_success_first_attempt(self) -> None:
        mock_fn = MagicMock(return_value="SUCCESS")
        result = ChaosEngine.simulate_retry_connection(mock_fn, "arg1", key="val")

        assert result == "SUCCESS"
        mock_fn.assert_called_once_with("arg1", key="val")

    def test_retry_transient_failure_then_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sleep = MagicMock()
        import time
        monkeypatch.setattr(time, "sleep", mock_sleep)

        mock_fn = MagicMock(
            side_effect=[
                confluent_kafka.KafkaException("Broker temporarily unavailable"),
                confluent_kafka.KafkaException("Queue full"),
                "DELIVERED",
            ]
        )

        result = ChaosEngine.simulate_retry_connection(mock_fn, max_retries=3, backoff_base=0.1)
        assert result == "DELIVERED"
        assert mock_fn.call_count == 3
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 0.1
        assert mock_sleep.call_args_list[1][0][0] == 0.2

    def test_retry_exhausted_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sleep = MagicMock()
        import time
        monkeypatch.setattr(time, "sleep", mock_sleep)

        mock_fn = MagicMock(side_effect=confluent_kafka.KafkaException("Fatal broker connection down"))

        with pytest.raises(confluent_kafka.KafkaException, match="Fatal broker connection down"):
            ChaosEngine.simulate_retry_connection(mock_fn, max_retries=2, backoff_base=0.05)

        assert mock_fn.call_count == 3
        assert mock_sleep.call_count == 2

    def test_retry_non_retryable_exception_raises_immediately(self) -> None:
        mock_fn = MagicMock(side_effect=ValueError("Non-retryable programmer error"))

        with pytest.raises(ValueError, match="Non-retryable programmer error"):
            ChaosEngine.simulate_retry_connection(mock_fn, max_retries=5)

        assert mock_fn.call_count == 1

    def test_retry_buffer_error_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sleep = MagicMock()
        import time
        monkeypatch.setattr(time, "sleep", mock_sleep)

        mock_fn = MagicMock(side_effect=[BufferError("Kafka buffer full"), "RECOVERED"])
        result = ChaosEngine.simulate_retry_connection(mock_fn, max_retries=2, backoff_base=0.01)

        assert result == "RECOVERED"
        assert mock_fn.call_count == 2
        assert mock_sleep.call_count == 1

    def test_retry_custom_retryable_exceptions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sleep = MagicMock()
        import time
        monkeypatch.setattr(time, "sleep", mock_sleep)

        mock_fn = MagicMock(side_effect=[KeyError("Custom transient key error"), "DONE"])
        result = ChaosEngine.simulate_retry_connection(
            mock_fn,
            max_retries=2,
            backoff_base=0.05,
            retryable_exceptions=(KeyError,),
        )
        assert result == "DONE"
        assert mock_fn.call_count == 2

    def test_retry_on_false_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_sleep = MagicMock()
        import time
        monkeypatch.setattr(time, "sleep", mock_sleep)

        mock_fn = MagicMock(side_effect=[False, False, True])
        result = ChaosEngine.simulate_retry_connection(mock_fn, max_retries=3, retry_on_false=True)
        assert result is True
        assert mock_fn.call_count == 3

        mock_failed_fn = MagicMock(return_value=False)
        exhausted_result = ChaosEngine.simulate_retry_connection(
            mock_failed_fn, max_retries=2, retry_on_false=True
        )
        assert exhausted_result is False
        assert mock_failed_fn.call_count == 3
