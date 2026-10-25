"""Event-time watermarking, stateful deduplication, and in-memory replay verification.

Provides utilities to:
- Convert Avro epoch milliseconds (timestamp-millis) into PySpark TimestampType.
- Derive ingestion partitioning attributes (ingestion_date, ingested_at).
- Configure PySpark Structured Streaming event-time watermarking (.withWatermark) for out-of-order events.
- Apply stateful deduplication on [event_id, event_time] to safely evict state beyond the watermark boundary.
- Emulate watermark state tracking and late-arrival dropping in pure Python (InMemoryWatermarkDeduplicator)
  for deterministic unit testing without requiring an active JVM.
"""

import logging
import re
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812

logger = logging.getLogger("processing.streaming.watermark")

DURATION_PATTERN = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$")

UNIT_MULTIPLIERS_MS: dict[str, int] = {
    # Milliseconds
    "ms": 1,
    "milli": 1,
    "millis": 1,
    "millisecond": 1,
    "milliseconds": 1,
    # Seconds
    "s": 1_000,
    "sec": 1_000,
    "secs": 1_000,
    "second": 1_000,
    "seconds": 1_000,
    # Minutes
    "m": 60_000,
    "min": 60_000,
    "mins": 60_000,
    "minute": 60_000,
    "minutes": 60_000,
    # Hours
    "h": 3_600_000,
    "hr": 3_600_000,
    "hrs": 3_600_000,
    "hour": 3_600_000,
    "hours": 3_600_000,
    # Days
    "d": 86_400_000,
    "day": 86_400_000,
    "days": 86_400_000,
}


def parse_watermark_duration_ms(watermark_delay: str) -> int:
    """Parse a human-readable duration expression into equivalent milliseconds.

    Supports expressions like '10 minutes', '30 seconds', '1 hour', '2 hours', '500 ms'.

    Args:
        watermark_delay: Duration string expression.

    Returns:
        Duration represented as an integer of milliseconds.

    Raises:
        ValueError: If watermark_delay format or time unit is invalid or non-positive.
    """
    if not isinstance(watermark_delay, str) or not watermark_delay.strip():
        raise ValueError(f"Watermark delay must be a non-empty string, got: {watermark_delay!r}")

    match = DURATION_PATTERN.match(watermark_delay.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid watermark duration format: '{watermark_delay}'. Expected format: '<number> <unit>' "
            "(e.g., '10 minutes', '30 seconds', '1 hour')."
        )

    val_str, unit = match.groups()
    val = float(val_str)
    if val <= 0:
        raise ValueError(f"Watermark duration must be strictly positive, got: {val}")

    if unit not in UNIT_MULTIPLIERS_MS:
        valid_units = ", ".join(sorted(set(UNIT_MULTIPLIERS_MS.keys())))
        raise ValueError(f"Unsupported duration unit '{unit}'. Supported units: {valid_units}")

    return int(val * UNIT_MULTIPLIERS_MS[unit])


def ms_to_utc_datetime(epoch_ms: int) -> datetime:
    """Convert Avro timestamp-millis epoch integer to timezone-aware UTC datetime.

    Args:
        epoch_ms: Epoch milliseconds integer.

    Returns:
        Timezone-aware UTC datetime object.

    Raises:
        ValueError: If epoch_ms cannot be converted to a valid datetime.
    """
    if not isinstance(epoch_ms, (int, float)):
        raise ValueError(f"epoch_ms must be an integer or float, got {type(epoch_ms).__name__}")
    try:
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise ValueError(f"Failed to convert epoch milliseconds ({epoch_ms}) to UTC datetime: {exc}") from exc


def apply_event_time_and_watermark(
    valid_df: DataFrame,
    watermark_delay: str = "10 minutes",
    timestamp_ms_col: str = "event_timestamp_ms",
    event_time_col: str = "event_time",
) -> DataFrame:
    """Derive event_time timestamp, partition dates, and apply Structured Streaming watermark.

    Args:
        valid_df: DataFrame containing valid events with epoch milliseconds timestamp column.
        watermark_delay: Spark watermark delay string (e.g., '10 minutes', '1 hour').
        timestamp_ms_col: Name of source column containing epoch milliseconds.
        event_time_col: Target column name for converted timestamp.

    Returns:
        DataFrame enriched with event_time, ingestion_date, ingested_at, and watermark boundary.
    """
    with_event_time = valid_df.withColumn(
        event_time_col,
        (F.col(timestamp_ms_col) / F.lit(1000.0)).cast("timestamp"),
    )

    with_dates = with_event_time.withColumn("ingestion_date", F.to_date(F.col(event_time_col))).withColumn(
        "ingested_at", F.current_timestamp()
    )

    return with_dates.withWatermark(event_time_col, watermark_delay)


def apply_stateful_deduplication(
    watermarked_df: DataFrame,
    dedup_columns: list[str] | None = None,
) -> DataFrame:
    """Apply stateful deduplication over event keys within the watermark window.

    In PySpark Structured Streaming, dropDuplicates with event-time watermark must include
    the watermarked timestamp column (event_time) to enable state eviction.

    Args:
        watermarked_df: Watermarked DataFrame.
        dedup_columns: Optional list of column names for deduplication. Defaults to
            ['event_id', 'event_time'] (or ['event_id'] if event_time is absent).

    Returns:
        Deduplicated DataFrame with state eviction bounds.
    """
    if dedup_columns is None:
        dedup_columns = ["event_id", "event_time"] if "event_time" in watermarked_df.columns else ["event_id"]

    return watermarked_df.dropDuplicates(dedup_columns)


class InMemoryWatermarkDeduplicator:
    """Pure-Python watermark state tracker and deduplicator for local testing and simulation.

    Simulates PySpark Structured Streaming's event-time watermarking and stateful deduplication:
    - Tracks max observed event timestamp.
    - Computes watermark = max_event_timestamp - watermark_delay.
    - Drops late events arriving with timestamp < watermark.
    - Drops duplicate events matching dedup_key within the active window.
    - Purges expired keys from state once their timestamp falls behind the current watermark.
    """

    def __init__(
        self,
        watermark_delay: str | int = "10 minutes",
        dedup_key: str = "event_id",
        timestamp_key: str = "event_timestamp_ms",
    ) -> None:
        """Initialize InMemoryWatermarkDeduplicator.

        Args:
            watermark_delay: Delay string (e.g., '10 minutes') or integer milliseconds.
            dedup_key: Event dictionary field name used as unique identifier.
            timestamp_key: Event dictionary field name containing epoch milliseconds.
        """
        if isinstance(watermark_delay, str):
            self.watermark_delay_ms: int = parse_watermark_duration_ms(watermark_delay)
        else:
            self.watermark_delay_ms = int(watermark_delay)

        self.dedup_key = dedup_key
        self.timestamp_key = timestamp_key
        self.max_event_timestamp_ms: int = 0
        self._seen_state: dict[str, int] = {}

    @property
    def current_watermark_ms(self) -> int:
        """Calculate current watermark threshold in epoch milliseconds.

        Returns:
            Current watermark boundary (0 if no events processed yet).
        """
        if self.max_event_timestamp_ms == 0:
            return 0
        return max(0, self.max_event_timestamp_ms - self.watermark_delay_ms)

    @property
    def state_size(self) -> int:
        """Return the number of unexpired event keys currently tracked in state.

        Returns:
            Current size of deduplication state cache.
        """
        return len(self._seen_state)

    def process_event(
        self,
        event: dict[str, Any],
        timestamp_key: str | None = None,
    ) -> tuple[bool, str | None]:
        """Process an individual event through watermark evaluation and deduplication.

        Args:
            event: Event payload dictionary.
            timestamp_key: Optional override for the event timestamp field name.

        Returns:
            Tuple of (is_accepted: bool, drop_reason: str | None).
        """
        key_field = self.dedup_key
        ts_field = timestamp_key or self.timestamp_key

        event_id = event.get(key_field)
        if event_id is None or event_id == "":
            return False, "MISSING_DEDUP_KEY"

        event_id_str = str(event_id)
        raw_ts = event.get(ts_field)
        if raw_ts is None:
            return False, "MISSING_TIMESTAMP"

        try:
            if isinstance(raw_ts, datetime):
                dt = raw_ts if raw_ts.tzinfo is not None else raw_ts.replace(tzinfo=UTC)
                event_ts_ms = int(dt.timestamp() * 1000)
            elif isinstance(raw_ts, (int, float)):
                event_ts_ms = int(raw_ts)
            elif isinstance(raw_ts, str):
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
                event_ts_ms = int(dt.timestamp() * 1000)
            else:
                return False, "INVALID_TIMESTAMP"
        except Exception:
            return False, "INVALID_TIMESTAMP"

        if event_ts_ms > self.max_event_timestamp_ms:
            self.max_event_timestamp_ms = event_ts_ms

        active_watermark = self.current_watermark_ms

        if event_ts_ms < active_watermark:
            return False, "LATE_EVENT_DROPPED_BY_WATERMARK"

        if event_id_str in self._seen_state:
            return False, "DUPLICATE_EVENT"

        self._seen_state[event_id_str] = event_ts_ms
        self._evict_expired_state(active_watermark)

        return True, None

    def _evict_expired_state(self, watermark_boundary_ms: int) -> int:
        """Evict tracked keys with timestamps older than the active watermark boundary."""
        expired_keys = [k for k, key_ts in self._seen_state.items() if key_ts < watermark_boundary_ms]
        for k in expired_keys:
            del self._seen_state[k]
        return len(expired_keys)

    def process_batch(
        self,
        events: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Process a sequence of events, partitioning into accepted events and dropped events."""
        accepted: list[dict[str, Any]] = []
        dropped: list[dict[str, Any]] = []

        for evt in events:
            is_valid, reason = self.process_event(evt)
            if is_valid:
                accepted.append(evt)
            else:
                dropped_copy = dict(evt)
                dropped_copy["_drop_reason"] = reason
                dropped.append(dropped_copy)

        return accepted, dropped

    def clear(self) -> None:
        """Reset watermark high-water mark and state cache."""
        self.max_event_timestamp_ms = 0
        self._seen_state.clear()
        logger.debug("InMemoryWatermarkDeduplicator state cleared.")
