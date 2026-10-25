"""Unit tests for Dead Letter Queue (DLQ) routing, bifurcation, and formatting.

Validates DLQRecord serialization, base64 payload normalization across data types,
UTC failure timestamp generation, and mock DataFrame/writeStream integration for
both continuous and Trigger.AvailableNow (once) execution modes.
"""

import base64
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from processing.streaming.config import SparkStreamingConfig
from processing.streaming.dlq import DLQRecord, DLQRouter


class TestDLQRecord:
    """Test suite for DLQRecord dataclass and serialization routines."""

    def test_to_dict_and_to_json(self) -> None:
        record = DLQRecord(
            _error_type="CORRUPT_FRAME",
            _error_message="Invalid magic byte 0x01",
            _raw_payload_base64="AQAAAAFzb21lcGF5bG9hZA==",
            _kafka_topic="marketplace.orders",
            _kafka_partition=2,
            _kafka_offset=150,
            _failed_at="2026-07-26T12:00:00.000Z",
            schema_id=42,
        )

        d = record.to_dict()
        assert d["_error_type"] == "CORRUPT_FRAME"
        assert d["_error_message"] == "Invalid magic byte 0x01"
        assert d["_raw_payload_base64"] == "AQAAAAFzb21lcGF5bG9hZA=="
        assert d["_kafka_topic"] == "marketplace.orders"
        assert d["_kafka_partition"] == 2
        assert d["_kafka_offset"] == 150
        assert d["_failed_at"] == "2026-07-26T12:00:00.000Z"
        assert d["schema_id"] == 42

        json_str = record.to_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed == d


class TestDLQRouter:
    """Test suite for DLQRouter payload formatting, bifurcation, and streaming writers."""

    def test_create_dlq_record_payload_types(self) -> None:
        raw_b = b"\x00\x01\x02\x03\x04"
        rec_b = DLQRouter.create_dlq_record(
            raw_payload=raw_b,
            error_type="RAW_BYTES_ERR",
            error_message="Error bytes",
            kafka_topic="marketplace.orders",
            kafka_partition=0,
            kafka_offset=10,
        )
        assert rec_b._raw_payload_base64 == base64.b64encode(raw_b).decode("ascii")

        valid_b64 = base64.b64encode(b"pre-encoded content").decode("ascii")
        rec_b64 = DLQRouter.create_dlq_record(
            raw_payload=valid_b64,
            error_type="B64_ERR",
            error_message="Error b64",
            kafka_topic="marketplace.payments",
            kafka_partition=1,
            kafka_offset=20,
        )
        assert rec_b64._raw_payload_base64 == valid_b64

        plain_str = "Plain unencoded string with special chars !@#$"
        rec_str = DLQRouter.create_dlq_record(
            raw_payload=plain_str,
            error_type="PLAIN_ERR",
            error_message="Error str",
            kafka_topic="marketplace.cart",
            kafka_partition=2,
            kafka_offset=30,
        )
        expected_b64 = base64.b64encode(plain_str.encode("utf-8")).decode("ascii")
        assert rec_str._raw_payload_base64 == expected_b64

        rec_none = DLQRouter.create_dlq_record(
            raw_payload=None,
            error_type="NONE_ERR",
            error_message="Error None",
            kafka_topic="marketplace.inventory",
            kafka_partition=3,
            kafka_offset=40,
        )
        assert rec_none._raw_payload_base64 == ""

    def test_create_dlq_record_failed_at_variations(self) -> None:
        rec_default = DLQRouter.create_dlq_record(
            raw_payload=b"test",
            error_type="ERR",
            error_message="Msg",
            kafka_topic="topic",
            kafka_partition=0,
            kafka_offset=0,
        )
        assert rec_default._failed_at.endswith("Z")
        datetime.fromisoformat(rec_default._failed_at.replace("Z", "+00:00"))

        dt_utc = datetime(2026, 7, 26, 15, 30, 45, 123000, tzinfo=UTC)
        rec_dt = DLQRouter.create_dlq_record(
            raw_payload=b"test",
            error_type="ERR",
            error_message="Msg",
            kafka_topic="topic",
            kafka_partition=0,
            kafka_offset=0,
            failed_at=dt_utc,
        )
        assert rec_dt._failed_at == "2026-07-26T15:30:45.123Z"

        dt_naive = datetime(2026, 7, 26, 15, 30, 45, 123000)
        rec_naive = DLQRouter.create_dlq_record(
            raw_payload=b"test",
            error_type="ERR",
            error_message="Msg",
            kafka_topic="topic",
            kafka_partition=0,
            kafka_offset=0,
            failed_at=dt_naive,
        )
        assert rec_naive._failed_at == "2026-07-26T15:30:45.123Z"

        rec_str = DLQRouter.create_dlq_record(
            raw_payload=b"test",
            error_type="ERR",
            error_message="Msg",
            kafka_topic="topic",
            kafka_partition=0,
            kafka_offset=0,
            failed_at="2026-07-26T15:30:45Z",
        )
        assert rec_str._failed_at == "2026-07-26T15:30:45Z"

    def test_bifurcate_stream_mock(self) -> None:
        mock_df = MagicMock(spec=DataFrame)
        mock_df.columns = [
            "value",
            "is_valid",
            "topic",
            "partition",
            "offset",
            "_error_type",
            "_error_message",
            "_raw_payload_base64",
        ]

        with patch("processing.streaming.dlq.F") as mock_f:
            valid_df, dlq_df = DLQRouter.bifurcate_stream(mock_df)

            assert mock_df.filter.call_count == 2
            assert valid_df is not None
            assert dlq_df is not None
            assert mock_f.col.called

    def test_format_for_kafka_dlq_mock(self) -> None:
        mock_dlq_df = MagicMock(spec=DataFrame)

        with patch("processing.streaming.dlq.F") as mock_f:
            result_df = DLQRouter.format_for_kafka_dlq(mock_dlq_df)

            assert mock_dlq_df.select.called
            assert result_df == mock_dlq_df.select.return_value
            assert mock_f.to_json.called
            assert mock_f.struct.called

    def test_write_dlq_to_kafka_continuous(self) -> None:
        config = SparkStreamingConfig(
            bootstrap_servers="localhost:9092",
            dlq_topic="marketplace.dlq",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints/ingestion",
            trigger_interval="15 seconds",
            once=False,
        )

        mock_dlq_df = MagicMock(spec=DataFrame)
        mock_formatted_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_formatted_df.writeStream.queryName.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.start.return_value = MagicMock()

        with patch.object(DLQRouter, "format_for_kafka_dlq", return_value=mock_formatted_df):
            query = DLQRouter.write_dlq_to_kafka(mock_dlq_df, config)

            mock_formatted_df.writeStream.queryName.assert_called_once_with("dlq_kafka_sink")
            mock_writer.format.assert_called_once_with("kafka")
            mock_writer.outputMode.assert_called_once_with("append")
            mock_writer.trigger.assert_called_once_with(processingTime="15 seconds")
            assert mock_writer.start.called
            assert query == mock_writer.start.return_value

            option_calls = {call[0][0]: call[0][1] for call in mock_writer.option.call_args_list}
            assert option_calls["kafka.bootstrap.servers"] == "localhost:9092"
            assert option_calls["topic"] == "marketplace.dlq"
            assert option_calls["checkpointLocation"] == config.get_dlq_kafka_checkpoint_path()

    def test_write_dlq_to_kafka_once(self) -> None:
        config = SparkStreamingConfig(
            bootstrap_servers="localhost:9092",
            dlq_topic="marketplace.dlq",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints/ingestion",
            once=True,
        )

        mock_dlq_df = MagicMock(spec=DataFrame)
        mock_formatted_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_formatted_df.writeStream.queryName.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.start.return_value = MagicMock()

        with patch.object(DLQRouter, "format_for_kafka_dlq", return_value=mock_formatted_df):
            DLQRouter.write_dlq_to_kafka(mock_dlq_df, config)
            mock_writer.trigger.assert_called_once_with(availableNow=True)

    def test_write_dlq_to_storage_continuous(self) -> None:
        config = SparkStreamingConfig(
            dlq_storage_path="s3a://lakehouse-dlq/events",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints/ingestion",
            trigger_interval="10 seconds",
            once=False,
        )

        mock_dlq_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_dlq_df.writeStream.queryName.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.start.return_value = MagicMock()

        query = DLQRouter.write_dlq_to_storage(mock_dlq_df, config)

        mock_dlq_df.writeStream.queryName.assert_called_once_with("dlq_storage_sink")
        mock_writer.format.assert_called_once_with("json")
        mock_writer.outputMode.assert_called_once_with("append")
        mock_writer.trigger.assert_called_once_with(processingTime="10 seconds")
        assert mock_writer.start.called
        assert query == mock_writer.start.return_value

        option_calls = {call[0][0]: call[0][1] for call in mock_writer.option.call_args_list}
        assert option_calls["path"] == "s3a://lakehouse-dlq/events"
        assert option_calls["checkpointLocation"] == config.get_dlq_storage_checkpoint_path()

    def test_write_dlq_to_storage_once(self) -> None:
        config = SparkStreamingConfig(
            dlq_storage_path="s3a://lakehouse-dlq/events",
            checkpoint_dir="s3a://lakehouse-bronze/checkpoints/ingestion",
            once=True,
        )

        mock_dlq_df = MagicMock(spec=DataFrame)
        mock_writer = MagicMock()
        mock_dlq_df.writeStream.queryName.return_value = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.trigger.return_value = mock_writer
        mock_writer.start.return_value = MagicMock()

        DLQRouter.write_dlq_to_storage(mock_dlq_df, config)
        mock_writer.trigger.assert_called_once_with(availableNow=True)
