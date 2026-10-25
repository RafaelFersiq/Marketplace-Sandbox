"""Unit tests for CLI streaming runner, preflight checks, and log formatting."""

import json
import logging
from unittest.mock import MagicMock, patch

from processing.streaming.config import SparkStreamingConfig
from scripts.run_streaming_ingestion import (
    JsonLogFormatter,
    StreamingIngestionRunner,
    build_runner_arg_parser,
    check_kafka_connectivity,
    check_minio_connectivity,
    check_schema_registry_connectivity,
    configure_logging,
    main,
    run_preflight_checks,
)


class TestJsonLogFormatter:
    """Test suite for JsonLogFormatter single-line JSON formatting."""

    def test_json_formatter_keys_and_extras(self) -> None:
        formatter = JsonLogFormatter()
        record = logging.LogRecord(
            name="test.runner",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="Streaming query started: %s",
            args=("landing_query",),
            exc_info=None,
        )
        record.__dict__["extra_key"] = "extra_value"

        output = formatter.format(record)
        parsed = json.loads(output)

        assert "timestamp" in parsed
        assert parsed["timestamp"].endswith("Z")
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.runner"
        assert parsed["message"] == "Streaming query started: landing_query"
        assert parsed["extra_key"] == "extra_value"

    def test_configure_logging(self) -> None:
        configure_logging(level=logging.DEBUG)
        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert any(isinstance(h.formatter, JsonLogFormatter) for h in root.handlers)


class TestPreflightChecks:
    """Test suite for preflight network connectivity checks."""

    @patch("socket.create_connection")
    def test_check_kafka_connectivity_success(self, mock_socket: MagicMock) -> None:
        mock_socket.return_value.__enter__.return_value = MagicMock()
        assert check_kafka_connectivity("localhost:9092,broker2:9092") is True
        mock_socket.assert_called_once_with(("localhost", 9092), timeout=3.0)

    @patch("socket.create_connection", side_effect=OSError("Connection refused"))
    def test_check_kafka_connectivity_failure(self, _mock_socket: MagicMock) -> None:
        assert check_kafka_connectivity("localhost:9092") is False

    def test_check_kafka_connectivity_empty(self) -> None:
        assert check_kafka_connectivity("") is False

    @patch("urllib.request.urlopen")
    def test_check_schema_registry_connectivity_success(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        assert check_schema_registry_connectivity("http://localhost:8081") is True

    @patch("urllib.request.urlopen", side_effect=OSError("Connection refused"))
    def test_check_schema_registry_connectivity_failure(self, _mock_urlopen: MagicMock) -> None:
        assert check_schema_registry_connectivity("http://localhost:8081") is False

    @patch("urllib.request.urlopen")
    def test_check_minio_connectivity_success(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        assert check_minio_connectivity("http://localhost:9000") is True

    @patch("urllib.request.urlopen", side_effect=OSError("Failed"))
    @patch("socket.create_connection")
    def test_check_minio_connectivity_socket_fallback(
        self,
        mock_socket: MagicMock,
        _mock_urlopen: MagicMock,
    ) -> None:
        mock_socket.return_value.__enter__.return_value = MagicMock()
        assert check_minio_connectivity("s3a://localhost:9000") is True

    @patch("scripts.run_streaming_ingestion.check_kafka_connectivity", return_value=True)
    @patch("scripts.run_streaming_ingestion.check_schema_registry_connectivity", return_value=True)
    @patch("scripts.run_streaming_ingestion.check_minio_connectivity", return_value=True)
    def test_run_preflight_checks(
        self,
        _mock_minio: MagicMock,
        _mock_sr: MagicMock,
        _mock_kafka: MagicMock,
    ) -> None:
        config = SparkStreamingConfig()
        results = run_preflight_checks(config)
        assert results == {"kafka": True, "schema_registry": True, "minio": True}


class TestStreamingIngestionRunner:
    """Test suite for StreamingIngestionRunner execution lifecycle."""

    def test_build_runner_arg_parser_arguments(self) -> None:
        parser = build_runner_arg_parser()
        args = parser.parse_args(
            [
                "--skip-preflight",
                "--disable-dlq-kafka",
                "--disable-dlq-storage",
                "--max-retries",
                "10",
                "--log-level",
                "DEBUG",
                "--once",
            ]
        )
        assert args.skip_preflight is True
        assert args.disable_dlq_kafka is True
        assert args.disable_dlq_storage is True
        assert args.max_retries == 10
        assert args.log_level == "DEBUG"
        assert args.once is True

    @patch("scripts.run_streaming_ingestion.run_preflight_checks")
    def test_runner_fails_on_preflight_failure(self, mock_preflight: MagicMock) -> None:
        mock_preflight.return_value = {"kafka": False, "schema_registry": True, "minio": True}
        config = SparkStreamingConfig()
        runner = StreamingIngestionRunner(config=config, skip_preflight=False)

        exit_code = runner.run()
        assert exit_code == 1

    @patch("scripts.run_streaming_ingestion.run_preflight_checks")
    def test_runner_executes_successfully(self, mock_preflight: MagicMock) -> None:
        mock_preflight.return_value = {"kafka": True, "schema_registry": True, "minio": True}
        mock_pipeline = MagicMock()
        mock_supervisor = MagicMock()
        mock_supervisor.run_with_recovery.return_value = 0
        mock_supervisor.metrics.total_restarts = 0
        mock_supervisor.metrics.successful_runs = 1
        mock_supervisor.metrics.failed_runs = 0
        mock_supervisor.metrics.elapsed_seconds = 5.0

        config = SparkStreamingConfig()
        runner = StreamingIngestionRunner(
            config=config,
            skip_preflight=False,
            pipeline=mock_pipeline,
            supervisor=mock_supervisor,
        )

        exit_code = runner.run()
        assert exit_code == 0
        mock_supervisor.run_with_recovery.assert_called_once_with(mock_pipeline.start_queries)
        mock_pipeline.stop_queries.assert_called_once()

    def test_runner_skip_preflight(self) -> None:
        mock_pipeline = MagicMock()
        mock_supervisor = MagicMock()
        mock_supervisor.run_with_recovery.return_value = 0
        mock_supervisor.metrics.total_restarts = 0
        mock_supervisor.metrics.successful_runs = 1
        mock_supervisor.metrics.failed_runs = 0
        mock_supervisor.metrics.elapsed_seconds = 2.0

        config = SparkStreamingConfig()
        runner = StreamingIngestionRunner(
            config=config,
            skip_preflight=True,
            pipeline=mock_pipeline,
            supervisor=mock_supervisor,
        )

        with patch("scripts.run_streaming_ingestion.run_preflight_checks") as mock_preflight:
            exit_code = runner.run()
            assert exit_code == 0
            mock_preflight.assert_not_called()

    def test_runner_handles_fatal_supervisor_exception(self) -> None:
        mock_pipeline = MagicMock()
        mock_supervisor = MagicMock()
        mock_supervisor.run_with_recovery.side_effect = RuntimeError("Fatal crash")

        config = SparkStreamingConfig()
        runner = StreamingIngestionRunner(
            config=config,
            skip_preflight=True,
            pipeline=mock_pipeline,
            supervisor=mock_supervisor,
        )

        exit_code = runner.run()
        assert exit_code == 1
        mock_pipeline.stop_queries.assert_called_once()

    @patch("scripts.run_streaming_ingestion.StreamingIngestionRunner.run", return_value=0)
    def test_main_cli_execution(self, mock_run: MagicMock) -> None:
        exit_code = main(["--skip-preflight", "--once"])
        assert exit_code == 0
        mock_run.assert_called_once()
