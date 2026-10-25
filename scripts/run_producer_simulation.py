"""Multi-threaded CLI simulation orchestrator running marketplace event producers concurrently.

Coordinates concurrent execution of domain event producers:
- CheckoutProducer (marketplace.orders, marketplace.cart)
- PaymentProducer (marketplace.payments)
- NavigationProducer (marketplace.navigation)
- InventoryProducer (marketplace.inventory, marketplace.catalog)
- AntiFraudProducer (marketplace.orders, marketplace.payments)

Supports configurable event rates, max events, runtime limits, graceful SIGINT/SIGTERM
shutdown, thread pool concurrency, and optional ChaosEngine resilience testing.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import Any

# Ensure project root is in sys.path for standalone script execution
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from ingestion.producers.antifraud_producer import AntiFraudProducer  # noqa: E402
from ingestion.producers.base_producer import BaseProducer  # noqa: E402
from ingestion.producers.chaos import ChaosConfig, ChaosEngine  # noqa: E402
from ingestion.producers.checkout_producer import CheckoutProducer  # noqa: E402
from ingestion.producers.config import ProducerConfig  # noqa: E402
from ingestion.producers.inventory_producer import InventoryProducer  # noqa: E402
from ingestion.producers.navigation_producer import NavigationProducer  # noqa: E402
from ingestion.producers.payment_producer import PaymentProducer  # noqa: E402
from ingestion.schema_registry import SchemaRegistryManager  # noqa: E402
from ingestion.serialization import AvroSerializerDeserializer  # noqa: E402

logger = logging.getLogger("simulation.runner")

VALID_PRODUCER_NAMES: tuple[str, ...] = ("checkout", "payment", "navigation", "inventory", "antifraud")
DEFAULT_BASE_RATE: float = 5.0
DEFAULT_NAVIGATION_RATE: float = 20.0


class JsonFormatter(logging.Formatter):
    """Structured JSON log formatter for simulation audit logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a structured JSON string."""
        log_data: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for key, value in record.__dict__.items():
            if key not in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            }:
                log_data[key] = value
        return json.dumps(log_data)


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structured JSON logging for the simulation orchestrator."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)


def normalize_producer_names(producer_args: list[str]) -> list[str]:
    """Normalize and validate a comma-separated or space-separated list of producer names.

    Args:
        producer_args: Raw arguments list from CLI.

    Returns:
        Deduplicated list of valid producer names.

    Raises:
        ValueError: If an unrecognized producer name is encountered.
    """
    resolved: list[str] = []
    for item in producer_args:
        tokens = [t.strip().lower() for t in item.split(",") if t.strip()]
        resolved.extend(tokens)

    if not resolved or "all" in resolved:
        return list(VALID_PRODUCER_NAMES)

    for name in resolved:
        if name not in VALID_PRODUCER_NAMES:
            raise ValueError(f"Unknown producer '{name}'. Valid options: {list(VALID_PRODUCER_NAMES)} or 'all'")

    return list(dict.fromkeys(resolved))


def create_producer(
    name: str,
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    events_per_second: float | None = None,
    max_events: int | None = None,
    runtime_seconds: float | None = None,
    schema_registry_manager: SchemaRegistryManager | None = None,
    serializer: AvroSerializerDeserializer | None = None,
) -> BaseProducer:
    """Instantiate a domain event producer with the specified configuration.

    Args:
        name: Name of domain producer ('checkout', 'payment', 'navigation', 'inventory', 'antifraud').
        bootstrap_servers: Kafka bootstrap brokers.
        schema_registry_url: Schema Registry base URL.
        events_per_second: Target production rate. If None, default rates apply
            (20.0 for navigation, 5.0 for others).
        max_events: Maximum events to produce.
        runtime_seconds: Maximum execution runtime in seconds.
        schema_registry_manager: Optional shared SchemaRegistryManager instance.
        serializer: Optional shared AvroSerializerDeserializer instance.

    Returns:
        Configured BaseProducer instance.

    Raises:
        ValueError: If name is not a recognized producer name.
    """
    target_rate = events_per_second
    if target_rate is None:
        target_rate = DEFAULT_NAVIGATION_RATE if name == "navigation" else DEFAULT_BASE_RATE

    sr_mgr = schema_registry_manager or SchemaRegistryManager(url=schema_registry_url)
    ser = serializer or AvroSerializerDeserializer(schema_registry_manager=sr_mgr)

    if name == "checkout":
        config = ProducerConfig(
            topic=CheckoutProducer.ORDERS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=target_rate,
            max_events=max_events,
            runtime_seconds=runtime_seconds,
            client_id="sim-checkout-producer",
        )
        return CheckoutProducer(
            config=config,
            mode="blended",
            schema_registry_manager=sr_mgr,
            serializer=ser,
        )

    if name == "payment":
        config = ProducerConfig(
            topic=PaymentProducer.PAYMENTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=target_rate,
            max_events=max_events,
            runtime_seconds=runtime_seconds,
            client_id="sim-payment-producer",
        )
        return PaymentProducer(
            config=config,
            schema_registry_manager=sr_mgr,
            serializer=ser,
        )

    if name == "navigation":
        config = ProducerConfig(
            topic=NavigationProducer.NAVIGATION_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=target_rate,
            max_events=max_events,
            runtime_seconds=runtime_seconds,
            client_id="sim-navigation-producer",
        )
        return NavigationProducer(
            config=config,
            schema_registry_manager=sr_mgr,
            serializer=ser,
        )

    if name == "inventory":
        config = ProducerConfig(
            topic=InventoryProducer.INVENTORY_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=target_rate,
            max_events=max_events,
            runtime_seconds=runtime_seconds,
            client_id="sim-inventory-producer",
        )
        return InventoryProducer(
            config=config,
            mode="blended",
            schema_registry_manager=sr_mgr,
            serializer=ser,
        )

    if name == "antifraud":
        config = ProducerConfig(
            topic=AntiFraudProducer.ORDERS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=target_rate,
            max_events=max_events,
            runtime_seconds=runtime_seconds,
            client_id="sim-antifraud-producer",
        )
        return AntiFraudProducer(
            config=config,
            pattern="high_value",
            schema_registry_manager=sr_mgr,
            serializer=ser,
        )

    raise ValueError(f"Unknown producer '{name}'. Valid choices: {list(VALID_PRODUCER_NAMES)}")


@dataclass
class SimulationSummary:
    """Aggregated telemetry and execution metrics for a simulation run."""

    total_produced: int = 0
    total_delivered: int = 0
    total_errors: int = 0
    elapsed_seconds: float = 0.0
    aggregate_throughput: float = 0.0
    producer_summaries: dict[str, dict[str, Any]] = field(default_factory=dict)
    chaos_metrics: dict[str, int] | None = None


class SimulationRunner:
    """Multi-threaded simulation orchestrator executing Kafka producers concurrently."""

    def __init__(
        self,
        producers: list[BaseProducer],
        chaos_engine: ChaosEngine | None = None,
        max_workers: int | None = None,
    ) -> None:
        """Initialize SimulationRunner.

        Args:
            producers: List of configured BaseProducer instances.
            chaos_engine: Optional ChaosEngine instance for injecting simulated anomalies.
            max_workers: Maximum thread pool worker count (defaults to len(producers)).
        """
        self.producers = producers
        self.chaos_engine = chaos_engine
        self.max_workers = max_workers or max(1, len(producers))
        self.stop_event = threading.Event()
        self.start_time: float = 0.0
        self.end_time: float | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._futures: list[Any] = []

    def _run_producer_worker(self, producer: BaseProducer) -> None:
        """Worker function running the production loop for a single producer in a thread.

        Args:
            producer: BaseProducer instance to execute.
        """
        producer_name = producer.__class__.__name__
        logger.info(
            "Worker started for producer %s (topic: %s, target_rate: %.1f msg/s)",
            producer_name,
            producer.config.topic,
            producer.config.events_per_second,
            extra={
                "producer": producer_name,
                "topic": producer.config.topic,
                "target_rate": producer.config.events_per_second,
            },
        )

        producer.setup()
        producer.is_running = True
        producer.metrics.start_time = time.monotonic()
        base_rate = producer.config.events_per_second

        try:
            while producer.is_running and not self.stop_event.is_set():
                loop_start = time.monotonic()

                # Check max events limit
                if (
                    producer.config.max_events is not None
                    and producer.metrics.produced_count >= producer.config.max_events
                ):
                    logger.info(
                        "Producer %s reached max_events limit (%d). Exiting loop.",
                        producer_name,
                        producer.config.max_events,
                        extra={"producer": producer_name, "max_events": producer.config.max_events},
                    )
                    break

                # Check runtime limit
                if (
                    producer.config.runtime_seconds is not None
                    and producer.metrics.elapsed_seconds >= producer.config.runtime_seconds
                ):
                    logger.info(
                        "Producer %s reached runtime limit (%.1fs). Exiting loop.",
                        producer_name,
                        producer.config.runtime_seconds,
                        extra={"producer": producer_name, "runtime_seconds": producer.config.runtime_seconds},
                    )
                    break

                if self.chaos_engine is not None:
                    event = producer.generate_event()
                    chaos_events = self.chaos_engine.apply_chaos(event)
                    for c_event in chaos_events:
                        producer.produce_one(event=c_event)
                    dynamic_rate = self.chaos_engine.calculate_burst_rate(
                        base_rate=base_rate,
                        elapsed_seconds=producer.metrics.elapsed_seconds,
                    )
                    target_interval = 1.0 / dynamic_rate if dynamic_rate > 0.0 else 0.0
                else:
                    producer.produce_one()
                    target_interval = 1.0 / base_rate if base_rate > 0.0 else 0.0

                elapsed = time.monotonic() - loop_start
                sleep_time = target_interval - elapsed
                if sleep_time > 0 and producer.is_running and not self.stop_event.is_set():
                    time.sleep(sleep_time)

        except Exception as exc:
            producer.metrics.error_count += 1
            logger.error(
                "Unhandled error in producer worker %s: %s",
                producer_name,
                exc,
                extra={"producer": producer_name, "error": str(exc)},
            )
        finally:
            producer.close()
            logger.info(
                "Worker finished for producer %s. Produced: %d, Delivered: %d, Errors: %d",
                producer_name,
                producer.metrics.produced_count,
                producer.metrics.delivered_count,
                producer.metrics.error_count,
                extra={
                    "producer": producer_name,
                    "produced": producer.metrics.produced_count,
                    "delivered": producer.metrics.delivered_count,
                    "errors": producer.metrics.error_count,
                },
            )

    def stop(self) -> None:
        """Signal all running producer workers to halt and flush their queues."""
        self.stop_event.set()
        for producer in self.producers:
            producer.is_running = False

    def get_summary(self) -> SimulationSummary:
        """Compute aggregated performance metrics across all producers.

        Returns:
            SimulationSummary instance.
        """
        total_produced = sum(p.metrics.produced_count for p in self.producers)
        total_delivered = sum(p.metrics.delivered_count for p in self.producers)
        total_errors = sum(p.metrics.error_count for p in self.producers)
        elapsed = (self.end_time or time.monotonic()) - self.start_time if self.start_time > 0.0 else 0.0
        throughput = total_delivered / elapsed if elapsed > 0.0 else 0.0

        producer_details: dict[str, dict[str, Any]] = {}
        for p in self.producers:
            p_name = p.__class__.__name__
            producer_details[p_name] = {
                "topic": p.config.topic,
                "produced": p.metrics.produced_count,
                "delivered": p.metrics.delivered_count,
                "errors": p.metrics.error_count,
                "elapsed_seconds": round(p.metrics.elapsed_seconds, 2),
                "throughput_eps": round(p.metrics.effective_throughput, 2),
            }

        chaos_dict: dict[str, int] | None = None
        if self.chaos_engine is not None:
            chaos_dict = {
                "evaluated": self.chaos_engine.metrics.evaluated_count,
                "delayed": self.chaos_engine.metrics.delayed_count,
                "duplicated": self.chaos_engine.metrics.duplicated_count,
                "violated": self.chaos_engine.metrics.violated_count,
                "dropped": self.chaos_engine.metrics.dropped_count,
            }

        return SimulationSummary(
            total_produced=total_produced,
            total_delivered=total_delivered,
            total_errors=total_errors,
            elapsed_seconds=round(elapsed, 2),
            aggregate_throughput=round(throughput, 2),
            producer_summaries=producer_details,
            chaos_metrics=chaos_dict,
        )

    def run(self, runtime_seconds: float | None = None) -> SimulationSummary:
        """Run the simulation until runtime expires or SIGINT/SIGTERM is received.

        Args:
            runtime_seconds: Max seconds to run. If None, runs indefinitely until signal.

        Returns:
            SimulationSummary containing aggregated metrics across all producers.
        """
        self.start_time = time.monotonic()
        self.stop_event.clear()

        def _signal_handler(signum: int, _frame: FrameType | None) -> None:
            sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
            logger.info(
                "Received termination signal %s. Initiating graceful shutdown...",
                sig_name,
                extra={"signal": sig_name},
            )
            self.stop()

        original_sigint = None
        original_sigterm = None
        try:
            original_sigint = signal.signal(signal.SIGINT, _signal_handler)
            original_sigterm = signal.signal(signal.SIGTERM, _signal_handler)
        except (ValueError, AttributeError):
            # Non-main thread execution
            pass

        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="sim-worker")
        self._futures = [self._executor.submit(self._run_producer_worker, p) for p in self.producers]

        try:
            while not self.stop_event.is_set():
                # Check if all workers have exited (e.g. max_events reached)
                if all(f.done() for f in self._futures):
                    logger.info("All producer worker threads completed execution.")
                    break

                # Check runtime limit
                elapsed = time.monotonic() - self.start_time
                if runtime_seconds is not None and elapsed >= runtime_seconds:
                    logger.info(
                        "Simulation reached global runtime limit of %.1fs. Halting.",
                        runtime_seconds,
                        extra={"runtime_seconds": runtime_seconds},
                    )
                    break

                time.sleep(0.2)
        finally:
            self.stop()
            if self._executor is not None:
                self._executor.shutdown(wait=True)
            self.end_time = time.monotonic()

            # Restore original signal handlers
            try:
                if original_sigint is not None:
                    signal.signal(signal.SIGINT, original_sigint)
                if original_sigterm is not None:
                    signal.signal(signal.SIGTERM, original_sigterm)
            except (ValueError, AttributeError):
                pass

        summary = self.get_summary()
        logger.info(
            "Simulation summary: Total Produced=%d, Delivered=%d, Errors=%d, Elapsed=%.2fs, Throughput=%.2f msg/s",
            summary.total_produced,
            summary.total_delivered,
            summary.total_errors,
            summary.elapsed_seconds,
            summary.aggregate_throughput,
            extra={
                "total_produced": summary.total_produced,
                "total_delivered": summary.total_delivered,
                "total_errors": summary.total_errors,
                "elapsed_seconds": summary.elapsed_seconds,
                "aggregate_throughput": summary.aggregate_throughput,
                "producer_summaries": summary.producer_summaries,
                "chaos_metrics": summary.chaos_metrics,
            },
        )
        return summary


def build_simulation_arg_parser() -> argparse.ArgumentParser:
    """Construct ArgumentParser with all simulation CLI options."""
    parser = argparse.ArgumentParser(
        description="Marketplace Event Producer Simulation Orchestrator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--producers",
        nargs="+",
        default=["all"],
        help="List of producers to run (comma or space separated; e.g. 'checkout,payment' or 'all'). "
        "Available: checkout, payment, navigation, inventory, antifraud, all",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap broker addresses (comma-separated)",
    )
    parser.add_argument(
        "--schema-registry-url",
        type=str,
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Confluent Schema Registry base URL",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Target events per second per producer (defaults: 5.0 for standard, 20.0 for navigation)",
    )
    parser.add_argument(
        "--runtime-seconds",
        type=float,
        default=None,
        help="Maximum duration to run simulation in seconds (default: infinite, until SIGINT/Ctrl+C)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum events to produce per producer before exiting (default: infinite)",
    )
    parser.add_argument(
        "--enable-chaos",
        action="store_true",
        default=False,
        help="Enable ChaosEngine injection (resilience simulation)",
    )
    parser.add_argument(
        "--chaos-duplicate-rate",
        type=float,
        default=0.0,
        help="Chaos probability of duplicating events [0.0, 1.0]",
    )
    parser.add_argument(
        "--chaos-delay-rate",
        type=float,
        default=0.0,
        help="Chaos probability of delaying timestamps [0.0, 1.0]",
    )
    parser.add_argument(
        "--chaos-invalid-rate",
        type=float,
        default=0.0,
        help="Chaos probability of corrupting schema payloads for DLQ testing [0.0, 1.0]",
    )
    parser.add_argument(
        "--chaos-burst-prob",
        type=float,
        default=0.0,
        help="Chaos probability of triggering throughput bursts [0.0, 1.0]",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level threshold",
    )
    return parser


def main(cli_args: list[str] | None = None) -> int:
    """CLI entry point for running the producer simulation orchestrator.

    Args:
        cli_args: Optional CLI arguments list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 for success, non-zero for error.
    """
    parser = build_simulation_arg_parser()
    args = parser.parse_args(args=cli_args)

    setup_logging(log_level=args.log_level)

    try:
        producer_names = normalize_producer_names(args.producers)
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    chaos_engine: ChaosEngine | None = None
    if args.enable_chaos:
        chaos_config = ChaosConfig(
            duplicate_rate=args.chaos_duplicate_rate,
            delay_rate=args.chaos_delay_rate,
            invalid_payload_rate=args.chaos_invalid_rate,
            burst_probability=args.chaos_burst_prob,
        )
        chaos_engine = ChaosEngine(config=chaos_config)
        logger.info(
            "ChaosEngine enabled with duplicate=%.2f, delay=%.2f, invalid=%.2f, burst=%.2f",
            args.chaos_duplicate_rate,
            args.chaos_delay_rate,
            args.chaos_invalid_rate,
            args.chaos_burst_prob,
        )

    sr_manager = SchemaRegistryManager(url=args.schema_registry_url)
    serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

    producers: list[BaseProducer] = []
    for name in producer_names:
        p = create_producer(
            name=name,
            bootstrap_servers=args.bootstrap_servers,
            schema_registry_url=args.schema_registry_url,
            events_per_second=args.rate,
            max_events=args.max_events,
            runtime_seconds=args.runtime_seconds,
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producers.append(p)

    runner = SimulationRunner(
        producers=producers,
        chaos_engine=chaos_engine,
    )

    logger.info(
        "Starting simulation with %d producers: %s",
        len(producers),
        [p.__class__.__name__ for p in producers],
        extra={"producers": [p.__class__.__name__ for p in producers]},
    )

    summary = runner.run(runtime_seconds=args.runtime_seconds)
    return 0 if summary.total_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
