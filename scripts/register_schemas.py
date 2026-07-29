"""CLI script to programmatically register all Avro schemas to Confluent Schema Registry.

Reads all 6 Avro schema definitions from `ingestion/schemas/avro/*.avsc` and registers
them under their corresponding subject names in Schema Registry.
"""

import json
import logging
import os
import sys
from pathlib import Path

# Add project root directory to sys.path for standalone script execution
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from ingestion.schema_registry import SchemaRegistryError, SchemaRegistryManager  # noqa: E402


class JsonFormatter(logging.Formatter):
    """Custom JSON log formatter for structured audit logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "subject"):
            log_data["subject"] = record.subject
        if hasattr(record, "schema_id"):
            log_data["schema_id"] = record.schema_id
        if hasattr(record, "status_code"):
            log_data["status_code"] = record.status_code
        return json.dumps(log_data)


def load_env_file(env_path: Path) -> None:
    """Parse a .env file into environment variables if not already set.

    Args:
        env_path: Path to .env file.
    """
    if not env_path.exists():
        return
    try:
        with env_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'\"")
                    if key not in os.environ:
                        os.environ[key] = value
    except OSError:
        pass


def setup_logger() -> logging.Logger:
    """Configure structured JSON logging handler.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger("register_schemas")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def main() -> int:
    """Run programmatic schema registration workflow.

    Returns:
        0 on success, 1 on error.
    """
    logger = setup_logger()
    root_dir = Path(__file__).resolve().parent.parent
    load_env_file(root_dir / ".env")

    schemas_dir = root_dir / "ingestion" / "schemas" / "avro"

    logger.info("Initializing Schema Registry registration process...")
    manager = SchemaRegistryManager()

    try:
        logger.info("Enforcing BACKWARD compatibility and registering all Avro schemas...")
        results = manager.register_all_schemas_in_dir(schemas_dir, compatibility_level="BACKWARD")
        logger.info(
            "Schema registration completed successfully. Registered subjects count: %d",
            len(results),
        )

        for subject, schema_id in results.items():
            logger.info(
                "Subject '%s' registered with ID %d.",
                subject,
                schema_id,
                extra={"subject": subject, "schema_id": schema_id},
            )
        return 0
    except (SchemaRegistryError, FileNotFoundError, OSError) as exc:
        logger.error("Schema registration failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
