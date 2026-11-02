"""Medallion Bronze landing zone lifecycle and retention engine.

Enforces Time-To-Live (TTL) policies on raw landing micro-batches post-bronze commit,
identifying and safely purging staging objects in MinIO/S3 older than configured thresholds
(default 48 hours). Supports dry-run inspection, bounded object purging, and structured
JSON lifecycle audit logging conforming to ADR-0008.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from processing.bronze.config import BronzeStorageConfig

logger = logging.getLogger("processing.bronze.retention")


class BronzeRetentionError(Exception):
    """Base exception for Bronze storage retention operations."""


class BronzeStorageClientError(BronzeRetentionError):
    """Raised when S3/MinIO client interactions fail."""


@dataclass
class RetentionPruneResult:
    """Telemetry and outcome summary of a Bronze landing lifecycle pruning execution."""

    scanned_objects_count: int
    pruned_objects_count: int
    pruned_bytes: int
    duration_ms: int
    dry_run: bool
    pruned_keys: list[str]
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert prune outcome into a JSON-serializable dictionary.

        Returns:
            Dictionary mapping outcome attributes to values.
        """
        return {
            "scanned_objects_count": self.scanned_objects_count,
            "pruned_objects_count": self.pruned_objects_count,
            "pruned_bytes": self.pruned_bytes,
            "duration_ms": self.duration_ms,
            "dry_run": self.dry_run,
            "pruned_keys": self.pruned_keys,
            "error_message": self.error_message,
        }


class BronzeRetentionManager:
    """Manages raw event landing zone lifecycle, TTL enforcement, and object purging.

    Identifies staging micro-batch objects that have surpassed configured retention
    thresholds (e.g. 48 hours post-commit), supports dry-run inspection, performs safe
    batch object deletions in S3/MinIO, and generates structured audit telemetry.
    """

    def __init__(
        self,
        config: BronzeStorageConfig | None = None,
        s3_client: Any | None = None,
    ) -> None:
        """Initialize BronzeRetentionManager.

        Args:
            config: BronzeStorageConfig containing storage paths, MinIO credentials, and TTL.
            s3_client: Optional pre-configured boto3 S3 client instance for testing or injection.
        """
        self.config = config or BronzeStorageConfig()
        self._s3_client = s3_client

    @property
    def s3_client(self) -> Any:
        """Return active boto3 S3 client, initializing with MinIO credentials if not injected."""
        if self._s3_client is None:
            import boto3

            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.config.minio_endpoint,
                aws_access_key_id=self.config.minio_access_key,
                aws_secret_access_key=self.config.minio_secret_key,
                region_name="us-east-1",
            )
        return self._s3_client

    @staticmethod
    def parse_s3_uri(uri: str) -> tuple[str, str]:
        """Parse S3 / S3A URI into bucket name and object key prefix.

        Args:
            uri: S3 URI (e.g. 's3a://lakehouse-bronze/landing' or 's3://my-bucket/prefix/').

        Returns:
            Tuple of (bucket_name, key_prefix).

        Raises:
            ValueError: If URI format is invalid.
        """
        clean_uri = uri.strip()
        if clean_uri.startswith("s3a://"):
            raw_path = clean_uri[len("s3a://"):]
        elif clean_uri.startswith("s3://"):
            raw_path = clean_uri[len("s3://"):]
        else:
            raw_path = clean_uri.lstrip("/")

        parts = raw_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        if not bucket:
            raise ValueError(f"Cannot parse bucket from S3 URI: '{uri}'")

        return bucket, prefix

    def prune_landing_zone(
        self,
        ttl_hours: int | None = None,
        dry_run: bool | None = None,
        max_objects: int = 1000,
        raise_on_error: bool = False,
    ) -> RetentionPruneResult:
        """Identify and purge landing zone micro-batch objects exceeding TTL threshold.

        Args:
            ttl_hours: Retention TTL in hours. If None, defaults to `config.landing_ttl_hours`.
            dry_run: If True, identifies expired objects without deletion. If None, defaults
                to `config.dry_run`.
            max_objects: Maximum number of expired objects to delete in a single invocation.
            raise_on_error: If True, raises exceptions encountered during deletion.

        Returns:
            RetentionPruneResult tracking scanned, expired, and deleted object metrics.

        Raises:
            BronzeStorageClientError: If storage deletion fails and `raise_on_error` is True.
        """
        start_time = time.monotonic()
        effective_ttl_hours = ttl_hours if ttl_hours is not None else self.config.landing_ttl_hours
        effective_dry_run = dry_run if dry_run is not None else self.config.dry_run

        bucket, raw_prefix = self.parse_s3_uri(self.config.landing_path)
        normalized_prefix = (
            raw_prefix if raw_prefix.endswith("/") or not raw_prefix else f"{raw_prefix}/"
        )
        cutoff_time = datetime.now(UTC) - timedelta(hours=effective_ttl_hours)

        scanned_count = 0
        expired_objects: list[dict[str, Any]] = []

        logger.info(
            "Starting Bronze landing zone retention evaluation",
            extra={
                "bucket": bucket,
                "prefix": normalized_prefix,
                "ttl_hours": effective_ttl_hours,
                "cutoff_time": cutoff_time.isoformat(),
                "dry_run": effective_dry_run,
                "max_objects": max_objects,
            },
        )

        client = self.s3_client
        try:
            if hasattr(client, "get_paginator"):
                paginator = client.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=bucket, Prefix=normalized_prefix)
            else:
                pages = [client.list_objects_v2(Bucket=bucket, Prefix=normalized_prefix)]

            for page in pages:
                contents = page.get("Contents", [])
                for obj in contents:
                    scanned_count += 1
                    last_modified: datetime | None = obj.get("LastModified")
                    if last_modified is not None:
                        if last_modified.tzinfo is None:
                            last_modified = last_modified.replace(tzinfo=UTC)
                        if last_modified < cutoff_time:
                            expired_objects.append(obj)
                            if len(expired_objects) >= max_objects:
                                break
                if len(expired_objects) >= max_objects:
                    break

        except Exception as exc:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            err_msg = f"Failed to list objects in landing zone {bucket}/{normalized_prefix}: {exc}"
            logger.error(err_msg, extra={"error": str(exc), "duration_ms": duration_ms})
            if raise_on_error:
                raise BronzeStorageClientError(err_msg) from exc
            return RetentionPruneResult(
                scanned_objects_count=scanned_count,
                pruned_objects_count=0,
                pruned_bytes=0,
                duration_ms=duration_ms,
                dry_run=effective_dry_run,
                pruned_keys=[],
                error_message=err_msg,
            )

        expired_keys = [o["Key"] for o in expired_objects]
        pruned_bytes = sum(int(o.get("Size", 0)) for o in expired_objects)

        # Handle Dry-Run Mode
        if effective_dry_run:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            logger.info(
                "Bronze landing retention evaluation completed (DRY RUN)",
                extra={
                    "scanned_objects": scanned_count,
                    "planned_prune_count": len(expired_keys),
                    "planned_prune_bytes": pruned_bytes,
                    "duration_ms": duration_ms,
                },
            )
            return RetentionPruneResult(
                scanned_objects_count=scanned_count,
                pruned_objects_count=len(expired_keys),
                pruned_bytes=pruned_bytes,
                duration_ms=duration_ms,
                dry_run=True,
                pruned_keys=expired_keys,
            )

        # Handle Actual Deletion
        deleted_count = 0
        if expired_keys:
            try:
                # S3 batch delete supports up to 1000 keys per request
                for i in range(0, len(expired_keys), 1000):
                    batch_keys = expired_keys[i : i + 1000]
                    delete_payload = {"Objects": [{"Key": k} for k in batch_keys]}
                    client.delete_objects(Bucket=bucket, Delete=delete_payload)
                    deleted_count += len(batch_keys)
            except Exception as exc:
                duration_ms = int((time.monotonic() - start_time) * 1000)
                err_msg = f"Error during object batch deletion in landing zone: {exc}"
                logger.error(err_msg, extra={"error": str(exc), "duration_ms": duration_ms})
                if raise_on_error:
                    raise BronzeStorageClientError(err_msg) from exc
                return RetentionPruneResult(
                    scanned_objects_count=scanned_count,
                    pruned_objects_count=deleted_count,
                    pruned_bytes=pruned_bytes,
                    duration_ms=duration_ms,
                    dry_run=False,
                    pruned_keys=expired_keys[:deleted_count],
                    error_message=err_msg,
                )

        duration_ms = int((time.monotonic() - start_time) * 1000)
        logger.info(
            "Bronze landing retention purge completed",
            extra={
                "scanned_objects": scanned_count,
                "pruned_objects": len(expired_keys),
                "pruned_bytes": pruned_bytes,
                "duration_ms": duration_ms,
            },
        )

        return RetentionPruneResult(
            scanned_objects_count=scanned_count,
            pruned_objects_count=len(expired_keys),
            pruned_bytes=pruned_bytes,
            duration_ms=duration_ms,
            dry_run=False,
            pruned_keys=expired_keys,
        )
