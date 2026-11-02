"""Unit test suite for Medallion BronzeRetentionManager and RetentionPruneResult.

Tests URI parsing, TTL calculation, dry-run object pruning, actual batch deletion,
bounded object processing (max_objects), and storage exception handling.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from processing.bronze.config import BronzeStorageConfig
from processing.bronze.retention import (
    BronzeRetentionManager,
    BronzeStorageClientError,
    RetentionPruneResult,
)


class TestBronzeRetentionManager:
    """Test suite verifying BronzeRetentionManager behavior with mocked boto3 S3 client."""

    def test_retention_prune_result_to_dict(self) -> None:
        """Verify RetentionPruneResult.to_dict() serialization."""
        result = RetentionPruneResult(
            scanned_objects_count=10,
            pruned_objects_count=4,
            pruned_bytes=102400,
            duration_ms=45,
            dry_run=True,
            pruned_keys=["landing/file1.avro", "landing/file2.avro"],
            error_message=None,
        )
        d = result.to_dict()
        assert d["scanned_objects_count"] == 10
        assert d["pruned_objects_count"] == 4
        assert d["pruned_bytes"] == 102400
        assert d["duration_ms"] == 45
        assert d["dry_run"] is True
        assert len(d["pruned_keys"]) == 2
        assert d["error_message"] is None

    def test_parse_s3_uri_formats(self) -> None:
        """Verify parse_s3_uri properly decomposes various S3/S3A URI structures."""
        bucket, prefix = BronzeRetentionManager.parse_s3_uri("s3a://lakehouse-bronze/landing")
        assert bucket == "lakehouse-bronze"
        assert prefix == "landing"

        bucket, prefix = BronzeRetentionManager.parse_s3_uri("s3://my-custom-bucket/landing/events/")
        assert bucket == "my-custom-bucket"
        assert prefix == "landing/events/"

        bucket, prefix = BronzeRetentionManager.parse_s3_uri("lakehouse-bronze/landing")
        assert bucket == "lakehouse-bronze"
        assert prefix == "landing"

        with pytest.raises(ValueError, match="Cannot parse bucket from S3 URI"):
            BronzeRetentionManager.parse_s3_uri("")

    def test_prune_landing_zone_dry_run(self) -> None:
        """Verify dry-run mode identifies expired files without executing S3 deletions."""
        now = datetime.now(UTC)
        expired_date = now - timedelta(hours=50)  # > 48h TTL
        recent_date = now - timedelta(hours=10)   # < 48h TTL

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "landing/orders/batch1.avro", "LastModified": expired_date, "Size": 1000},
                    {"Key": "landing/orders/batch2.avro", "LastModified": recent_date, "Size": 2000},
                    {"Key": "landing/orders/batch3.avro", "LastModified": expired_date, "Size": 3000},
                ]
            }
        ]

        config = BronzeStorageConfig(landing_ttl_hours=48)
        manager = BronzeRetentionManager(config=config, s3_client=mock_s3)
        result = manager.prune_landing_zone(dry_run=True)

        assert result.scanned_objects_count == 3
        assert result.pruned_objects_count == 2
        assert result.pruned_bytes == 4000
        assert result.dry_run is True
        assert "landing/orders/batch1.avro" in result.pruned_keys
        assert "landing/orders/batch3.avro" in result.pruned_keys
        assert "landing/orders/batch2.avro" not in result.pruned_keys
        # Assert S3 delete was never called in dry-run mode
        mock_s3.delete_objects.assert_not_called()

    def test_prune_landing_zone_actual_deletion(self) -> None:
        """Verify non-dry-run mode executes S3 batch delete on expired files."""
        now = datetime.now(UTC)
        expired_date = now - timedelta(hours=60)

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "landing/payments/batch1.avro", "LastModified": expired_date, "Size": 5000},
                    {"Key": "landing/payments/batch2.avro", "LastModified": expired_date, "Size": 7000},
                ]
            }
        ]

        config = BronzeStorageConfig(landing_ttl_hours=48)
        manager = BronzeRetentionManager(config=config, s3_client=mock_s3)
        result = manager.prune_landing_zone(dry_run=False)

        assert result.scanned_objects_count == 2
        assert result.pruned_objects_count == 2
        assert result.pruned_bytes == 12000
        assert result.dry_run is False
        assert len(result.pruned_keys) == 2

        # Assert S3 delete was called with correct payload
        mock_s3.delete_objects.assert_called_once_with(
            Bucket="lakehouse-bronze",
            Delete={
                "Objects": [
                    {"Key": "landing/payments/batch1.avro"},
                    {"Key": "landing/payments/batch2.avro"},
                ]
            },
        )

    def test_prune_landing_zone_empty_contents(self) -> None:
        """Verify handling when landing bucket has no objects."""
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

        manager = BronzeRetentionManager(s3_client=mock_s3)
        result = manager.prune_landing_zone()

        assert result.scanned_objects_count == 0
        assert result.pruned_objects_count == 0
        assert result.pruned_bytes == 0
        assert result.pruned_keys == []
        mock_s3.delete_objects.assert_not_called()

    def test_prune_landing_zone_max_objects_cap(self) -> None:
        """Verify max_objects limits number of pruned objects per execution."""
        now = datetime.now(UTC)
        expired_date = now - timedelta(hours=72)

        items = [{"Key": f"landing/batch_{i}.avro", "LastModified": expired_date, "Size": 100} for i in range(10)]
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": items}]

        manager = BronzeRetentionManager(s3_client=mock_s3)
        result = manager.prune_landing_zone(dry_run=True, max_objects=4)

        assert result.pruned_objects_count == 4
        assert len(result.pruned_keys) == 4

    def test_prune_landing_zone_error_handling(self) -> None:
        """Verify error handling and raise_on_error flag."""
        mock_s3 = MagicMock()
        mock_s3.get_paginator.side_effect = RuntimeError("S3 Endpoint unreachable")

        manager = BronzeRetentionManager(s3_client=mock_s3)

        # Without raise_on_error: returns result with error_message
        result = manager.prune_landing_zone(raise_on_error=False)
        assert result.pruned_objects_count == 0
        assert result.error_message is not None
        assert "S3 Endpoint unreachable" in result.error_message

        # With raise_on_error: raises BronzeStorageClientError
        with pytest.raises(BronzeStorageClientError, match="Failed to list objects in landing zone"):
            manager.prune_landing_zone(raise_on_error=True)
