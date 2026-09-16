"""Unit and integration tests for Confluent Schema Registry interactions and Avro schema definitions.

Validates:
- Avro schema JSON structure and fastavro parsing for all 6 schema definitions.
- Programmatic schema registration logic using mocks for offline safety.
- Live Schema Registry service registration when the local environment is running.
"""

import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import fastavro
import pytest
import requests

from ingestion.schema_registry import (
    DEFAULT_SUBJECT_MAPPING,
    SchemaNotFoundError,
    SchemaRegistrationError,
    SchemaRegistryError,
    SchemaRegistryManager,
)


@pytest.fixture
def schemas_dir() -> Path:
    """Return path to Avro schemas directory."""
    return Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro"


@pytest.fixture
def mock_session() -> MagicMock:
    """Return a mocked requests.Session object."""
    return MagicMock(spec=requests.Session)


def is_service_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (TimeoutError, OSError):
        return False


def test_schema_files_exist_and_parse(schemas_dir: Path) -> None:
    """Validate all 6 .avsc schema files exist and parse with fastavro."""
    expected_files = [
        "cart_events.avsc",
        "catalog_changes.avsc",
        "order_creation.avsc",
        "payment_updates.avsc",
        "stock_updates.avsc",
        "user_navigation.avsc",
    ]

    for filename in expected_files:
        file_path = schemas_dir / filename
        assert file_path.exists(), f"Schema file missing: {filename}"

        schema_dict, _ = SchemaRegistryManager.load_schema_from_file(file_path)
        assert isinstance(schema_dict, dict)
        assert "type" in schema_dict and schema_dict["type"] == "record"
        assert "name" in schema_dict
        assert "fields" in schema_dict

        # Verify fastavro can parse the schema definition without error
        parsed = fastavro.parse_schema(schema_dict)
        assert parsed is not None


def test_register_schema_success_mocked(mock_session: MagicMock) -> None:
    """Test successful schema registration using a mocked session."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 101}
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)
    schema_id = manager.register_schema("marketplace.orders-value", {"type": "record", "name": "Test", "fields": []})

    assert schema_id == 101
    mock_session.request.assert_called_once()


def test_register_schema_http_error(mock_session: MagicMock) -> None:
    """Test schema registration handling when HTTP returns an error."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.text = "Invalid schema syntax"
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    with pytest.raises(SchemaRegistrationError) as exc_info:
        manager.register_schema("marketplace.orders-value", {"type": "record", "name": "Test", "fields": []})

    assert "400" in str(exc_info.value)
    assert "Invalid schema syntax" in str(exc_info.value)


def test_register_all_schemas_in_dir_mocked(schemas_dir: Path, mock_session: MagicMock) -> None:
    """Test registering all directory schemas using a mocked session."""
    counter = 0

    def mock_request(method="GET", **_kwargs):
        nonlocal counter
        resp = MagicMock(spec=requests.Response)

        if method == "PUT":
            resp.status_code = 200
            resp.json.return_value = {"compatibility": "BACKWARD"}
        elif method == "POST":
            counter += 1
            resp.status_code = 201
            resp.json.return_value = {"id": counter}
        return resp

    mock_session.request.side_effect = mock_request

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)
    results = manager.register_all_schemas_in_dir(schemas_dir)

    assert len(results) == 6
    for _stem, subject in DEFAULT_SUBJECT_MAPPING.items():
        assert subject in results
        assert isinstance(results[subject], int)


def test_get_latest_schema_mocked(mock_session: MagicMock) -> None:
    """Test retrieving latest schema with a mocked session."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": 15,
        "subject": "marketplace.orders-value",
        "version": 1,
        "schema": '{"type": "record", "name": "OrderCreationEvent", "fields": []}',
    }
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)
    schema_id, schema_dict = manager.get_latest_schema("marketplace.orders-value")

    assert schema_id == 15
    assert schema_dict["name"] == "OrderCreationEvent"


def test_check_compatibility_mocked(mock_session: MagicMock) -> None:
    """Test schema compatibility checking with a mocked session."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"is_compatible": True}
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)
    dummy_schema = {"type": "record", "name": "Test", "fields": []}
    is_compatible = manager.check_compatibility("marketplace.orders-value", dummy_schema)

    assert is_compatible is True


def test_retry_mechanism(mock_session: MagicMock) -> None:
    """Test retry backoff mechanism when request initially fails with connection error."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 42}

    # First call raises RequestException, second call succeeds
    mock_session.request.side_effect = [requests.RequestException("Conn error"), mock_response]

    dummy_schema = {"type": "record", "name": "Test", "fields": []}
    with patch("time.sleep", return_value=None):
        manager = SchemaRegistryManager(url="http://mock-registry:8081", retry_attempts=2, session=mock_session)
        schema_id = manager.register_schema("marketplace.orders-value", dummy_schema)

    assert schema_id == 42
    assert mock_session.request.call_count == 2


def test_persistent_network_failure(mock_session: MagicMock) -> None:
    """Test exception raised after max retry attempts fail."""
    mock_session.request.side_effect = requests.RequestException("Connection refused")

    dummy_schema = {"type": "record", "name": "Test", "fields": []}
    with patch("time.sleep", return_value=None):
        manager = SchemaRegistryManager(url="http://mock-registry:8081", retry_attempts=2, session=mock_session)
        with pytest.raises(SchemaRegistryError) as exc_info:
            manager.register_schema("marketplace.orders-value", dummy_schema)

    assert "Failed to connect to Schema Registry" in str(exc_info.value)


def test_get_schema_by_id_cache_hit(mock_session: MagicMock) -> None:
    """Test that get_schema_by_id uses in-memory cache and avoids redundant HTTP requests."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"schema": '{"type": "record", "name": "CachedOrderEvent", "fields": []}'}
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    # First call: cache miss, triggers HTTP GET request
    schema_1 = manager.get_schema_by_id(100)
    assert schema_1["name"] == "CachedOrderEvent"
    assert mock_session.request.call_count == 1

    # Second call: cache hit, skips HTTP GET request
    schema_2 = manager.get_schema_by_id(100)
    assert schema_2["name"] == "CachedOrderEvent"
    assert mock_session.request.call_count == 1

    # Clear cache and query again: cache miss, triggers HTTP GET request
    manager.clear_schema_cache()
    schema_3 = manager.get_schema_by_id(100)
    assert schema_3["name"] == "CachedOrderEvent"
    assert mock_session.request.call_count == 2


def test_get_compatibility_level_mocked(mock_session: MagicMock) -> None:
    """Test retrieving global and subject-level compatibility configuration."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"compatibilityLevel": "FULL"}
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    # Global query
    level_global = manager.get_compatibility_level()
    assert level_global == "FULL"
    mock_session.request.assert_called_with(
        method="GET",
        url="http://mock-registry:8081/config",
        json=None,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10.0,
    )

    # Subject-level query
    mock_response.json.return_value = {"compatibility": "BACKWARD"}
    level_subject = manager.get_compatibility_level("marketplace.orders-value")
    assert level_subject == "BACKWARD"
    mock_session.request.assert_called_with(
        method="GET",
        url="http://mock-registry:8081/config/marketplace.orders-value",
        json=None,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10.0,
    )


def test_get_compatibility_level_not_found(mock_session: MagicMock) -> None:
    """Test get_compatibility_level raising SchemaNotFoundError when HTTP status is 404."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 404
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    with pytest.raises(SchemaNotFoundError) as exc_info:
        manager.get_compatibility_level("marketplace.unknown-value")

    assert "Compatibility configuration for subject 'marketplace.unknown-value' not found" in str(exc_info.value)


def test_set_compatibility_level_mocked(mock_session: MagicMock) -> None:
    """Test setting compatibility level globally and per-subject."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"compatibility": "BACKWARD"}
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    # Set subject compatibility level
    result = manager.set_compatibility_level("BACKWARD", subject="marketplace.orders-value")
    assert result is True
    mock_session.request.assert_called_with(
        method="PUT",
        url="http://mock-registry:8081/config/marketplace.orders-value",
        json={"compatibility": "BACKWARD"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10.0,
    )

    # Set global compatibility level
    result_global = manager.set_compatibility_level("FULL_TRANSITIVE")
    assert result_global is True
    mock_session.request.assert_called_with(
        method="PUT",
        url="http://mock-registry:8081/config",
        json={"compatibility": "FULL_TRANSITIVE"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10.0,
    )


def test_set_compatibility_level_invalid(mock_session: MagicMock) -> None:
    """Test set_compatibility_level raises ValueError for invalid compatibility mode."""
    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    with pytest.raises(ValueError) as exc_info:
        manager.set_compatibility_level("INVALID_MODE")

    assert "Invalid compatibility level 'INVALID_MODE'" in str(exc_info.value)


def test_set_compatibility_level_http_error(mock_session: MagicMock) -> None:
    """Test set_compatibility_level raises SchemaRegistryError when HTTP response is 400."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.text = "Invalid compatibility level parameter"
    mock_session.request.return_value = mock_response

    manager = SchemaRegistryManager(url="http://mock-registry:8081", session=mock_session)

    with pytest.raises(SchemaRegistryError) as exc_info:
        manager.set_compatibility_level("FORWARD", subject="marketplace.orders-value")

    assert "Failed to set compatibility level to 'FORWARD'" in str(exc_info.value)
    assert "400" in str(exc_info.value)


def test_live_schema_registry_registration(schemas_dir: Path) -> None:
    """Integration test against live Schema Registry container if operational."""
    if not is_service_available("localhost", 8081):
        pytest.skip("Local Schema Registry container (localhost:8081) is not reachable.")

    manager = SchemaRegistryManager(url="http://localhost:8081")
    results = manager.register_all_schemas_in_dir(schemas_dir)

    assert len(results) == 6
    for subject, schema_id in results.items():
        assert schema_id > 0
        fetched_id, fetched_schema = manager.get_latest_schema(subject)
        assert fetched_id == schema_id
        assert "type" in fetched_schema
