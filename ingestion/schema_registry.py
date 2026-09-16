"""Confluent Schema Registry client manager for Avro schema registration and retrieval.

Provides programmatic interaction with the Confluent Schema Registry service endpoint,
supporting registration, compatibility checking, and schema lookup with retries and structured logging.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger("schema_registry")

# Default mapping from schema filename stem to Confluent Schema Registry subject name
DEFAULT_SUBJECT_MAPPING: dict[str, str] = {
    "cart_events": "marketplace.cart-value",
    "catalog_changes": "marketplace.catalog-value",
    "order_creation": "marketplace.orders-value",
    "payment_updates": "marketplace.payments-value",
    "stock_updates": "marketplace.inventory-value",
    "user_navigation": "marketplace.navigation-value",
}

VALID_COMPATIBILITY_LEVELS: set[str] = {
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
    "NONE",
}


class SchemaRegistryError(Exception):
    """Base exception for Schema Registry operations."""


class SchemaRegistrationError(SchemaRegistryError):
    """Raised when schema registration fails."""


class SchemaNotFoundError(SchemaRegistryError):
    """Raised when a requested schema or subject is not found."""


class SchemaCompatibilityError(SchemaRegistryError):
    """Raised when a schema is incompatible with existing registered versions."""


class SchemaRegistryManager:
    """Manages interactions with the Confluent Schema Registry service."""

    def __init__(
        self,
        url: str | None = None,
        timeout: float = 10.0,
        retry_attempts: int = 3,
        retry_backoff: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        """Initialize the SchemaRegistryManager.

        Args:
            url: Base URL of Schema Registry (e.g. 'http://localhost:8081').
                If None, resolves from SCHEMA_REGISTRY_URL or SCHEMA_REGISTRY_PORT env vars.
            timeout: HTTP request timeout in seconds.
            retry_attempts: Maximum retry attempts for transient errors.
            retry_backoff: Initial backoff delay in seconds between retries.
            session: Optional custom requests.Session for testing or connection pooling.
        """
        if url:
            self.url = url.rstrip("/")
        else:
            env_url = os.getenv("SCHEMA_REGISTRY_URL")
            if env_url:
                self.url = env_url.rstrip("/")
            else:
                port = os.getenv("SCHEMA_REGISTRY_PORT", "8081")
                self.url = f"http://localhost:{port}"

        self.timeout = timeout
        self.retry_attempts = max(1, retry_attempts)
        self.retry_backoff = retry_backoff
        self.session = session or requests.Session()
        self._schema_id_cache: dict[int, dict[str, Any]] = {}

    def _request_with_retry(
        self,
        method: str,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """Execute an HTTP request with exponential backoff retry logic.

        Args:
            method: HTTP method ('GET', 'POST', etc.).
            endpoint: API path relative to registry URL (e.g. '/subjects').
            json_data: Optional JSON body payload.
            headers: Optional HTTP headers.

        Returns:
            requests.Response object from successful request.

        Raises:
            SchemaRegistryError: On persistent network or connection failure.
        """
        full_url = f"{self.url}{endpoint}"
        req_headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        if headers:
            req_headers.update(headers)

        last_exception: Exception | None = None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=full_url,
                    json=json_data,
                    headers=req_headers,
                    timeout=self.timeout,
                )
                return response
            except (requests.RequestException, OSError) as exc:
                last_exception = exc
                logger.warning(
                    "Schema Registry request failed (attempt %d/%d): %s",
                    attempt,
                    self.retry_attempts,
                    exc,
                    extra={"url": full_url, "attempt": attempt},
                )
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_backoff * (2 ** (attempt - 1)))

        raise SchemaRegistryError(
            f"Failed to connect to Schema Registry at {full_url} after {self.retry_attempts} attempts: {last_exception}"
        ) from last_exception

    def register_schema(self, subject: str, schema_dict_or_str: dict[str, Any] | str) -> int:
        """Register an Avro schema under a given subject in the Schema Registry.

        Args:
            subject: The subject name (e.g. 'marketplace.orders-value').
            schema_dict_or_str: Avro schema as a Python dictionary or JSON string.

        Returns:
            Registered schema ID (integer).

        Raises:
            SchemaRegistrationError: If registration request returns an error HTTP status.
        """
        schema_str = json.dumps(schema_dict_or_str) if isinstance(schema_dict_or_str, dict) else schema_dict_or_str

        payload = {"schema": schema_str}
        endpoint = f"/subjects/{subject}/versions"

        logger.info("Registering schema for subject '%s'...", subject, extra={"subject": subject})

        response = self._request_with_retry("POST", endpoint, json_data=payload)

        if response.status_code in (200, 201):
            data = response.json()
            schema_id = int(data["id"])
            schema_dict = schema_dict_or_str if isinstance(schema_dict_or_str, dict) else json.loads(schema_dict_or_str)
            self._schema_id_cache[schema_id] = schema_dict
            logger.info(
                "Successfully registered schema for subject '%s' with ID %d.",
                subject,
                schema_id,
                extra={"subject": subject, "schema_id": schema_id},
            )
            return schema_id

        error_detail = response.text
        logger.error(
            "Failed to register schema for subject '%s' (HTTP %d): %s",
            subject,
            response.status_code,
            error_detail,
            extra={"subject": subject, "status_code": response.status_code},
        )
        raise SchemaRegistrationError(
            f"Schema registration for subject '{subject}' failed with status {response.status_code}: {error_detail}"
        )

    def get_latest_schema(self, subject: str) -> tuple[int, dict[str, Any]]:
        """Retrieve the latest schema version for a given subject.

        Args:
            subject: The subject name.

        Returns:
            Tuple of (schema_id, parsed_schema_dict).

        Raises:
            SchemaNotFoundError: If the subject does not exist.
            SchemaRegistryError: On other HTTP errors.
        """
        endpoint = f"/subjects/{subject}/versions/latest"
        response = self._request_with_retry("GET", endpoint)

        if response.status_code == 404:
            raise SchemaNotFoundError(f"Subject '{subject}' not found in Schema Registry.")

        if response.status_code == 200:
            data = response.json()
            schema_id = int(data["id"])
            schema_dict = json.loads(data["schema"])
            self._schema_id_cache[schema_id] = schema_dict
            return schema_id, schema_dict

        raise SchemaRegistryError(
            f"Failed to fetch latest schema for subject '{subject}' (HTTP {response.status_code}): {response.text}"
        )

    def get_schema_by_id(self, schema_id: int) -> dict[str, Any]:
        """Fetch a schema by its global unique Schema ID.

        Checks the in-memory schema cache first. On cache miss, queries the Schema Registry endpoint
        and stores the resulting schema dictionary in the cache.

        Args:
            schema_id: The schema identifier integer.

        Returns:
            Parsed schema dictionary.

        Raises:
            SchemaNotFoundError: If schema ID is not registered.
            SchemaRegistryError: On HTTP error.
        """
        if schema_id in self._schema_id_cache:
            logger.debug(
                "Schema ID %d retrieved from in-memory cache.",
                schema_id,
                extra={"schema_id": schema_id},
            )
            return self._schema_id_cache[schema_id]

        endpoint = f"/schemas/ids/{schema_id}"
        response = self._request_with_retry("GET", endpoint)

        if response.status_code == 404:
            raise SchemaNotFoundError(f"Schema ID {schema_id} not found in Schema Registry.")

        if response.status_code == 200:
            data = response.json()
            schema_dict = json.loads(data["schema"])
            self._schema_id_cache[schema_id] = schema_dict
            return schema_dict

        raise SchemaRegistryError(
            f"Failed to fetch schema ID {schema_id} (HTTP {response.status_code}): {response.text}"
        )

    def clear_schema_cache(self) -> None:
        """Clear all cached schema definitions from memory."""
        self._schema_id_cache.clear()
        logger.info("In-memory schema cache cleared.")

    def get_compatibility_level(self, subject: str | None = None) -> str:
        """Query global or subject-level compatibility configuration.

        Args:
            subject: Optional subject name. If None, queries global compatibility configuration.

        Returns:
            Compatibility level string (e.g., 'BACKWARD').

        Raises:
            SchemaNotFoundError: If subject compatibility configuration does not exist (HTTP 404).
            SchemaRegistryError: On HTTP or unexpected response format errors.
        """
        endpoint = f"/config/{subject}" if subject else "/config"
        response = self._request_with_retry("GET", endpoint)

        if response.status_code == 404:
            target = f"subject '{subject}'" if subject else "global configuration"
            raise SchemaNotFoundError(f"Compatibility configuration for {target} not found.")

        if response.status_code == 200:
            data = response.json()
            level = data.get("compatibilityLevel") or data.get("compatibility")
            if level:
                return str(level)
            raise SchemaRegistryError(f"Unexpected response structure when fetching compatibility level: {data}")

        raise SchemaRegistryError(f"Failed to fetch compatibility level (HTTP {response.status_code}): {response.text}")

    def set_compatibility_level(self, compatibility_level: str, subject: str | None = None) -> bool:
        """Set the compatibility mode globally or for a specific subject.

        Args:
            compatibility_level: One of 'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD',
                'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE'.
            subject: Optional subject name. If None, updates global compatibility level.

        Returns:
            True if setting the compatibility level succeeded.

        Raises:
            ValueError: If an unsupported compatibility level is provided.
            SchemaRegistryError: On HTTP request failure.
        """
        level_upper = compatibility_level.upper()
        if level_upper not in VALID_COMPATIBILITY_LEVELS:
            valid_str = ", ".join(sorted(VALID_COMPATIBILITY_LEVELS))
            raise ValueError(f"Invalid compatibility level '{compatibility_level}'. Must be one of: {valid_str}")

        endpoint = f"/config/{subject}" if subject else "/config"
        payload = {"compatibility": level_upper}

        response = self._request_with_retry("PUT", endpoint, json_data=payload)

        if response.status_code == 200:
            target = f"subject '{subject}'" if subject else "global config"
            logger.info(
                "Successfully set compatibility level to '%s' for %s.",
                level_upper,
                target,
                extra={"compatibility_level": level_upper, "subject": subject},
            )
            return True

        error_detail = response.text
        logger.error(
            "Failed to set compatibility level to '%s' (HTTP %d): %s",
            level_upper,
            response.status_code,
            error_detail,
            extra={"status_code": response.status_code, "subject": subject},
        )
        raise SchemaRegistryError(
            f"Failed to set compatibility level to '{level_upper}' (HTTP {response.status_code}): {error_detail}"
        )

    def check_compatibility(self, subject: str, schema_dict_or_str: dict[str, Any] | str) -> bool:
        """Test compatibility of a proposed schema against the latest version of a subject.

        Args:
            subject: The subject name.
            schema_dict_or_str: Proposed Avro schema.

        Returns:
            True if compatible or if subject does not yet exist, False if incompatible.
        """
        schema_str = json.dumps(schema_dict_or_str) if isinstance(schema_dict_or_str, dict) else schema_dict_or_str

        endpoint = f"/compatibility/subjects/{subject}/versions/latest"
        payload = {"schema": schema_str}

        try:
            response = self._request_with_retry("POST", endpoint, json_data=payload)
        except SchemaRegistryError as exc:
            logger.warning("Compatibility check failed due to request error: %s", exc)
            return False

        if response.status_code == 404:
            # Subject has no registered versions yet, so it is compatible
            return True

        if response.status_code == 200:
            data = response.json()
            return bool(data.get("is_compatible", False))

        logger.warning("Compatibility check returned HTTP %d: %s", response.status_code, response.text)
        return False

    @staticmethod
    def load_schema_from_file(file_path: str | Path) -> tuple[dict[str, Any], str]:
        """Load an Avro schema file (.avsc) from the local filesystem.

        Args:
            file_path: Path to the .avsc file.

        Returns:
            Tuple of (parsed_schema_dict, raw_json_string).
        """
        path = Path(file_path)
        with path.open(encoding="utf-8") as f:
            raw_content = f.read()

        schema_dict = json.loads(raw_content)
        return schema_dict, raw_content

    def register_all_schemas_in_dir(
        self,
        schemas_dir: str | Path,
        subject_mapping: dict[str, str] | None = None,
        compatibility_level: str | None = "BACKWARD",
    ) -> dict[str, int]:
        """Load and register all .avsc schemas from a directory into the Schema Registry.

        Args:
            schemas_dir: Directory containing .avsc schema files.
            subject_mapping: Mapping of file stem -> subject name. Defaults to DEFAULT_SUBJECT_MAPPING.
            compatibility_level: Optional compatibility level to set for each subject prior to schema registration.

        Returns:
            Dictionary mapping subject names to registered schema IDs.
        """
        mapping = subject_mapping or DEFAULT_SUBJECT_MAPPING
        dir_path = Path(schemas_dir)
        registered_ids: dict[str, int] = {}

        if not dir_path.is_dir():
            raise FileNotFoundError(f"Schemas directory not found at {dir_path}")

        avsc_files = sorted(dir_path.glob("*.avsc"))
        logger.info("Found %d Avro schema files in '%s'.", len(avsc_files), dir_path)

        for schema_file in avsc_files:
            stem = schema_file.stem
            subject = mapping.get(stem, f"{stem}-value")

            if compatibility_level:
                self.set_compatibility_level(compatibility_level, subject=subject)

            schema_dict, _ = self.load_schema_from_file(schema_file)
            schema_id = self.register_schema(subject, schema_dict)
            registered_ids[subject] = schema_id

        logger.info("Successfully registered %d schemas in Schema Registry.", len(registered_ids))
        return registered_ids
