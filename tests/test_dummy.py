"""Baseline test suite for marketplace sandbox CI/CD validation."""

import unittest
from pathlib import Path

from scripts.verify_services import ServiceCheck, check_tcp_port, get_service_checks


class TestBaselineVerification(unittest.TestCase):
    """Test suite for repository integrity and service verification logic."""

    def test_baseline_environment(self) -> None:
        """Verify that the repository root path and base structure exist."""
        repo_root = Path(__file__).resolve().parent.parent
        self.assertTrue((repo_root / "pyproject.toml").exists())
        self.assertTrue((repo_root / "infrastructure" / "docker-compose.yml").exists())
        self.assertTrue((repo_root / "scripts" / "verify_services.py").exists())

    def test_service_check_data_model(self) -> None:
        """Verify the ServiceCheck dataclass structure and instantiation."""
        check = ServiceCheck(
            name="TestService",
            host="localhost",
            port=8080,
            check_type="http",
            endpoint="http://localhost:8080/health",
        )
        self.assertEqual(check.name, "TestService")
        self.assertEqual(check.port, 8080)
        self.assertEqual(check.check_type, "http")
        self.assertEqual(check.endpoint, "http://localhost:8080/health")

    def test_get_service_checks_default(self) -> None:
        """Verify get_service_checks returns configured list of 10 infrastructure services."""
        checks = get_service_checks()
        self.assertIsInstance(checks, list)
        self.assertEqual(len(checks), 10)

        service_names = [c.name for c in checks]
        expected_services = [
            "PostgreSQL",
            "MinIO",
            "Apache Kafka",
            "Kafka UI",
            "Schema Registry",
            "Spark Master RPC",
            "Spark UI",
            "Airflow Webserver",
            "Prometheus",
            "Grafana",
        ]
        for expected in expected_services:
            self.assertIn(expected, service_names)

    def test_tcp_check_invalid_port(self) -> None:
        """Verify check_tcp_port returns False for an unassigned closed port."""
        result = check_tcp_port("127.0.0.1", 59999, timeout_sec=0.5)
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
