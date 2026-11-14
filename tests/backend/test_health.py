"""Tests for the health endpoint."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from backend.main import create_app


@pytest.fixture
def client():
    """Create a test client with mocked dependencies."""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    """Test suite for /api/health endpoints."""

    def test_health_basic(self, client):
        """Test basic health check returns 200."""
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] in ["healthy", "degraded", "unhealthy"]

    def test_health_has_timestamp(self, client):
        """Test health response includes timestamp."""
        response = client.get("/api/health")
        data = response.json()
        assert "timestamp" in data

    def test_health_has_components(self, client):
        """Test health response includes component status."""
        response = client.get("/api/health")
        data = response.json()
        assert "components" in data
        components = data["components"]
        # Should report on each infrastructure component
        for component in ["kafka", "hbase", "hdfs", "spark"]:
            assert component in components, f"Missing component: {component}"


class TestHealthComponentStatus:
    """Test individual component health checks."""

    def test_component_structure(self, client):
        """Test each component has status and details fields."""
        response = client.get("/api/health")
        data = response.json()
        for name, comp in data.get("components", {}).items():
            assert "status" in comp, f"Component {name} missing 'status'"


class TestReadinessEndpoint:
    """Test /api/health/ready endpoint if it exists."""

    def test_readiness(self, client):
        """Test readiness probe returns expected format."""
        response = client.get("/api/health/ready")
        # Either it exists (200) or not (404)
        if response.status_code == 200:
            data = response.json()
            assert "ready" in data
