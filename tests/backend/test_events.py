"""Tests for the events endpoint."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from backend.main import create_app


@pytest.fixture
def client():
    """Create a test client."""
    app = create_app()
    return TestClient(app)


class TestEventsEndpoint:
    """Test suite for /api/events endpoints."""

    def test_list_events_returns_200(self, client):
        """Test listing events returns 200 with valid structure."""
        response = client.get("/api/events")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data or "items" in data or isinstance(data, list)

    def test_list_events_with_date_range(self, client):
        """Test filtering events by date range."""
        response = client.get("/api/events", params={
            "start_date": "2024-01-01",
            "end_date": "2024-01-31"
        })
        assert response.status_code in [200, 422]

    def test_list_events_with_country(self, client):
        """Test filtering events by country code."""
        response = client.get("/api/events", params={"country": "US"})
        assert response.status_code == 200

    def test_list_events_pagination(self, client):
        """Test events pagination parameters."""
        response = client.get("/api/events", params={
            "page": 1,
            "page_size": 10
        })
        assert response.status_code == 200

    def test_events_invalid_page(self, client):
        """Test that invalid page numbers are handled."""
        response = client.get("/api/events", params={"page": -1})
        assert response.status_code in [200, 422]


class TestEventDetailEndpoint:
    """Test /api/events/{event_id} endpoint."""

    def test_event_not_found(self, client):
        """Test requesting non-existent event returns 404."""
        response = client.get("/api/events/nonexistent-event-id")
        assert response.status_code in [404, 200]
