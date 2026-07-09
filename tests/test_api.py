import os

import httpx
import pytest

API_BASE = os.getenv("API_BASE", "http://localhost:8000")


def _api_available():
    try:
        response = httpx.get(f"{API_BASE}/health", timeout=3.0)
        return response.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _api_available(), reason="API FastAPI non disponible sur localhost:8000"
)


def test_health_endpoint():
    response = httpx.get(f"{API_BASE}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["database"] == "connected"


def test_team_performance_endpoint():
    response = httpx.get(f"{API_BASE}/api/v1/team-performance", params={"limit": 5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] > 0
    assert "team" in payload["data"][0]


def test_tournament_stats_endpoint():
    response = httpx.get(f"{API_BASE}/api/v1/tournament-stats")
    assert response.status_code == 200
    assert response.json()["count"] > 0


def test_predictions_2026_endpoint():
    response = httpx.get(f"{API_BASE}/api/v1/predictions-2026", params={"limit": 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 10
    assert "predicted_winner" in payload["data"][0]


def test_top_teams_endpoint():
    response = httpx.get(f"{API_BASE}/api/v1/top-teams", params={"limit": 3})
    assert response.status_code == 200
    assert response.json()["count"] == 3


def test_metrics_endpoint():
    response = httpx.get(f"{API_BASE}/metrics")
    assert response.status_code == 200
    assert "fastapi_requests_total" in response.text
