from __future__ import annotations

from fastapi.testclient import TestClient

import app.config as config
from app.main import app
from app.web.routers import api_ai


TEST_ADMIN_TOKEN = "test-secret-token-12345"


def _set_admin_token(monkeypatch, token: str = TEST_ADMIN_TOKEN) -> None:
    monkeypatch.setenv("ADMIN_TOKEN", token)
    config.get_settings.cache_clear()


def test_ai_stream_last_error_endpoint_returns_recorded_backend_failure(monkeypatch):
    _set_admin_token(monkeypatch)
    api_ai._clear_ai_stream_error(model="fake-model", symbol="BTCUSDT")
    api_ai._record_ai_stream_error(
        model="fake-model",
        symbol="BTCUSDT",
        error="Analysis Error: upstream connection reset",
        phase="worker",
    )

    client = TestClient(app)
    headers = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}
    response = client.get(
        "/api/ai-analyze/last-error?model=fake-model&symbol=BTCUSDT",
        headers=headers,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["error"] == "Analysis Error: upstream connection reset"
    assert payload["item"]["phase"] == "worker"
