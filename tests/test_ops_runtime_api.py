from __future__ import annotations

from fastapi.testclient import TestClient

import app.config as config
from app.main import app
from app.web.routes import api_ops


TEST_ADMIN_TOKEN = "test-secret-token-12345"


def _set_admin_token(monkeypatch, token: str = TEST_ADMIN_TOKEN) -> None:
    monkeypatch.setenv("ADMIN_TOKEN", token)
    config.get_settings.cache_clear()


def test_ops_runtime_requires_admin_token(monkeypatch):
    _set_admin_token(monkeypatch)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/api/ops/runtime")

    assert response.status_code == 403


def test_ops_runtime_shutdown_requests_stop_signal(monkeypatch):
    _set_admin_token(monkeypatch)
    runtime_state = {
        "mode": "single_worker",
        "host": "127.0.0.1",
        "port": 8000,
        "supervisor_pid": 1234,
    }
    captured: list[dict[str, object]] = []

    monkeypatch.setattr(api_ops, "read_runtime_state", lambda: runtime_state)
    monkeypatch.setattr(
        api_ops,
        "request_runtime_stop",
        lambda **kwargs: captured.append(kwargs) or {
            "requested_by": kwargs["requested_by"],
            "reason": kwargs["reason"],
            "delay_seconds": kwargs["delay_seconds"],
        },
    )

    client = TestClient(app)
    headers = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}
    response = client.post(
        "/api/ops/runtime/shutdown",
        headers=headers,
        json={"reason": "ui_stop_button", "delay_seconds": 1.25},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["runtime"]["mode"] == "single_worker"
    assert captured == [{
        "reason": "ui_stop_button",
        "requested_by": "api",
        "delay_seconds": 1.25,
    }]
