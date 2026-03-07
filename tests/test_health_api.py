from __future__ import annotations

import sqlite3
import time

from fastapi.testclient import TestClient

from app.main import app
from app.web import views


def test_health_endpoint_returns_fast_on_sqlite_operational_error(monkeypatch):
    monkeypatch.setattr(views.settings, "database_url", "sqlite:///./data/locked_test.db")

    class _NoopCursor:
        def fetchone(self):
            return None

    class _LockedConnection:
        def execute(self, sql, *_args):
            if "SELECT 1" in sql:
                raise sqlite3.OperationalError("database is locked")
            return _NoopCursor()

        def close(self):
            return None

    monkeypatch.setattr(views.sqlite3, "connect", lambda *_args, **_kwargs: _LockedConnection())

    client = TestClient(app)
    start = time.perf_counter()
    response = client.get("/api/health")
    elapsed = time.perf_counter() - start

    assert response.status_code == 200
    payload = response.json()
    assert payload["api_ok"] is True
    assert payload["db_ok"] is False
    assert "asr" in payload
    assert payload["asr"]["status"] in {"disabled", "ready", "degraded"}
    assert elapsed < 1.0
