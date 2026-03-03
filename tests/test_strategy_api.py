from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
import app.web.views as views


def test_strategy_page_available():
    client = TestClient(app)
    resp = client.get("/strategy")
    assert resp.status_code == 200
    assert "Strategy Replay" in resp.text


def test_strategy_decisions_api_raw_and_densified(monkeypatch):
    monkeypatch.setattr(
        views,
        "list_strategy_decisions_raw",
        lambda *args, **kwargs: ([{"id": 1, "symbol": "BTCUSDT", "decision_ts": 1}], True, 1),
    )
    monkeypatch.setattr(
        views,
        "list_strategy_decisions_densified",
        lambda *args, **kwargs: [{"bucket_ts": 1, "count": 3, "tp": 1, "sl": 1, "ambiguous": 1}],
    )
    client = TestClient(app)

    raw = client.get("/api/strategy/decisions?symbol=BTCUSDT&from=0&to=100&mode=raw&limit=10")
    assert raw.status_code == 200
    raw_json = raw.json()
    assert raw_json["mode"] == "raw"
    assert raw_json["has_more"] is True
    assert raw_json["next_cursor"] == 1
    assert len(raw_json["items"]) == 1

    densified = client.get("/api/strategy/decisions?symbol=BTCUSDT&from=0&to=100&mode=densified")
    assert densified.status_code == 200
    den_json = densified.json()
    assert den_json["mode"] == "densified"
    assert den_json["has_more"] is False
    assert den_json["next_cursor"] is None
    assert len(den_json["items"]) == 1


def test_ohlcv_api_serializes_epoch(monkeypatch):
    ts = datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        views,
        "list_ohlcv_range",
        lambda *args, **kwargs: [
            SimpleNamespace(ts=ts, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0),
        ],
    )
    client = TestClient(app)
    resp = client.get("/api/ohlcv?symbol=BTCUSDT&timeframe=1m&from=1700000000&to=1700000600")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["ts"] == int(ts.timestamp())
