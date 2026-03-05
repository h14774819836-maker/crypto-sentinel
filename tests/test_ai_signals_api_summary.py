from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
import app.web.routers.api_market as api_market


def test_ai_signals_api_returns_analysis_summary_and_handles_null_analysis(monkeypatch):
    rows = [
        SimpleNamespace(
            symbol="BTCUSDT",
            direction="HOLD",
            entry_price=None,
            take_profit=None,
            stop_loss=None,
            confidence=40,
            reasoning="test",
            analysis_json=None,
            model_requested="m1",
            model_name="m1",
            created_at=datetime.now(timezone.utc),
        ),
        SimpleNamespace(
            symbol="ETHUSDT",
            direction="LONG",
            entry_price=100.0,
            take_profit=110.0,
            stop_loss=95.0,
            confidence=78,
            reasoning="test2",
            analysis_json={
                "validation": {
                    "status": "downgraded",
                    "warnings": ["w1"],
                    "downgrade_reason": "RR too low",
                    "rr": 1.2,
                    "sl_atr_multiple": 0.4,
                },
                "youtube_reflection": {"status": "conflicted"},
                "context_digest": {
                    "data_quality": {"overall": "DEGRADED"},
                    "input_budget_meta": {"alerts_digest_chars": 120},
                    "tradeable_gate": {"tradeable": False, "reasons": ["多周期不一致"]},
                },
                "risk": {"rr": 1.2},
            },
            model_requested="m2",
            model_name="m2",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    monkeypatch.setattr(api_market, "list_ai_signals", lambda db, limit=50, symbol=None: rows)
    client = TestClient(app)
    resp = client.get("/api/ai-signals")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 2

    first = data["items"][0]
    assert first["analysis_json"] is None
    assert first["analysis_summary"] is None
    assert first["validation_warnings"] is None

    second = data["items"][1]
    assert second["analysis_summary"]["validation_status"] == "downgraded"
    assert second["analysis_summary"]["warning_count"] == 1
    assert second["analysis_summary"]["youtube_reflection_status"] == "conflicted"
    assert second["analysis_summary"]["data_quality"]["overall"] == "DEGRADED"
    assert second["analysis_summary"]["tradeable_gate"]["tradeable"] is False
    assert second["validation_warnings"] == ["w1"]
