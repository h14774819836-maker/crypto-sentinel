from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.repository import get_llm_calls, insert_ai_analysis_failure, insert_llm_call, list_ai_analysis_failures
from app.db.session import Base
from app.main import app
import app.web.views as views


def test_ai_analysis_failure_repository_insert_and_list():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    with SessionLocal() as db:
        insert_ai_analysis_failure(
            db,
            {
                "task": "market",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "attempt": 2,
                "phase": "grounding",
                "provider_name": "OpenAICompatibleProvider",
                "model_requested": "deepseek-reasoner",
                "model_actual": "deepseek-chat",
                "error_code": "grounding",
                "error_summary": "grounding mismatch",
                "details_json": {"errors": ["mismatch"]},
            },
            commit=True,
        )
        rows = list_ai_analysis_failures(db, limit=10, task="market", symbol="BTCUSDT")
        assert len(rows) == 1
        assert rows[0].phase == "grounding"
        assert rows[0].symbol == "BTCUSDT"


def test_llm_failures_api_returns_items(monkeypatch):
    now = datetime.now(timezone.utc)
    fake_rows = [
        SimpleNamespace(
            id=1,
            task="market",
            symbol="BTCUSDT",
            timeframe="1m",
            ts=now,
            attempt=1,
            phase="schema",
            provider_name="OpenAICompatibleProvider",
            model_requested="deepseek-reasoner",
            model_actual="deepseek-reasoner",
            error_code="schema",
            error_summary="missing anchors",
            raw_response_excerpt="{}",
            details_json={"errors": ["anchors 至少需要 2 条"]},
            created_at=now,
        )
    ]
    monkeypatch.setattr(views, "list_ai_analysis_failures", lambda db, limit=50, task="market", symbol=None: fake_rows)
    client = TestClient(app)
    resp = client.get("/api/llm/failures?task=market&symbol=BTCUSDT&limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert len(data["items"]) == 1
    assert data["items"][0]["phase"] == "schema"


def test_insert_llm_call_ignores_extra_fields():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    with SessionLocal() as db:
        insert_llm_call(
            db,
            {
                "task": "market",
                "provider_name": "OpenAICompatibleProvider",
                "model": "deepseek-chat",
                "status": "ok",
                "duration_ms": 12,
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "error_summary": None,
                "failure_events": [{"phase": "schema"}],
            },
            commit=True,
        )
        rows = get_llm_calls(db, limit=10, task="market")
        assert len(rows) == 1
