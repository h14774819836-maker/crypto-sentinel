from __future__ import annotations

import asyncio
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.ai.analyst as analyst_module
import app.ai.openai_provider as provider_module
import app.web.views as views
from app.db.models import AiSignal
from app.db.session import Base


class _FakeRequest:
    def __init__(self):
        self.calls = 0

    async def is_disconnected(self) -> bool:
        self.calls += 1
        # first probe connected, second probe disconnected
        return self.calls >= 2


def _setup_db():
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_ai_stream_disconnect_cancels_worker_and_rolls_back_symbol(monkeypatch):
    SessionLocal = _setup_db()
    monkeypatch.setattr(views, "SessionLocal", SessionLocal)
    monkeypatch.setattr(views, "settings", SimpleNamespace(watchlist_symbols=["BTCUSDT"]))

    request_opts = SimpleNamespace(
        llm_config=SimpleNamespace(enabled=True, api_key="k"),
        effective_model="fake-model",
    )
    monkeypatch.setattr(views, "_resolve_market_ai_request_options", lambda _model: request_opts)
    monkeypatch.setattr(views, "_build_recent_alerts_by_symbol", lambda _db, limit=60: {})
    monkeypatch.setattr(views, "_build_funding_current_by_symbol", lambda _db: {})
    monkeypatch.setattr(
        views,
        "_build_market_ai_symbol_inputs",
        lambda _db, _symbol, recent_alerts_by_symbol=None, funding_current_by_symbol=None: ({"1m": {"latest": {"close": 1}}}, {}),
    )

    monkeypatch.setattr(provider_module, "OpenAICompatibleProvider", lambda _cfg: object())

    class _FakeAnalyst:
        def __init__(self, *_args, **_kwargs):
            pass

        async def analyze(self, *_args, **_kwargs):
            await asyncio.sleep(1.2)
            return (
                [
                    SimpleNamespace(
                        symbol="BTCUSDT",
                        direction="LONG",
                        entry_price=100.0,
                        take_profit=101.0,
                        stop_loss=99.0,
                        confidence=75,
                        reasoning="fake",
                        analysis_json={"a": 1},
                        model_requested="fake-model",
                        model_name="fake-model",
                        prompt_tokens=10,
                        completion_tokens=20,
                    )
                ],
                None,
            )

    monkeypatch.setattr(analyst_module, "MarketAnalyst", _FakeAnalyst)
    monkeypatch.setattr(analyst_module, "attach_context_digest_to_analysis_json", lambda j, _ctx: j)

    async def _run():
        response = await views.ai_analyze_stream(_FakeRequest(), model=None)
        async for _chunk in response.body_iterator:
            pass

    asyncio.run(_run())

    with SessionLocal() as db:
        rows = list(db.scalars(select(AiSignal)))
    assert rows == []

