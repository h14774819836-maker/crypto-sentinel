from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.ai.analyst as analyst_module
import app.ai.openai_provider as provider_module
from app.web.routers import api_ai
from app.db.models import AiSignal, MarketMetric, Ohlcv
from app.db.session import Base


def _setup_db():
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def test_ai_stream_disconnect_cancels_worker_and_rolls_back_symbol(monkeypatch):
    SessionLocal = _setup_db()
    monkeypatch.setattr(api_ai, "SessionLocal", SessionLocal)
    monkeypatch.setattr(api_ai, "settings", SimpleNamespace(watchlist_symbols=["BTCUSDT"]))

    request_opts = SimpleNamespace(
        llm_config=SimpleNamespace(enabled=True, api_key="k"),
        effective_model="fake-model",
    )
    monkeypatch.setattr(api_ai, "_resolve_market_ai_request_options", lambda _model: request_opts)
    async def _fake_refresh():
        return {"ok": True}
    monkeypatch.setattr(api_ai, "_refresh_market_data_before_ai_analysis", _fake_refresh)
    monkeypatch.setattr(api_ai, "_build_recent_alerts_by_symbol", lambda _db, limit=60: {})
    monkeypatch.setattr(api_ai, "_build_funding_current_by_symbol", lambda _db: {})
    monkeypatch.setattr(
        api_ai,
        "_build_market_ai_symbol_inputs",
        lambda _db, _symbol, recent_alerts_by_symbol=None, funding_current_by_symbol=None: ({"1m": {"latest": {"close": 1}}}, {}),
    )

    monkeypatch.setattr(provider_module, "OpenAICompatibleProvider", lambda _cfg: object())

    class _FakeAnalyst:
        def __init__(self, *_args, **_kwargs):
            pass

        async def analyze(self, *_args, **_kwargs):
            await asyncio.sleep(2.5)
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
        response = await api_ai.ai_analyze_stream(model=None, symbol=None, _admin="dummy")
        async for _chunk in response.body_iterator:
            pass

    asyncio.run(_run())

    with SessionLocal() as db:
        rows = list(db.scalars(select(AiSignal)))
    assert len(rows) == 1


def test_ai_stream_serializes_datetime_in_signal_payload(monkeypatch):
    SessionLocal = _setup_db()
    monkeypatch.setattr(api_ai, "SessionLocal", SessionLocal)
    monkeypatch.setattr(api_ai, "settings", SimpleNamespace(watchlist_symbols=["BTCUSDT"]))

    request_opts = SimpleNamespace(
        llm_config=SimpleNamespace(enabled=True, api_key="k"),
        effective_model="fake-model",
    )
    monkeypatch.setattr(api_ai, "_resolve_market_ai_request_options", lambda _model: request_opts)

    async def _fake_refresh():
        return {"ok": True, "refreshed_at": datetime(2026, 3, 7, 6, 3, 46, tzinfo=timezone.utc)}

    monkeypatch.setattr(api_ai, "_refresh_market_data_before_ai_analysis", _fake_refresh)
    monkeypatch.setattr(api_ai, "_build_recent_alerts_by_symbol", lambda _db, limit=60: {})
    monkeypatch.setattr(api_ai, "_build_funding_current_by_symbol", lambda _db: {})
    monkeypatch.setattr(
        api_ai,
        "_build_market_ai_symbol_inputs",
        lambda _db, _symbol, recent_alerts_by_symbol=None, funding_current_by_symbol=None: ({"1m": {"latest": {"close": 1}}}, {}),
    )
    monkeypatch.setattr(provider_module, "OpenAICompatibleProvider", lambda _cfg: object())

    class _FakeAnalyst:
        def __init__(self, *_args, **_kwargs):
            pass

        async def analyze(self, *_args, **_kwargs):
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
                        analysis_json={"generated_at": datetime(2026, 3, 7, 6, 3, 46, tzinfo=timezone.utc)},
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
        response = await api_ai.ai_analyze_stream(model=None, symbol=None, _admin="dummy")
        chunks: list[str] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)
        return chunks

    chunks = asyncio.run(_run())
    payloads = [
        json.loads(chunk.removeprefix("data: ").strip())
        for chunk in chunks
        if isinstance(chunk, str) and chunk.startswith("data: ")
    ]

    done = next(item for item in payloads if item["type"] == "done")
    assert done["refresh"]["refreshed_at"] == "2026-03-07T06:03:46+00:00"

    encoded = json.loads(
        api_ai._json_dumps_safe(
            {
                "type": "symbol_done",
                "signals": [
                    {
                        "analysis_json": {
                            "generated_at": datetime(2026, 3, 7, 6, 3, 46, tzinfo=timezone.utc)
                        }
                    }
                ],
            }
        )
    )
    assert encoded["signals"][0]["analysis_json"]["generated_at"] == "2026-03-07T06:03:46+00:00"


def test_market_ai_symbol_snapshots_prefers_latest_ohlcv_ts(monkeypatch):
    SessionLocal = _setup_db()
    monkeypatch.setattr(api_ai, "settings", SimpleNamespace(multi_tf_interval_list=["5m"], ai_history_candles=5))

    metric_ts = datetime(2026, 3, 7, 1, 32, tzinfo=timezone.utc)
    candle_ts = datetime(2026, 3, 7, 5, 55, tzinfo=timezone.utc)

    with SessionLocal() as db:
        db.add(
            MarketMetric(
                symbol="BTCUSDT",
                timeframe="1m",
                ts=metric_ts,
                close=100.0,
                ret_1m=0.01,
            )
        )
        db.add(
            Ohlcv(
                symbol="BTCUSDT",
                timeframe="1m",
                ts=candle_ts,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=10.0,
                source="binance",
            )
        )
        db.commit()

        snapshots = api_ai._build_market_ai_symbol_snapshots(db, "BTCUSDT")

    latest = snapshots["1m"]["latest"]
    assert _as_utc(latest["ts"]) == candle_ts
    assert _as_utc(latest["candle_ts"]) == candle_ts
    assert _as_utc(latest["metric_ts"]) == metric_ts
    assert latest["close"] == 100.5
    assert latest["ret_1m"] == 0.01


def test_feature_refresh_with_catchup_repeats_until_backlog_cleared():
    calls: list[int] = []

    async def _fake_feature_job(_runtime):
        calls.append(1)
        backlog_values = [120, 40, 0]
        return {"rows_written": 60, "backlog": backlog_values[len(calls) - 1]}

    async def _run():
        steps: list[dict] = []
        result = await api_ai._run_feature_refresh_with_catchup(
            runtime=object(),
            steps=steps,
            feature_job_fn=_fake_feature_job,
            max_runs=5,
        )
        return steps, result

    steps, result = asyncio.run(_run())
    assert len(calls) == 3
    assert [step["step"] for step in steps] == ["feature", "feature_catchup_2", "feature_catchup_3"]
    assert result == {"rows_written": 60, "backlog": 0}
