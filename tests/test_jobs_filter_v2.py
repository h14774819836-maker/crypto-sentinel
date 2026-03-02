from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.db.repository import insert_ai_signal
from app.scheduler.jobs import (
    _adaptive_enter_persist_bars,
    _adaptive_exit_threshold,
    _anomaly_extreme_bypass_data_ok,
    _classify_ai_data_freshness,
    _dedup_ai_signal_v2,
    _escalate_min_interval_seconds,
    _evaluate_ai_signal_gate_v2,
    _pick_ai_gate_atr_ref,
    _resolve_signal_prices_for_filter,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    with SessionLocal() as db:
        yield db


def _snapshot(*, regime: str, observations: dict, ts: datetime | None = None):
    return SimpleNamespace(
        regime=regime,
        observations=observations,
        ts=ts or datetime.now(timezone.utc),
    )


def _settings(**overrides):
    defaults = {
        "anomaly_persist_enter_bars": 2,
        "anomaly_hysteresis_exit_delta": 12,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _candle(ts: datetime):
    return SimpleNamespace(ts=ts)


def _symbol_data(now: datetime, *, age_1m: int | None, age_5m: int | None = None, age_15m: int | None = None, atr_1m: float | None = None, atr_5m: float | None = None, atr_15m: float | None = None):
    def _latest(age: int | None, atr: float | None):
        if age is None and atr is None:
            return {}
        data = {}
        if age is not None:
            data["ts"] = now - timedelta(seconds=age)
        if atr is not None:
            data["atr_14"] = atr
        return {"latest": data}

    return {
        "1m": _latest(age_1m, atr_1m),
        "5m": _latest(age_5m, atr_5m),
        "15m": _latest(age_15m, atr_15m),
    }


def test_anomaly_adaptive_persist_bars_and_exit_threshold():
    settings = _settings(anomaly_persist_enter_bars=2, anomaly_hysteresis_exit_delta=12)

    volatile = _snapshot(regime="VOLATILE", observations={"vol_percentile": 0.98})
    ranging_squeeze = _snapshot(regime="RANGING", observations={"vol_percentile": 0.15})
    missing_obs = _snapshot(regime="NEUTRAL", observations={})

    assert _adaptive_enter_persist_bars(volatile, settings) == 1
    assert _adaptive_enter_persist_bars(ranging_squeeze, settings) == 3
    assert _adaptive_enter_persist_bars(missing_obs, settings) == 2

    enter_threshold = 80
    exit_volatile = _adaptive_exit_threshold(volatile, enter_threshold, settings)
    exit_ranging = _adaptive_exit_threshold(ranging_squeeze, enter_threshold, settings)
    assert exit_volatile < exit_ranging


def test_anomaly_escalate_interval_and_extreme_bypass_data_quality():
    assert _escalate_min_interval_seconds(98, 92) == 60
    assert _escalate_min_interval_seconds(96, 92) == 90
    assert _escalate_min_interval_seconds(94, 92) == 120
    assert _escalate_min_interval_seconds(92, 92) == 180

    now = datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc)
    candles_ok = [_candle(now - timedelta(minutes=9 - i)) for i in range(10)]
    ok, detail = _anomaly_extreme_bypass_data_ok(_snapshot(regime="VOLATILE", observations={}, ts=now - timedelta(seconds=30)), candles_ok, now)
    assert ok is True
    assert detail["sample_size"] == 10

    candles_gap = candles_ok[:-1] + [_candle(now)]
    candles_gap[-2] = _candle(now - timedelta(minutes=4))
    ok_gap, detail_gap = _anomaly_extreme_bypass_data_ok(_snapshot(regime="VOLATILE", observations={}, ts=now - timedelta(seconds=30)), candles_gap, now)
    assert ok_gap is False
    assert detail_gap["max_gap_sec"] > 150


def test_ai_helper_atr_freshness_and_price_resolution():
    now = datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc)
    symbol_data = _symbol_data(now, age_1m=60, age_5m=300, age_15m=600, atr_1m=5.0, atr_5m=12.0, atr_15m=20.0)
    atr, tf = _pick_ai_gate_atr_ref(symbol_data)
    assert (atr, tf) == (12.0, "5m")

    soft = _classify_ai_data_freshness(_symbol_data(now, age_1m=241, age_5m=600, age_15m=1200), now)
    hard = _classify_ai_data_freshness(_symbol_data(now, age_1m=500, age_5m=2000, age_15m=4000), now)
    assert soft["action"] == "downgrade"
    assert soft["reason_category"] == "stale_soft"
    assert hard["action"] == "skip"
    assert hard["reason_category"] == "stale_hard"

    sig = SimpleNamespace(
        direction="LONG",
        entry_price=100.0,
        take_profit=None,
        stop_loss=99.0,
        analysis_json={"signal": {"take_profit_levels": [101.5, 103.0]}},
    )
    resolved = _resolve_signal_prices_for_filter(sig)
    assert resolved["tp"] == 101.5

    sig_levels_only = SimpleNamespace(
        direction="LONG",
        entry_price=None,
        take_profit=None,
        stop_loss=None,
        analysis_json={"signal": {}, "levels": {"supports": [95], "resistances": [105]}},
    )
    resolved_levels_only = _resolve_signal_prices_for_filter(sig_levels_only)
    assert resolved_levels_only["supports_res_only"] is True


def test_ai_gate_v2_downgrade_and_skip_cases():
    now = datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc)

    rr_low_sig = SimpleNamespace(
        symbol="BTCUSDT",
        direction="LONG",
        confidence=80,
        entry_price=100.0,
        take_profit=101.0,
        stop_loss=99.0,
        market_regime="RANGING",
        analysis_json={},
    )
    rr_low_decision = _evaluate_ai_signal_gate_v2(
        sig=rr_low_sig,
        symbol_data=_symbol_data(now, age_1m=60, age_5m=300, age_15m=900, atr_5m=1.0),
        now=now,
        threshold=70,
    )
    assert rr_low_decision.action == "downgrade"
    assert rr_low_decision.reason_category == "rr_low"

    stale_sig = SimpleNamespace(
        symbol="BTCUSDT",
        direction="SHORT",
        confidence=80,
        entry_price=100.0,
        take_profit=97.0,
        stop_loss=101.0,
        market_regime="TRENDING",
        analysis_json={},
    )
    stale_soft_decision = _evaluate_ai_signal_gate_v2(
        sig=stale_sig,
        symbol_data=_symbol_data(now, age_1m=300, age_5m=600, age_15m=1500, atr_5m=1.0),
        now=now,
        threshold=70,
    )
    assert stale_soft_decision.action == "downgrade"
    assert stale_soft_decision.reason_category == "stale_soft"

    bad_dir_sig = SimpleNamespace(
        symbol="BTCUSDT",
        direction="LONG",
        confidence=80,
        entry_price=100.0,
        take_profit=99.0,
        stop_loss=101.0,
        market_regime="TRENDING",
        analysis_json={},
    )
    bad_dir_decision = _evaluate_ai_signal_gate_v2(
        sig=bad_dir_sig,
        symbol_data=_symbol_data(now, age_1m=60, age_5m=600, age_15m=1500, atr_5m=2.0),
        now=now,
        threshold=70,
    )
    assert bad_dir_decision.action == "skip"
    assert bad_dir_decision.reason_category == "direction_inconsistent"

    levels_only_sig = SimpleNamespace(
        symbol="BTCUSDT",
        direction="LONG",
        confidence=82,
        entry_price=None,
        take_profit=None,
        stop_loss=None,
        market_regime="RANGING",
        analysis_json={"signal": {}, "levels": {"supports": [95.0], "resistances": [105.0]}},
    )
    levels_only_decision = _evaluate_ai_signal_gate_v2(
        sig=levels_only_sig,
        symbol_data=_symbol_data(now, age_1m=60, age_5m=600, age_15m=1500, atr_5m=2.0),
        now=now,
        threshold=70,
    )
    assert levels_only_decision.action == "downgrade"
    assert levels_only_decision.reason_category == "missing_prices"


def test_ai_dedup_v2_price_bucket_and_observation_reason(session):
    now = datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc)
    insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "ts": now - timedelta(minutes=1),
            "direction": "LONG",
            "entry_price": 100.05,
            "take_profit": 102.0,
            "stop_loss": 99.0,
            "confidence": 80,
            "reasoning": "test",
            "sent_to_telegram": True,
            "created_at": now - timedelta(seconds=60),
            "analysis_json": {"delivery": {"gate_version": "v2", "reason_category": None}},
        },
    )
    sig = SimpleNamespace(symbol="BTCUSDT", direction="LONG")
    dup, debug = _dedup_ai_signal_v2(
        session,
        sig=sig,
        timeframe="1m",
        now=now,
        cooldown_seconds=600,
        entry_price=100.06,
        atr_ref=1.0,
        is_observation=False,
        reason_category=None,
    )
    assert dup is True
    assert debug["mode"] == "price_bucket"

    insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "ts": now - timedelta(minutes=2),
            "direction": "LONG",
            "entry_price": None,
            "take_profit": None,
            "stop_loss": None,
            "confidence": 60,
            "reasoning": "observe",
            "sent_to_telegram": True,
            "created_at": now - timedelta(seconds=120),
            "analysis_json": {"delivery": {"gate_version": "v2", "reason_category": "rr_low"}},
        },
    )
    dup_obs, _ = _dedup_ai_signal_v2(
        session,
        sig=sig,
        timeframe="1m",
        now=now,
        cooldown_seconds=600,
        entry_price=None,
        atr_ref=1.0,
        is_observation=True,
        reason_category="rr_low",
    )
    assert dup_obs is True

    dup_obs_other, _ = _dedup_ai_signal_v2(
        session,
        sig=sig,
        timeframe="1m",
        now=now,
        cooldown_seconds=600,
        entry_price=None,
        atr_ref=1.0,
        is_observation=True,
        reason_category="stale_soft",
    )
    assert dup_obs_other is False

