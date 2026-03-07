from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.ai.market_context_builder import (
    CONTEXT_MODE_MINIMAL,
    build_market_analysis_context,
    filter_snapshots_for_context_mode,
    resolve_context_mode,
    sanitize_account_snapshot_for_ai,
    to_minimal_context,
)


def _snap(ts: datetime):
    history = []
    price = 100.0
    for i in range(60):
        price += 0.2 if i % 3 else -0.05
        history.append({
            "ts": ts - timedelta(minutes=(60 - i)),
            "open": price - 0.1,
            "high": price + 0.3,
            "low": price - 0.4,
            "close": price,
        })
    latest = {
        "ts": ts,
        "close": history[-1]["close"],
        "atr_14": 1.2,
        "bb_bandwidth": 0.05,
        "rsi_14": 58.0,
        "stoch_rsi_k": 64.0,
        "stoch_rsi_d": 61.0,
        "macd_hist": 0.12,
        "obv": 12345.0,
        "volume_zscore": 2.1,
        "ema_ribbon_trend": "UP",
    }
    return {"latest": latest, "history": history}


def _snapshots(now: datetime):
    return {
        "4h": _snap(now - timedelta(minutes=2)),
        "1h": _snap(now - timedelta(minutes=2)),
        "15m": _snap(now - timedelta(minutes=1)),
        "5m": _snap(now - timedelta(seconds=50)),
        "1m": _snap(now - timedelta(seconds=20)),
    }


def test_market_context_builder_handles_missing_youtube_without_crashing():
    now = datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=30), "last_funding_rate": 0.0001, "open_interest": 1000},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )

    assert ctx["youtube_radar"]["available"] is False
    assert ctx["data_quality"]["youtube_stale"] is True
    assert ctx["brief"]["symbol"] == "BTCUSDT"
    assert "tradeable_gate" in ctx["brief"]
    assert isinstance(ctx["decision_ts"], int)
    assert ctx["valid_until_utc"] >= int(now.timestamp())


def test_market_context_builder_clips_youtube_radar_deterministically():
    now = datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc)
    huge_text = "x" * 5000
    consensus_row = SimpleNamespace(
        created_at=now,
        source_video_ids=["v1", "v2", "v3"],
        consensus_json={
            "consensus_bias": "BULL",
            "confidence": 78,
            "key_levels": {"support": [100, 99, 98, 97], "resistance": [101, 102, 103, 104]},
            "scenarios": [
                {"then": huge_text, "risk": huge_text},
                {"then": huge_text, "risk": huge_text},
                {"then": huge_text, "risk": huge_text},
            ],
            "disagreements": [{"analyst": "A", "view": huge_text}, {"analyst": "B", "view": huge_text}],
        },
    )
    insight = SimpleNamespace(
        analyst_view_json={
            "meta": {"analyst": "Channel A"},
            "market_view": {"bias_1_7d": "BULL", "conviction": "HIGH", "one_liner": huge_text},
            "levels": {"support": [{"level": 99.5}]},
            "computed_weight": 0.9876,
        }
    )

    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[
            {"symbol": "BTCUSDT", "alert_type": "VOL", "severity": "high", "reason": huge_text, "ts": now.isoformat()}
            for _ in range(6)
        ],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=consensus_row,
        youtube_insights=[insight, insight],
        now=now,
    )

    meta = ctx["input_budget_meta"]
    assert meta["youtube_radar_chars_after_clip"] <= 2600
    assert isinstance(meta["clip_steps_applied"], list)
    assert len(ctx["youtube_radar"]["top_voices"]) <= 2
    levels = ctx["youtube_radar"]["consensus_levels"]
    assert len(levels["support"]) <= 3
    assert len(levels["resistance"]) <= 3
    assert len(ctx["alerts_digest"]["top_events"]) <= 3


def test_market_context_builder_marks_stale_consensus_with_newer_insights():
    now = datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc)
    consensus_row = SimpleNamespace(
        created_at=now - timedelta(hours=26),
        source_video_ids=["v1"],
        consensus_json={
            "consensus_bias": "STRONG_BEAR",
            "confidence": 72,
            "key_levels": {"support": [64700], "resistance": [66000]},
            "scenarios": [],
            "disagreements": [],
        },
    )
    fresh_insight = SimpleNamespace(
        created_at=now - timedelta(hours=3),
        analyst_view_json={
            "meta": {"analyst": "Channel X"},
            "market_view": {"bias_1_7d": "BEAR", "conviction": "HIGH", "one_liner": "test"},
            "levels": {"support": [{"level": 64000}]},
            "computed_weight": 0.5,
        },
    )

    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=consensus_row,
        youtube_insights=[fresh_insight],
        now=now,
    )

    yt = ctx["youtube_radar"]
    dq = ctx["data_quality"]
    assert yt["stale"] is True
    assert yt["fresh_content_after_consensus"] is True
    assert yt["latest_insight_at"] is not None
    assert dq["youtube_stale"] is True
    assert dq["youtube_has_newer_insights"] is True
    assert any("较新洞察未纳入共识" in n for n in dq["notes"])


def test_market_context_builder_marks_stale_higher_tf_as_degraded():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    snapshots = _snapshots(now)
    snapshots["1h"]["latest"]["ts"] = now - timedelta(hours=21)
    snapshots["4h"]["latest"]["ts"] = now - timedelta(hours=26)

    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=snapshots,
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )

    dq = ctx["data_quality"]
    assert "1h" in (dq.get("stale_timeframes") or [])
    assert "4h" in (dq.get("stale_timeframes") or [])
    assert dq["overall"] in {"DEGRADED", "POOR"}
    assert any("周期数据延迟" in n for n in dq["notes"])


def test_market_context_builder_injects_compact_account_snapshot():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        account_snapshot={
            "watch_symbol": "BTCUSDT",
            "as_of_utc": now.isoformat(),
            "futures": {
                "available_balance": 88.2,
                "total_margin_balance": 120.5,
                "position_amt": 0.01,
                "mark_price": 100.0,
                "liquidation_price": 90.0,
                "raw_dump": "x" * 5000,  # should be dropped by compact snapshot builder
            },
            "margin": {"margin_level": 1.35, "margin_call_bar": 1.1},
            "risk_flags": {"available_balance_low": True},
        },
        now=now,
    )

    account = ctx.get("account_snapshot") or {}
    assert account.get("watch_symbol") == "BTCUSDT"
    assert account.get("futures", {}).get("available_balance") == 88.2
    assert "raw_dump" not in (account.get("futures") or {})
    budget = ctx.get("input_budget_meta") or {}
    assert "account_snapshot_chars_before_clip" in budget
    assert "account_snapshot_chars_after_clip" in budget


def test_market_context_builder_uses_latest_market_ts_for_decision_window():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    snapshots = _snapshots(now)
    snapshots["1h"]["latest"]["ts"] = now - timedelta(hours=2)
    snapshots["1m"]["latest"]["ts"] = now - timedelta(seconds=30)

    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=snapshots,
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )

    assert ctx["decision_ts"] == int((now - timedelta(seconds=30)).timestamp())
    assert ctx["valid_until_utc"] >= int(now.timestamp())


def test_market_context_builder_filters_old_alerts_for_ai_context():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[
            {
                "symbol": "BTCUSDT",
                "alert_type": "ACCOUNT_LIQUIDATION_RISK",
                "severity": "CRITICAL",
                "reason": "old risk",
                "ts": (now - timedelta(hours=36)).isoformat(),
            },
            {
                "symbol": "BTCUSDT",
                "alert_type": "MOMENTUM",
                "severity": "HIGH",
                "reason": "fresh momentum",
                "ts": (now - timedelta(minutes=30)).isoformat(),
            },
        ],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )

    top_events = ctx["alerts_digest"]["top_events"]
    assert len(top_events) == 1
    assert top_events[0]["alert_type"] == "MOMENTUM"
    assert top_events[0]["score"] >= 0.9


def test_sanitize_account_snapshot_for_ai_drops_stale_zero_position():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sanitize_account_snapshot_for_ai(
        {
            "watch_symbol": "BTCUSDT",
            "as_of_utc": (now - timedelta(hours=27)).isoformat(),
            "futures": {
                "available_balance": 88.2,
                "total_margin_balance": 120.5,
                "position_amt": 0.0,
                "mark_price": 100.0,
                "liquidation_price": 90.0,
                "liq_distance_pct": 18.0,
            },
            "margin": {"margin_level": 1.5, "margin_call_bar": 1.1},
            "risk_flags": {"available_balance_low": False, "margin_near_call": False},
        },
        now=now,
    )
    assert snapshot == {}


def test_sanitize_account_snapshot_for_ai_keeps_stale_risky_summary():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    snapshot = sanitize_account_snapshot_for_ai(
        {
            "watch_symbol": "BTCUSDT",
            "as_of_utc": (now - timedelta(hours=5)).isoformat(),
            "futures": {
                "available_balance": 88.2,
                "total_margin_balance": 120.5,
                "position_amt": 0.02,
                "mark_price": 100.0,
                "liquidation_price": 98.0,
                "liq_distance_pct": 2.0,
            },
            "margin": {"margin_level": 1.05, "margin_call_bar": 1.1},
            "risk_flags": {"available_balance_low": False, "margin_near_call": True},
        },
        now=now,
    )
    assert snapshot["stale"] is True
    assert snapshot["futures"]["position_amt"] == 0.02
    assert snapshot["risk_flags"]["margin_near_call"] is True


def test_market_context_builder_minimal_mode():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[{"symbol": "BTCUSDT", "alert_type": "X", "severity": "high", "reason": "test", "ts": now.isoformat()}],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
        context_mode=CONTEXT_MODE_MINIMAL,
    )
    assert ctx["youtube_radar"]["available"] is False
    assert ctx["intel_digest"] == {}
    assert ctx["account_snapshot"] == {}
    assert ctx["alerts_digest"]["top_events"] == []
    assert "minimal_context" in (ctx.get("input_budget_meta") or {}).get("clip_steps_applied", [])
    assert set(ctx["brief"]["timeframes"]) <= {"1m", "5m"}
    assert "tradeable_gate" in ctx["brief"]
    assert "funding_deltas" in ctx
    assert "data_quality" in ctx


def test_resolve_context_mode():
    assert resolve_context_mode(
        data_quality={"overall": "GOOD"},
        tradeable_gate={"tradeable": True},
        min_context_on_poor_data=True,
        min_context_on_non_tradeable=True,
    ) == "full"
    assert resolve_context_mode(
        data_quality={"overall": "POOR"},
        tradeable_gate={"tradeable": True},
        min_context_on_poor_data=True,
        min_context_on_non_tradeable=True,
    ) == "minimal"
    assert resolve_context_mode(
        data_quality={"overall": "GOOD"},
        tradeable_gate={"tradeable": False},
        min_context_on_poor_data=True,
        min_context_on_non_tradeable=True,
    ) == "minimal"
    assert resolve_context_mode(
        data_quality={"overall": "POOR"},
        tradeable_gate={"tradeable": False},
        min_context_on_poor_data=False,
        min_context_on_non_tradeable=False,
    ) == "full"


def test_filter_snapshots_for_context_mode():
    snapshots = {"4h": {}, "1h": {}, "15m": {}, "5m": {}, "1m": {}}
    minimal = filter_snapshots_for_context_mode(snapshots, CONTEXT_MODE_MINIMAL)
    assert set(minimal.keys()) == {"5m", "1m"}
    full = filter_snapshots_for_context_mode(snapshots, "full")
    assert full == snapshots


def test_to_minimal_context():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    full = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )
    minimal = to_minimal_context(full, symbol="BTCUSDT", snapshots=_snapshots(now))
    assert minimal["youtube_radar"]["available"] is False
    assert minimal["intel_digest"] == {}
    assert minimal["account_snapshot"] == {}
    assert "minimal_context" in (minimal.get("input_budget_meta") or {}).get("clip_steps_applied", [])
