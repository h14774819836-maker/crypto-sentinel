from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.ai.market_context_builder import build_market_analysis_context


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
