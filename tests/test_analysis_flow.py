"""Tests for app.ai.analysis_flow."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.ai.analysis_flow import (
    build_scanner_hold_signal,
    prepare_context_and_snapshots,
    scanner_gate_passes,
)
from app.ai.market_context_builder import build_market_analysis_context


def _snapshots(now: datetime):
    return {
        "4h": {"latest": {"ts": now - timedelta(minutes=2), "close": 100.0}, "history": []},
        "1h": {"latest": {"ts": now - timedelta(minutes=2), "close": 100.0}, "history": []},
        "15m": {"latest": {"ts": now - timedelta(minutes=1), "close": 100.0}, "history": []},
        "5m": {"latest": {"ts": now - timedelta(seconds=50), "close": 100.0}, "history": []},
        "1m": {"latest": {"ts": now - timedelta(seconds=20), "close": 100.0}, "history": []},
    }


def _build_ctx(alignment_score: int = 80):
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )
    (ctx.get("brief") or {}).setdefault("cross_tf_summary", {})["alignment_score"] = alignment_score
    return ctx


def test_scanner_gate_passes_when_disabled():
    ctx = _build_ctx(alignment_score=30)
    ok, reason = scanner_gate_passes(ctx, two_stage_enabled=False, scan_threshold=60)
    assert ok is True
    assert reason is None


def test_scanner_gate_rejects_low_alignment():
    ctx = _build_ctx(alignment_score=30)
    ok, reason = scanner_gate_passes(ctx, two_stage_enabled=True, scan_threshold=60)
    assert ok is False
    assert reason == "alignment_low_30"


def test_scanner_gate_passes_high_alignment():
    ctx = _build_ctx(alignment_score=80)
    ok, reason = scanner_gate_passes(ctx, two_stage_enabled=True, scan_threshold=60)
    assert ok is True
    assert reason is None


def test_build_scanner_hold_signal():
    ctx = _build_ctx()
    sig = build_scanner_hold_signal("BTCUSDT", ctx, "alignment_low_40")
    assert sig.symbol == "BTCUSDT"
    assert sig.direction == "HOLD"
    assert sig.entry_price is None
    assert sig.confidence == 35
    assert "Scanner Gate" in sig.reasoning
    assert sig.model_name == "scanner_gate"
    assert sig.analysis_json["validation"]["status"] == "scanner_skip"


def test_prepare_context_and_snapshots_full():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    snapshots = _snapshots(now)
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
    ctx["data_quality"] = {"overall": "GOOD"}
    ctx.setdefault("brief", {})["tradeable_gate"] = {"tradeable": True, "reasons": []}
    out_ctx, out_snap = prepare_context_and_snapshots(
        ctx, snapshots, "BTCUSDT",
        min_context_on_poor_data=True,
        min_context_on_non_tradeable=True,
    )
    assert out_ctx is ctx
    assert out_snap is snapshots
    assert len(out_snap) == 5


def test_prepare_context_and_snapshots_minimal_on_poor():
    now = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    ctx = build_market_analysis_context(
        symbol="BTCUSDT",
        snapshots=_snapshots(now),
        recent_alerts=[],
        funding_current={"ts": now - timedelta(minutes=20), "last_funding_rate": 0.0002, "open_interest": 1200},
        funding_history=[],
        youtube_consensus=None,
        youtube_insights=None,
        now=now,
    )
    ctx["data_quality"] = {"overall": "POOR"}
    out_ctx, out_snap = prepare_context_and_snapshots(
        ctx, _snapshots(now), "BTCUSDT",
        min_context_on_poor_data=True,
        min_context_on_non_tradeable=True,
    )
    assert out_ctx["youtube_radar"]["available"] is False
    assert out_ctx["intel_digest"] == {}
    assert set(out_snap.keys()) <= {"5m", "1m"}
