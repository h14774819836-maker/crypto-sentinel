"""Shared analysis flow helpers: scanner gate, hold signal, context preparation."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ai.analyst import AiTradeSignal, attach_context_digest_to_analysis_json
from app.ai.market_context_builder import (
    CONTEXT_MODE_MINIMAL,
    filter_snapshots_for_context_mode,
    resolve_context_mode,
    to_minimal_context,
)


def scanner_gate_passes(
    context: dict[str, Any],
    *,
    two_stage_enabled: bool = True,
    scan_threshold: int = 60,
    skip_on_poor_data: bool = True,
    skip_on_non_tradeable: bool = True,
) -> tuple[bool, str | None]:
    """Return (passes, skip_reason) for the pre-LLM gate."""
    data_quality = context.get("data_quality") or {}
    if skip_on_poor_data and str(data_quality.get("overall") or "").upper() == "POOR":
        return False, "poor_data"

    brief = context.get("brief") or {}
    tradeable_gate = brief.get("tradeable_gate") or {}
    if skip_on_non_tradeable and not bool(tradeable_gate.get("tradeable", True)):
        return False, "not_tradeable"

    if not two_stage_enabled:
        return True, None

    cross = brief.get("cross_tf_summary") or {}
    alignment = int(cross.get("alignment_score", 0) or 0)
    if alignment < scan_threshold:
        return False, f"alignment_low_{alignment}"
    return True, None


def build_scanner_hold_signal(
    symbol: str,
    context: dict[str, Any],
    skip_reason: str,
) -> AiTradeSignal:
    """Build synthetic HOLD signal when the pre-LLM gate rejects."""
    gate_label = "Scanner Gate" if str(skip_reason or "").startswith("alignment_low_") else "Pre-LLM Gate"
    reasoning = f"{gate_label} failed ({skip_reason}); forced HOLD."
    analysis_json: dict[str, Any] = {
        "market_regime": "uncertain",
        "signal": {
            "symbol": symbol.upper(),
            "direction": "HOLD",
            "entry_price": None,
            "take_profit": None,
            "stop_loss": None,
            "confidence": 35,
            "reasoning": reasoning,
        },
        "trade_plan": {
            "market_type": "futures",
            "margin_mode": None,
            "leverage": None,
            "capital_alloc_usdt": None,
            "entry_mode": "market",
            "entry_price": None,
            "take_profit": None,
            "stop_loss": None,
            "expiration_ts_utc": int(datetime.now(timezone.utc).timestamp()) + 3600,
            "max_hold_bars": 60,
            "liq_price_est": None,
            "fees_bps_assumption": None,
            "slippage_bps_assumption": None,
        },
        "meta": {
            "base_timeframe": "1m",
            "confidence": 0.35,
            "reason_brief": f"{gate_label}: {skip_reason}",
            "regime_calc_mode": "online",
        },
        "evidence": [],
        "anchors": [],
        "levels": {"supports": [], "resistances": []},
        "risk": {"rr": None, "sl_atr_multiple": None, "invalidations": []},
        "scenarios": {"base": "", "bull": "", "bear": ""},
        "validation_notes": [],
        "youtube_reflection": {},
        "validation": {"status": "scanner_skip", "stage": "scan", "skip_reason": skip_reason},
    }
    analysis_json = attach_context_digest_to_analysis_json(analysis_json, context) or analysis_json
    return AiTradeSignal(
        symbol=symbol.upper(),
        direction="HOLD",
        entry_price=None,
        take_profit=None,
        stop_loss=None,
        confidence=35,
        reasoning=reasoning,
        model_requested=None,
        model_name="scanner_gate",
        market_regime="uncertain",
        analysis_json=analysis_json,
        validation_warnings=["scanner_skip"],
    )


def prepare_context_and_snapshots(
    context: dict[str, Any],
    snapshots: dict[str, dict[str, Any]],
    symbol: str,
    *,
    min_context_on_poor_data: bool = True,
    min_context_on_non_tradeable: bool = True,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Apply minimal context when POOR or non-tradeable. Returns (context, snapshots)."""
    context_mode = resolve_context_mode(
        data_quality=context.get("data_quality") or {},
        tradeable_gate=(context.get("brief") or {}).get("tradeable_gate"),
        min_context_on_poor_data=min_context_on_poor_data,
        min_context_on_non_tradeable=min_context_on_non_tradeable,
    )
    if context_mode == CONTEXT_MODE_MINIMAL:
        context = to_minimal_context(context, symbol=symbol, snapshots=snapshots)
        snapshots = filter_snapshots_for_context_mode(snapshots, context_mode)
    return context, snapshots
