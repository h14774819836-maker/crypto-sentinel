from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AiSignal, StrategyDecision
from app.db.repository import write_decision_from_ai_signal


def compute_liq_price_est(
    *,
    entry_price: float | None,
    leverage: float | None,
    position_side: str,
    maintenance_margin_ratio: float = 0.005,
) -> float | None:
    if entry_price is None or leverage is None or leverage <= 0:
        return None
    side = str(position_side or "").upper()
    if side == "LONG":
        return float(entry_price) * (1.0 - (1.0 / float(leverage)) + maintenance_margin_ratio)
    if side == "SHORT":
        return float(entry_price) * (1.0 + (1.0 / float(leverage)) - maintenance_margin_ratio)
    return None


def write_decision(
    db: Session,
    *,
    ai_signal: AiSignal,
    facts_snapshot: dict[str, Any] | None = None,
    commit: bool = True,
) -> StrategyDecision | None:
    # facts_snapshot is reserved for richer risk/margin enrichment in later phases.
    _ = facts_snapshot
    decision = write_decision_from_ai_signal(db, ai_signal, commit=False)
    if decision and decision.liq_price_est is None:
        decision.liq_price_est = compute_liq_price_est(
            entry_price=decision.entry_price,
            leverage=decision.leverage,
            position_side=decision.position_side,
        )
    if commit:
        db.commit()
    return decision
