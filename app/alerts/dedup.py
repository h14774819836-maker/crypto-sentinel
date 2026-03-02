from __future__ import annotations

from datetime import datetime

from app.db.repository import recent_alert_exists, recent_ai_signal_exists


def should_emit(session, symbol: str, alert_type: str, cooldown_seconds: int) -> bool:
    return not recent_alert_exists(session, symbol=symbol, alert_type=alert_type, cooldown_seconds=cooldown_seconds)


def should_emit_ai_signal(
    session,
    symbol: str,
    direction: str,
    entry_price: float | None,
    timeframe: str,
    cooldown_seconds: int
) -> bool:
    entry_bucket = round(entry_price, 3) if entry_price is not None else None
    
    return not recent_ai_signal_exists(
        session,
        symbol=symbol,
        direction=direction,
        entry_bucket=entry_bucket,
        timeframe=timeframe,
        cooldown_seconds=cooldown_seconds
    )


def minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)
