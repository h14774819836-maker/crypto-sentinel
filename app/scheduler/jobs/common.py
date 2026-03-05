"""Shared utilities for scheduler jobs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.utils.time import ensure_utc


def _epoch_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    dt_utc = ensure_utc(dt).astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _bjt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    bjt = ensure_utc(dt).astimezone(timezone(timedelta(hours=8)))
    return bjt.isoformat()


def _liquidation_distance_pct(mark_price: float, liq_price: float, position_amt: float) -> float | None:
    if mark_price <= 0 or liq_price <= 0 or position_amt == 0:
        return None
    if position_amt > 0:
        distance = (mark_price - liq_price) / mark_price
    else:
        distance = (liq_price - mark_price) / mark_price
    if distance < 0:
        return None
    return distance * 100.0


def _is_effectively_equal(a: float | None, b: float | None, tolerance: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= max(0.0, float(tolerance))


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num != num:  # NaN
        return None
    return num


def _clamp_int(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))
