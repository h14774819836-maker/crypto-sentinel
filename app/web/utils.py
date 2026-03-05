"""Shared utilities for web routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _epoch_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _datetime_from_epoch(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _json_datetime(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def format_bj_time(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime in Beijing time."""
    if not dt:
        return "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    bj_dt = dt.astimezone(timezone(timedelta(hours=8)))
    return bj_dt.strftime(fmt)


def _to_utc_or_none(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _parse_utc_datetime(raw: Any) -> datetime | None:
    if hasattr(raw, "year"):  # datetime-like
        return _to_utc_or_none(raw)
    if not isinstance(raw, str):
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _to_utc_or_none(dt)
