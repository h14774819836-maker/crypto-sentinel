from __future__ import annotations

from datetime import datetime, timedelta, timezone


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_day_bounds(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    now_utc = ensure_utc(now_utc or utc_now())
    start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

