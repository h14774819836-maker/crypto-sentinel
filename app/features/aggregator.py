from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.repository import list_ohlcv_range, upsert_ohlcv
from app.logging import logger


def floor_utc_10m(ts: datetime) -> datetime:
    aligned = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return aligned.replace(minute=(aligned.minute // 10) * 10)


def _floor_utc_nm(ts: datetime, n: int) -> datetime:
    """Floor timestamp to the nearest N-minute boundary."""
    aligned = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return aligned.replace(minute=(aligned.minute // n) * n)


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def aggregate_nm_from_1m(session, symbol: str, bucket_start: datetime, n_minutes: int, commit: bool = True) -> bool:
    """Aggregate N-minute candle from 1m candles. Works for 5m, 10m, 15m, etc."""
    bucket_start = _floor_utc_nm(bucket_start, n_minutes)
    bucket_end = bucket_start + timedelta(minutes=n_minutes)
    rows = list_ohlcv_range(session, symbol, "1m", bucket_start, bucket_end)

    if len(rows) != n_minutes:
        return False

    expected = [_to_utc(bucket_start + timedelta(minutes=i)) for i in range(n_minutes)]
    timestamps = [_to_utc(row.ts) for row in rows]
    if timestamps != expected:
        logger.debug(
            "Skip %dm aggregate %s %s due to missing 1m continuity (%d/%d)",
            n_minutes, symbol, bucket_start, len(rows), n_minutes,
        )
        return False

    tf_label = f"{n_minutes}m"
    payload = {
        "symbol": symbol,
        "timeframe": tf_label,
        "ts": bucket_start,
        "open": rows[0].open,
        "high": max(c.high for c in rows),
        "low": min(c.low for c in rows),
        "close": rows[-1].close,
        "volume": sum(c.volume for c in rows),
        "source": "local_agg_1m",
    }
    upsert_ohlcv(session, payload, commit=commit)
    return True


def aggregate_10m_from_1m(session, symbol: str, bucket_start: datetime, commit: bool = True) -> bool:
    """Backward-compatible 10m aggregation."""
    return aggregate_nm_from_1m(session, symbol, bucket_start, 10, commit=commit)


def rebuild_nm_range(session, symbol: str, start_ts: datetime, end_ts: datetime, n_minutes: int) -> int:
    """Rebuild N-minute aggregated candles over a timestamp range."""
    if end_ts < start_ts:
        return 0
    current = _floor_utc_nm(start_ts, n_minutes)
    if current < start_ts:
        current += timedelta(minutes=n_minutes)

    count = 0
    while current <= end_ts:
        if aggregate_nm_from_1m(session, symbol, current, n_minutes, commit=False):
            count += 1
        current += timedelta(minutes=n_minutes)
    if count:
        session.commit()
    return count


def rebuild_10m_range(session, symbol: str, start_ts: datetime, end_ts: datetime) -> int:
    """Backward-compatible 10m rebuild."""
    return rebuild_nm_range(session, symbol, start_ts, end_ts, 10)
