from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.models import MarketMetric
from app.db.repository import upsert_ohlcv
from app.db.session import Base
from app.features.feature_pipeline import compute_and_store_pending_metrics


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return SessionLocal()


def _insert_ohlcv(db, symbol: str, timeframe: str, start: datetime, n: int, step_minutes: int = 1):
    for i in range(n):
        ts = start + timedelta(minutes=i * step_minutes)
        upsert_ohlcv(
            db,
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "ts": ts,
                "open": 100 + i,
                "high": 100.5 + i,
                "low": 99.5 + i,
                "close": 100.2 + i,
                "volume": 10 + i,
                "source": "test",
            },
            commit=False,
        )
    db.commit()


def test_incremental_feature_job_skips_when_no_new_bars():
    with _session() as db:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        _insert_ohlcv(db, "BTCUSDT", "1m", start, 80, step_minutes=1)

        first = compute_and_store_pending_metrics(
            db,
            symbol="BTCUSDT",
            timeframe="1m",
            lookback_rows=80,
            max_pending_bars=20,
            max_batches=3,
        )
        assert first.processed_rows >= 1

        second = compute_and_store_pending_metrics(
            db,
            symbol="BTCUSDT",
            timeframe="1m",
            lookback_rows=80,
            max_pending_bars=20,
            max_batches=3,
        )
        assert second.processed_rows == 0
        assert second.pending_after_run == 0


def test_incremental_feature_job_processes_only_new_rows():
    with _session() as db:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        _insert_ohlcv(db, "BTCUSDT", "1m", start, 60, step_minutes=1)

        compute_and_store_pending_metrics(db, symbol="BTCUSDT", timeframe="1m", lookback_rows=60)
        before = list(db.scalars(select(MarketMetric).where(MarketMetric.symbol == "BTCUSDT", MarketMetric.timeframe == "1m")))

        # Append two new 1m candles.
        _insert_ohlcv(db, "BTCUSDT", "1m", start + timedelta(minutes=60), 2, step_minutes=1)
        result = compute_and_store_pending_metrics(
            db,
            symbol="BTCUSDT",
            timeframe="1m",
            lookback_rows=60,
            max_pending_bars=20,
            max_batches=3,
        )
        assert result.processed_rows == 2

        after = list(db.scalars(select(MarketMetric).where(MarketMetric.symbol == "BTCUSDT", MarketMetric.timeframe == "1m")))
        assert len(after) >= len(before) + 2


def test_incremental_feature_runs_per_timeframe_independently():
    with _session() as db:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        _insert_ohlcv(db, "ETHUSDT", "1m", start, 80, step_minutes=1)
        _insert_ohlcv(db, "ETHUSDT", "1h", start, 40, step_minutes=60)

        r1 = compute_and_store_pending_metrics(db, symbol="ETHUSDT", timeframe="1m", lookback_rows=80)
        r2 = compute_and_store_pending_metrics(db, symbol="ETHUSDT", timeframe="1h", lookback_rows=40)
        assert r1.last_metric_ts is not None
        assert r2.last_metric_ts is not None
        assert r1.timeframe == "1m"
        assert r2.timeframe == "1h"

