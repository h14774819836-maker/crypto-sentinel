from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.db.models import AiSignal, Ohlcv, WorkerStatus
from app.db.repository import insert_ai_signal, list_recent_sent_ai_signals, upsert_ohlcv, upsert_worker_status
from app.db.session import Base
from app.features.aggregator import aggregate_10m_from_1m


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    with SessionLocal() as db:
        yield db


def test_ohlcv_upsert_is_idempotent(session):
    ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    payload = {
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "ts": ts,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 10.0,
        "source": "test",
    }
    upsert_ohlcv(session, payload)

    payload2 = {**payload, "close": 102.0, "volume": 20.0}
    upsert_ohlcv(session, payload2)

    count = session.scalar(select(func.count(Ohlcv.id)))
    row = session.scalar(select(Ohlcv).where(Ohlcv.symbol == "BTCUSDT", Ohlcv.timeframe == "1m", Ohlcv.ts == ts))

    assert count == 1
    assert row is not None
    assert row.close == 102.0
    assert row.volume == 20.0


def test_aggregate_10m_is_idempotent(session):
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    for i in range(10):
        ts = start + timedelta(minutes=i)
        upsert_ohlcv(
            session,
            {
                "symbol": "ETHUSDT",
                "timeframe": "1m",
                "ts": ts,
                "open": 200.0 + i,
                "high": 201.0 + i,
                "low": 199.0 + i,
                "close": 200.5 + i,
                "volume": 5.0 + i,
                "source": "test",
            },
        )

    assert aggregate_10m_from_1m(session, "ETHUSDT", start)
    assert aggregate_10m_from_1m(session, "ETHUSDT", start)

    count = session.scalar(
        select(func.count(Ohlcv.id)).where(Ohlcv.symbol == "ETHUSDT", Ohlcv.timeframe == "10m", Ohlcv.ts == start)
    )
    row = session.scalar(select(Ohlcv).where(Ohlcv.symbol == "ETHUSDT", Ohlcv.timeframe == "10m", Ohlcv.ts == start))

    assert count == 1
    assert row is not None
    assert row.open == 200.0
    assert row.close == 209.5


def test_worker_status_upsert(session):
    started = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    seen_1 = started + timedelta(seconds=15)
    seen_2 = started + timedelta(seconds=45)

    upsert_worker_status(session, worker_id="worker-1", started_at=started, last_seen=seen_1, version="0.1.0")
    upsert_worker_status(session, worker_id="worker-1", started_at=started, last_seen=seen_2, version="0.1.0")

    count = session.scalar(select(func.count(WorkerStatus.id)))
    row = session.scalar(select(WorkerStatus).where(WorkerStatus.worker_id == "worker-1"))

    assert count == 1
    assert row is not None
    assert row.last_seen.replace(tzinfo=timezone.utc) == seen_2


def test_insert_ai_signal_ignores_unknown_payload_keys(session):
    ts = datetime(2026, 2, 24, 9, 0, tzinfo=timezone.utc)
    row = insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "ts": ts,
            "direction": "LONG",
            "confidence": 80,
            "reasoning": "test",
            "validation_warnings": ["should be ignored"],
        },
    )

    assert row is not None
    assert row.id is not None
    assert session.scalar(select(func.count(AiSignal.id))) == 1


def test_insert_ai_signal_sets_ts_when_missing(session):
    row = insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "direction": "HOLD",
            "confidence": 45,
            "reasoning": "auto timestamp",
        },
    )

    assert row is not None
    assert row.ts is not None
    assert row.timeframe == "1m"


def test_list_recent_sent_ai_signals_filters_by_time_and_sent(session):
    now = datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc)
    insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "ts": now - timedelta(minutes=10),
            "direction": "LONG",
            "entry_price": 100.0,
            "take_profit": 101.0,
            "stop_loss": 99.0,
            "confidence": 80,
            "reasoning": "sent recent",
            "sent_to_telegram": True,
            "created_at": now - timedelta(minutes=2),
        },
    )
    insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "ts": now - timedelta(minutes=20),
            "direction": "LONG",
            "entry_price": 100.0,
            "take_profit": 101.0,
            "stop_loss": 99.0,
            "confidence": 80,
            "reasoning": "unsent recent",
            "sent_to_telegram": False,
            "created_at": now - timedelta(minutes=1),
        },
    )
    insert_ai_signal(
        session,
        {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "ts": now - timedelta(hours=2),
            "direction": "SHORT",
            "entry_price": 100.0,
            "take_profit": 99.0,
            "stop_loss": 101.0,
            "confidence": 70,
            "reasoning": "sent old",
            "sent_to_telegram": True,
            "created_at": now - timedelta(hours=1),
        },
    )

    rows = list_recent_sent_ai_signals(
        session,
        symbol="BTCUSDT",
        timeframe="1m",
        since_ts=now - timedelta(minutes=5),
    )

    assert len(rows) == 1
    assert rows[0].sent_to_telegram is True
    assert rows[0].reasoning == "sent recent"
