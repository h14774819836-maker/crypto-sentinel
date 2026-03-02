from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AlertEvent
from app.db.repository import count_sent_alerts_today
from app.db.session import Base
from app.utils.time import ensure_utc, utc_day_bounds


def _mk_alert(uid: str, created_at: datetime, sent: bool) -> AlertEvent:
    return AlertEvent(
        event_uid=uid,
        symbol="BTCUSDT",
        timeframe="1m",
        ts=created_at,
        alert_type="MOMENTUM_ANOMALY_UP",
        severity="WARNING",
        reason="test",
        rule_version="score_v1",
        metrics_json={},
        sent_to_telegram=sent,
        created_at=created_at,
        updated_at=created_at,
    )


def test_count_sent_alerts_today_uses_utc_bounds_and_sent_flag():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    now = datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc)
    start, end = utc_day_bounds(now)

    with SessionLocal() as db:
        db.add(_mk_alert("a1", start + timedelta(hours=1), True))
        db.add(_mk_alert("a2", start + timedelta(hours=2), False))  # 不应占预算
        db.add(_mk_alert("a3", end + timedelta(minutes=1), True))   # 次日，不应计入
        db.commit()

        count = count_sent_alerts_today(db, "BTCUSDT", start, end)
        assert count == 1


def test_ensure_utc_treats_naive_as_utc():
    naive = datetime(2026, 2, 24, 0, 0, 0)
    converted = ensure_utc(naive)
    assert converted.tzinfo == timezone.utc
    assert converted.hour == 0

