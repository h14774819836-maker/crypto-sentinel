from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.signals.anomaly import build_anomaly_state_key, build_event_uid


def test_state_key_is_stable_while_event_uid_changes():
    ts = datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc)
    state_key_1 = build_anomaly_state_key("BTCUSDT", "1m", "momentum", "UP")
    state_key_2 = build_anomaly_state_key("BTCUSDT", "1m", "momentum", "UP")

    uid_1 = build_event_uid("BTCUSDT", "MOMENTUM_ANOMALY_UP", "1m", ts, "score_v1")
    uid_2 = build_event_uid("BTCUSDT", "MOMENTUM_ANOMALY_UP", "1m", ts + timedelta(minutes=1), "score_v1")

    assert state_key_1 == state_key_2
    assert uid_1 != uid_2

