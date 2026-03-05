from datetime import datetime, timedelta, timezone

from app.scheduler._jobs_impl import _compute_tick_return


def test_compute_tick_return_uses_lookback_base():
    now = datetime(2026, 3, 4, 12, 0, 20, tzinfo=timezone.utc)
    points = [
        (now - timedelta(seconds=20), 100.0),
        (now - timedelta(seconds=12), 101.0),
        (now, 102.0),
    ]
    ret = _compute_tick_return(points, now=now, lookback_seconds=15)
    assert ret is not None
    pct, base = ret
    assert round(base, 4) == 100.0
    assert round(pct, 4) == 0.02


def test_compute_tick_return_fallbacks_to_first_point():
    now = datetime(2026, 3, 4, 12, 0, 10, tzinfo=timezone.utc)
    points = [
        (now - timedelta(seconds=5), 100.0),
        (now, 100.5),
    ]
    ret = _compute_tick_return(points, now=now, lookback_seconds=15)
    assert ret is not None
    pct, base = ret
    assert round(base, 4) == 100.0
    assert round(pct, 4) == 0.005
