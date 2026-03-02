from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.config import Settings
from app.signals.anomaly import score_anomaly_snapshot


def test_score_breakdown_meta_marks_phase2_components():
    settings = Settings(_env_file=None)
    ts0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    candles = []
    for i in range(80):
        candles.append(
            SimpleNamespace(
                ts=ts0 + timedelta(minutes=i),
                high=100 + i * 0.2 + 1,
                low=100 + i * 0.2 - 1,
                close=100 + i * 0.2 + (0.5 if i == 79 else 0),
            )
        )

    metric = SimpleNamespace(
        ts=ts0 + timedelta(minutes=79),
        close=float(candles[-1].close),
        ret_1m=0.02,
        atr_14=0.6,
        volume_zscore=3.2,
        rolling_vol_20=0.015,
        bb_zscore=1.8,
        bb_bandwidth=0.04,
        ema_ribbon_trend="UP",
        macd_hist=0.5,
    )
    recent_1m_metrics = [
        SimpleNamespace(bb_bandwidth=0.03 + i * 0.0001) for i in range(120)
    ]
    vol_history = [0.003 + i * 0.00005 for i in range(120)]

    snap = score_anomaly_snapshot(
        symbol="BTCUSDT",
        metric=metric,
        recent_1m_candles=candles,
        vol_history=vol_history,
        settings=settings,
        recent_1m_metrics=recent_1m_metrics,
    )

    assert snap is not None
    meta = snap.score_breakdown_meta
    assert meta["phase"] == "phase2_no_mtf_structure"
    assert "structure.trend_align_5m" in meta["components_missing"]
    assert "structure.breakout_hit_1m" in meta["components_enabled"]

