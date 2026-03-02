from __future__ import annotations

from types import SimpleNamespace

from app.config import Settings
from app.signals.anomaly import compute_mtf_confirmation


def test_mtf_confirm_uses_close_sequence_not_ret_field():
    settings = Settings(_env_file=None, anomaly_require_mtf_confirm=True, anomaly_mtf_confirm_tfs="5m,15m")

    rows_5m = [
        SimpleNamespace(close=100.0, ema_ribbon_trend="MIXED", bb_zscore=0.1),
        SimpleNamespace(close=101.0, ema_ribbon_trend="UP", bb_zscore=0.5),
    ]
    rows_15m = []

    result = compute_mtf_confirmation(direction="UP", rows_5m=rows_5m, rows_15m=rows_15m, settings=settings)
    assert result.status == "confirmed_5m"
    assert result.five_m_confirmed is True


def test_mtf_confirm_pending_when_direction_not_confirmed():
    settings = Settings(_env_file=None, anomaly_require_mtf_confirm=True, anomaly_mtf_confirm_tfs="5m")
    rows_5m = [
        SimpleNamespace(close=100.0, ema_ribbon_trend="UP", bb_zscore=0.2),
        SimpleNamespace(close=99.0, ema_ribbon_trend="UP", bb_zscore=0.1),
    ]
    result = compute_mtf_confirmation(direction="UP", rows_5m=rows_5m, rows_15m=[], settings=settings)
    assert result.status == "pending_mtf"
    assert result.five_m_confirmed is False

