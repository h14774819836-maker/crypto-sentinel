from __future__ import annotations

from typing import Any


def metric_to_dict(m) -> dict[str, Any]:
    """Convert a MarketMetric row to a dict for AI prompts."""
    return {
        "ts": m.ts,
        "close": m.close,
        "ret_1m": m.ret_1m,
        "ret_10m": m.ret_10m,
        "rolling_vol_20": m.rolling_vol_20,
        "volume_zscore": m.volume_zscore,
        "rsi_14": m.rsi_14,
        "stoch_rsi_k": getattr(m, "stoch_rsi_k", None),
        "stoch_rsi_d": getattr(m, "stoch_rsi_d", None),
        "macd_hist": m.macd_hist,
        "bb_zscore": m.bb_zscore,
        "bb_bandwidth": m.bb_bandwidth,
        "atr_14": m.atr_14,
        "obv": getattr(m, "obv", None),
        "ema_ribbon_trend": getattr(m, "ema_ribbon_trend", None),
    }

