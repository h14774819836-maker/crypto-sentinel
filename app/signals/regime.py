from __future__ import annotations


def classify_regime(rolling_vol_20: float | None, ret_10m: float | None) -> str:
    if rolling_vol_20 is None:
        return "unknown"
    if rolling_vol_20 > 0.02:
        return "high_vol"
    if ret_10m is not None and abs(ret_10m) > 0.01:
        return "trending"
    return "normal"
