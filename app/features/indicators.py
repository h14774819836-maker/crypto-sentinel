from __future__ import annotations

import numpy as np
import pandas as pd


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _stochastic_rsi(rsi_series: pd.Series, period: int = 14, k_smooth: int = 3, d_smooth: int = 3) -> tuple[pd.Series, pd.Series]:
    """Stochastic RSI: maps RSI into 0-100 range for more sensitive overbought/oversold detection."""
    rsi_min = rsi_series.rolling(period).min()
    rsi_max = rsi_series.rolling(period).max()
    rsi_range = rsi_max - rsi_min
    stoch_rsi = ((rsi_series - rsi_min) / rsi_range.replace(0, np.nan)) * 100
    k = stoch_rsi.rolling(k_smooth).mean()
    d = k.rolling(d_smooth).mean()
    return k, d


def _ema_ribbon_trend(close: pd.Series, spans: tuple[int, ...] = (8, 13, 21, 34, 55)) -> pd.Series:
    """Classify EMA ribbon trend: UP (all EMAs ascending), DOWN (descending), MIXED."""
    emas = [close.ewm(span=s, adjust=False).mean() for s in spans]

    def classify_row(idx: int) -> str | float:
        values = [e.iat[idx] for e in emas]
        if any(np.isnan(v) for v in values):
            return np.nan
        # UP: short EMAs above long EMAs (ascending order)
        if all(values[i] >= values[i + 1] for i in range(len(values) - 1)):
            return "UP"
        # DOWN: short EMAs below long EMAs (descending order)
        if all(values[i] <= values[i + 1] for i in range(len(values) - 1)):
            return "DOWN"
        return "MIXED"

    return pd.Series([classify_row(i) for i in range(len(close))], index=close.index)


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume: cumulative volume flow based on price direction."""
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    return (direction * volume).cumsum()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    frame = df.copy().sort_values("ts").reset_index(drop=True)

    # --- Returns ---
    frame["ret_1m"] = frame["close"].pct_change(1)
    frame["ret_3m"] = frame["close"].pct_change(3)
    frame["ret_5m"] = frame["close"].pct_change(5)
    frame["ret_10m"] = frame["close"].pct_change(10)

    # --- Volatility ---
    returns = frame["close"].pct_change(1)
    frame["rolling_vol_20"] = returns.rolling(20).std()

    # --- ATR(14) ---
    prev_close = frame["close"].shift(1)
    tr_1 = frame["high"] - frame["low"]
    tr_2 = (frame["high"] - prev_close).abs()
    tr_3 = (frame["low"] - prev_close).abs()
    true_range = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)
    frame["atr_14"] = true_range.rolling(14).mean()

    # --- Bollinger Bands ---
    ma_20 = frame["close"].rolling(20).mean()
    std_20 = frame["close"].rolling(20).std()
    frame["bb_zscore"] = (frame["close"] - ma_20) / std_20.replace(0, np.nan)
    frame["bb_bandwidth"] = (4 * std_20) / ma_20.replace(0, np.nan)

    # --- RSI(14) ---
    frame["rsi_14"] = _rsi(frame["close"], period=14)

    # --- MACD ---
    ema_12 = frame["close"].ewm(span=12, adjust=False).mean()
    ema_26 = frame["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema_12 - ema_26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    frame["macd_hist"] = macd_line - signal_line

    # --- Volume Z-score ---
    vol_mean = frame["volume"].rolling(20).mean()
    vol_std = frame["volume"].rolling(20).std().replace(0, np.nan)
    frame["volume_zscore"] = (frame["volume"] - vol_mean) / vol_std

    # --- OBV (On-Balance Volume) ---
    frame["obv"] = _obv(frame["close"], frame["volume"])

    # --- Stochastic RSI ---
    frame["stoch_rsi_k"], frame["stoch_rsi_d"] = _stochastic_rsi(frame["rsi_14"])

    # --- EMA Ribbon trend ---
    frame["ema_ribbon_trend"] = _ema_ribbon_trend(frame["close"])

    return frame
