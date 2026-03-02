from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from app.db.repository import (
    get_latest_market_metric_ts,
    list_ohlcv_after_ts,
    list_recent_ohlcv,
    list_recent_ohlcv_upto_ts,
    upsert_market_metric,
)
from app.features.indicators import compute_indicators
from app.logging import logger


@dataclass(slots=True)
class FeatureResult:
    symbol: str
    timeframe: str
    ts: Any
    values: dict[str, Any]


@dataclass(slots=True)
class FeatureIncrementalResult:
    symbol: str
    timeframe: str
    processed_rows: int
    last_metric_ts: datetime | None
    pending_after_run: int


def _nan_to_none(value: Any) -> Any:
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


_NUMERIC_FIELDS = [
    "ret_1m", "ret_3m", "ret_5m", "ret_10m",
    "rolling_vol_20", "atr_14",
    "bb_zscore", "bb_bandwidth",
    "rsi_14", "macd_hist", "volume_zscore",
    "obv", "stoch_rsi_k", "stoch_rsi_d",
]


def _build_feature_frame(candles: list[Any]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts": c.ts,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in candles
        ]
    )


def _payload_from_feature_row(symbol: str, timeframe: str, latest: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "ts": latest["ts"],
        "close": float(latest["close"]),
    }

    for field in _NUMERIC_FIELDS:
        if field in latest.index:
            val = latest[field]
            payload[field] = _nan_to_none(float(val)) if pd.notna(val) else None
        else:
            payload[field] = None

    ema_trend = latest.get("ema_ribbon_trend") if "ema_ribbon_trend" in latest.index else None
    payload["ema_ribbon_trend"] = ema_trend if isinstance(ema_trend, str) else None
    return payload


def compute_and_store_latest_metric(session, symbol: str, timeframe: str = "1m", window: int = 300) -> FeatureResult | None:
    candles = list_recent_ohlcv(session, symbol=symbol, timeframe=timeframe, limit=window)
    if len(candles) < 30:
        logger.debug("Feature warmup for %s %s: only %d rows", symbol, timeframe, len(candles))
        return None

    frame = _build_feature_frame(candles)

    featured = compute_indicators(frame)
    latest = featured.iloc[-1]

    payload = _payload_from_feature_row(symbol, timeframe, latest)
    upsert_market_metric(session, payload)
    return FeatureResult(symbol=symbol, timeframe=timeframe, ts=payload["ts"], values=payload)


def compute_and_store_pending_metrics(
    session,
    *,
    symbol: str,
    timeframe: str = "1m",
    lookback_rows: int = 180,
    max_pending_bars: int = 20,
    max_batches: int = 3,
) -> FeatureIncrementalResult:
    processed_rows = 0
    latest_metric_ts = get_latest_market_metric_ts(session, symbol=symbol, timeframe=timeframe)

    # Bootstrap path for empty metric table: compute the latest from recent candles.
    if latest_metric_ts is None:
        bootstrap = compute_and_store_latest_metric(session, symbol=symbol, timeframe=timeframe, window=max(lookback_rows, 30))
        if bootstrap is None:
            return FeatureIncrementalResult(
                symbol=symbol,
                timeframe=timeframe,
                processed_rows=0,
                last_metric_ts=None,
                pending_after_run=0,
            )
        processed_rows = 1
        latest_metric_ts = bootstrap.ts

    for _ in range(max(1, max_batches)):
        if latest_metric_ts is None:
            break
        pending = list_ohlcv_after_ts(
            session,
            symbol=symbol,
            timeframe=timeframe,
            after_ts=latest_metric_ts,
            limit=max(1, max_pending_bars),
        )
        if not pending:
            break

        batch_end_ts = pending[-1].ts
        candles = list_recent_ohlcv_upto_ts(
            session,
            symbol=symbol,
            timeframe=timeframe,
            upto_ts=batch_end_ts,
            limit=max(lookback_rows, 30),
        )
        if len(candles) < 30:
            logger.debug("Feature warmup pending path for %s %s: only %d rows", symbol, timeframe, len(candles))
            break

        frame = _build_feature_frame(candles)
        featured = compute_indicators(frame)
        try:
            new_rows = featured[featured["ts"] > latest_metric_ts]
        except Exception:
            logger.exception("Failed to filter featured rows for %s %s", symbol, timeframe)
            break

        if new_rows.empty:
            latest_metric_ts = batch_end_ts
            continue

        for _, row in new_rows.iterrows():
            payload = _payload_from_feature_row(symbol, timeframe, row)
            upsert_market_metric(session, payload, commit=False)
            processed_rows += 1
            latest_metric_ts = payload["ts"]
        session.commit()

    pending_after_run = 0
    if latest_metric_ts is not None:
        pending_after_run = len(
            list_ohlcv_after_ts(
                session,
                symbol=symbol,
                timeframe=timeframe,
                after_ts=latest_metric_ts,
                limit=max(1, max_pending_bars),
            )
        )

    return FeatureIncrementalResult(
        symbol=symbol,
        timeframe=timeframe,
        processed_rows=processed_rows,
        last_metric_ts=latest_metric_ts,
        pending_after_run=pending_after_run,
    )
