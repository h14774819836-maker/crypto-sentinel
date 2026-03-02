from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import numpy as np

from app.config import Settings
from app.logging import logger

RULE_VERSION = "v0.2"
RULE_VERSION_SCORE_V1 = "score_v1"


@dataclass(slots=True)
class AlertCandidate:
    symbol: str
    timeframe: str
    ts: datetime
    alert_type: str
    severity: str
    reason: str
    metrics: dict
    rule_version: str = RULE_VERSION
    score: int | None = None
    regime: str | None = None
    direction: str | None = None
    confirm_status: str | None = None
    cooldown_seconds: int | None = None
    summary_zh: str | None = None
    debug: dict | None = None
    delivery_hint: dict | None = None


@dataclass(slots=True)
class ScoredAnomalySnapshot:
    symbol: str
    timeframe: str
    ts: datetime
    score: int
    direction: str
    regime: str
    score_breakdown: dict[str, float]
    score_breakdown_meta: dict[str, Any]
    thresholds: dict[str, Any]
    observations: dict[str, Any]
    event_family: str = "momentum"
    rule_version: str = RULE_VERSION_SCORE_V1
    alert_type: str | None = None


@dataclass(slots=True)
class MTFConfirmResult:
    status: str
    five_m_confirmed: bool | None
    fifteen_m_confirmed: bool | None
    detail: dict[str, Any]


def build_event_uid(symbol: str, alert_type: str, timeframe: str, ts: datetime, rule_version: str = RULE_VERSION) -> str:
    ts_bucket = ts.replace(second=0, microsecond=0).isoformat()
    raw = f"{symbol}|{alert_type}|{timeframe}|{ts_bucket}|{rule_version}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_anomaly_state_key(symbol: str, timeframe: str, event_family: str, direction: str) -> str:
    return f"{symbol}|{timeframe}|{event_family}|{direction}"


def _is_valid_number(value: float | None) -> bool:
    return value is not None and not np.isnan(value)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    return v


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _percentile_rank(sample: Iterable[float], x: float | None) -> float | None:
    if x is None:
        return None
    vals = [float(v) for v in sample if _is_valid_number(float(v))]
    if not vals:
        return None
    arr = np.array(vals, dtype=float)
    return float((arr <= x).sum() / len(arr))


def _score_threshold_for_regime(regime: str, settings: Settings) -> int:
    regime = (regime or "").upper()
    if regime == "TRENDING":
        return int(settings.anomaly_score_threshold_trending)
    if regime == "VOLATILE":
        return int(settings.anomaly_score_threshold_volatile)
    return int(settings.anomaly_score_threshold_ranging)


def _score_to_severity(score: int) -> str:
    if score >= 90:
        return "CRITICAL"
    if score >= 75:
        return "WARNING"
    return "INFO"


def score_to_severity_label_zh(score: int) -> str:
    if score >= 90:
        return "极端"
    if score >= 80:
        return "严重"
    if score >= 70:
        return "中等"
    return "轻微"


def pick_adaptive_cooldown_seconds(score: int, settings: Settings) -> int:
    if score >= 92:
        return int(settings.anomaly_cooldown_seconds_score_92_plus)
    if score >= 85:
        return int(settings.anomaly_cooldown_seconds_score_85_91)
    return int(settings.anomaly_cooldown_seconds_score_80_84)


def classify_regime(
    *,
    metric: Any,
    vol_history: list[float],
    recent_1m_metrics: list[Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    ema_trend = str(getattr(metric, "ema_ribbon_trend", "") or "").upper()
    macd_hist = _safe_float(getattr(metric, "macd_hist", None))
    current_vol = _safe_float(getattr(metric, "rolling_vol_20", None))
    bb_bandwidth = _safe_float(getattr(metric, "bb_bandwidth", None))

    vol_percentile = _percentile_rank(vol_history, current_vol)
    bb_history = []
    if recent_1m_metrics:
        for row in recent_1m_metrics:
            val = _safe_float(getattr(row, "bb_bandwidth", None))
            if val is not None:
                bb_history.append(val)
    bb_percentile = _percentile_rank(bb_history, bb_bandwidth) if bb_history else None

    if vol_percentile is not None and vol_percentile >= 0.90:
        regime = "VOLATILE"
    elif ema_trend in {"UP", "DOWN"} and macd_hist is not None and ((ema_trend == "UP" and macd_hist > 0) or (ema_trend == "DOWN" and macd_hist < 0)):
        regime = "TRENDING"
    elif ema_trend == "MIXED" and ((bb_percentile is not None and bb_percentile <= 0.35) or (bb_percentile is None and bb_bandwidth is not None and bb_bandwidth <= 0.08)):
        regime = "RANGING"
    else:
        regime = "NEUTRAL"

    return regime, {
        "ema_ribbon_trend_1m": ema_trend or None,
        "macd_hist_1m": macd_hist,
        "vol_percentile": vol_percentile,
        "bb_bandwidth_1m": bb_bandwidth,
        "bb_bandwidth_percentile_1m": bb_percentile,
    }


def score_anomaly_snapshot(
    *,
    symbol: str,
    metric: Any,
    recent_1m_candles: list[Any],
    vol_history: list[float],
    settings: Settings,
    recent_1m_metrics: list[Any] | None = None,
) -> ScoredAnomalySnapshot | None:
    if metric is None:
        return None

    ts = getattr(metric, "ts", None)
    if ts is None:
        return None

    close = _safe_float(getattr(metric, "close", None))
    ret_1m = _safe_float(getattr(metric, "ret_1m", None))
    atr_14 = _safe_float(getattr(metric, "atr_14", None))
    volume_z = _safe_float(getattr(metric, "volume_zscore", None))
    current_vol = _safe_float(getattr(metric, "rolling_vol_20", None))
    bb_z = _safe_float(getattr(metric, "bb_zscore", None))
    ema_trend = str(getattr(metric, "ema_ribbon_trend", "") or "").upper()

    if close is None or close <= 0 or ret_1m is None:
        return None

    if atr_14 is not None and atr_14 > 0:
        atr_pct = atr_14 / close
        price_threshold = atr_pct * settings.spike_atr_multiplier
        threshold_mode = "atr_dynamic"
    else:
        atr_pct = None
        price_threshold = settings.spike_fallback_threshold
        threshold_mode = "fallback_fixed"

    eps = 1e-9
    price_ratio = abs(ret_1m) / max(price_threshold, eps)
    price_factor = _clip(price_ratio / 2.0, 0.0, 1.0)

    volume_factor = _clip(max(volume_z or 0.0, 0.0) / 4.0, 0.0, 1.0)

    vol_percentile = _percentile_rank(vol_history, current_vol) if current_vol is not None else None
    volatility_factor = 0.0
    if vol_percentile is not None:
        volatility_factor = _clip((vol_percentile - 0.5) / 0.5, 0.0, 1.0)

    breakout_up = breakout_down = False
    lookback_n = settings.breakout_lookback
    if len(recent_1m_candles) >= lookback_n:
        lookback = recent_1m_candles[-lookback_n:]
        prior = lookback[:-1] or lookback
        latest = lookback[-1]
        try:
            prev_high = max(row.high for row in prior)
            prev_low = min(row.low for row in prior)
            breakout_up = float(latest.close) > float(prev_high)
            breakout_down = float(latest.close) < float(prev_low)
        except Exception:
            breakout_up = breakout_down = False

    direction = "NONE"
    if ret_1m > 0:
        direction = "UP"
    elif ret_1m < 0:
        direction = "DOWN"

    breakout_factor = 0.0
    if direction == "UP" and breakout_up:
        breakout_factor = 0.7
    elif direction == "DOWN" and breakout_down:
        breakout_factor = 0.7

    bb_factor = 0.0
    if bb_z is not None:
        if direction == "UP" and bb_z > 0:
            bb_factor = _clip((bb_z - 1.0) / 2.0, 0.0, 0.3)
        elif direction == "DOWN" and bb_z < 0:
            bb_factor = _clip((abs(bb_z) - 1.0) / 2.0, 0.0, 0.3)

    trend_1m_factor = 0.0
    if direction == "UP" and ema_trend == "UP":
        trend_1m_factor = 0.1
    elif direction == "DOWN" and ema_trend == "DOWN":
        trend_1m_factor = 0.1

    structure_factor = _clip(breakout_factor + bb_factor + trend_1m_factor, 0.0, 1.0)

    score_breakdown = {
        "price": round(30.0 * price_factor, 2),
        "volume": round(25.0 * volume_factor, 2),
        "volatility": round(25.0 * volatility_factor, 2),
        "structure": round(20.0 * structure_factor, 2),
    }
    score = int(round(_clip(sum(score_breakdown.values()), 0.0, 100.0)))

    regime, regime_debug = classify_regime(metric=metric, vol_history=vol_history, recent_1m_metrics=recent_1m_metrics)
    regime_threshold = _score_threshold_for_regime(regime, settings)
    exit_threshold = max(0, regime_threshold - int(settings.anomaly_hysteresis_exit_delta))

    alert_type = None
    if direction == "UP":
        alert_type = "MOMENTUM_ANOMALY_UP"
    elif direction == "DOWN":
        alert_type = "MOMENTUM_ANOMALY_DOWN"

    return ScoredAnomalySnapshot(
        symbol=symbol,
        timeframe="1m",
        ts=ts,
        score=score,
        direction=direction,
        regime=regime,
        score_breakdown=score_breakdown,
        score_breakdown_meta={
            "phase": "phase2_no_mtf_structure",
            "components_enabled": ["price", "volume", "volatility", "structure.breakout_hit_1m", "structure.bb_extreme_1m", "structure.trend_align_1m"],
            "components_missing": ["structure.trend_align_5m", "structure.trend_align_15m"],
        },
        thresholds={
            "score_enter": regime_threshold,
            "score_exit": exit_threshold,
            "price_threshold_ret": price_threshold,
            "price_threshold_mode": threshold_mode,
            "atr_pct": atr_pct,
        },
        observations={
            "ret_1m": ret_1m,
            "volume_zscore": volume_z,
            "rolling_vol_20": current_vol,
            "vol_percentile": vol_percentile,
            "bb_zscore_1m": bb_z,
            "breakout_up_1m": breakout_up,
            "breakout_down_1m": breakout_down,
            **regime_debug,
        },
        alert_type=alert_type,
    )


def _close_direction_and_trend(rows: list[Any], expect_direction: str) -> tuple[bool | None, dict[str, Any]]:
    if len(rows) < 2:
        return None, {"data_ok": False}
    prev = rows[-2]
    curr = rows[-1]
    prev_close = _safe_float(getattr(prev, "close", None))
    curr_close = _safe_float(getattr(curr, "close", None))
    trend = str(getattr(curr, "ema_ribbon_trend", "") or "").upper() or None
    bb_z = _safe_float(getattr(curr, "bb_zscore", None))
    if prev_close is None or curr_close is None:
        return None, {"data_ok": False, "ema_ribbon_trend": trend, "bb_zscore": bb_z}

    if expect_direction == "UP":
        close_ok = curr_close > prev_close
        trend_ok = trend == "UP"
        bb_ok = bb_z is None or bb_z >= 0
    else:
        close_ok = curr_close < prev_close
        trend_ok = trend == "DOWN"
        bb_ok = bb_z is None or bb_z <= 0

    ok = bool(close_ok and trend_ok and bb_ok)
    return ok, {
        "data_ok": True,
        "prev_close": prev_close,
        "curr_close": curr_close,
        "ema_ribbon_trend": trend,
        "bb_zscore": bb_z,
        "close_ok": close_ok,
        "trend_ok": trend_ok,
        "bb_ok": bb_ok,
    }


def compute_mtf_confirmation(
    *,
    direction: str,
    rows_5m: list[Any],
    rows_15m: list[Any],
    settings: Settings,
) -> MTFConfirmResult:
    if not settings.anomaly_require_mtf_confirm:
        return MTFConfirmResult(
            status="not_required",
            five_m_confirmed=None,
            fifteen_m_confirmed=None,
            detail={"required_tfs": []},
        )

    if direction not in {"UP", "DOWN"}:
        return MTFConfirmResult(
            status="not_required",
            five_m_confirmed=None,
            fifteen_m_confirmed=None,
            detail={"required_tfs": settings.anomaly_mtf_confirm_tf_list},
        )

    required = set(settings.anomaly_mtf_confirm_tf_list or ["5m", "15m"])
    five_ok, five_detail = (None, {"skipped": True})
    fifteen_ok, fifteen_detail = (None, {"skipped": True})

    if "5m" in required:
        five_ok, five_detail = _close_direction_and_trend(rows_5m, direction)
    if "15m" in required:
        fifteen_ok, fifteen_detail = _close_direction_and_trend(rows_15m, direction)

    if five_ok is True:
        status = "confirmed_5m"
    elif fifteen_ok is True:
        status = "confirmed_15m"
    else:
        data_missing = False
        if "5m" in required and five_ok is None:
            data_missing = True
        if "15m" in required and fifteen_ok is None:
            data_missing = True
        status = "insufficient_data" if data_missing else "pending_mtf"

    return MTFConfirmResult(
        status=status,
        five_m_confirmed=five_ok,
        fifteen_m_confirmed=fifteen_ok,
        detail={
            "required_tfs": sorted(required),
            "five_m": five_detail,
            "fifteen_m": fifteen_detail,
        },
    )


def evaluate_anomalies(
    *,
    symbol: str,
    metric,
    recent_1m_candles,
    vol_history: list[float],
    settings: Settings,
) -> list[AlertCandidate]:
    alerts: list[AlertCandidate] = []
    if metric is None:
        return alerts

    ts = metric.ts

    # --- PRICE_SPIKE: ATR-based dynamic threshold ---
    try:
        ret_1m = float(metric.ret_1m) if metric.ret_1m is not None else None
        atr_14 = float(metric.atr_14) if getattr(metric, "atr_14", None) is not None else None
        close = float(metric.close) if metric.close is not None else None

        if _is_valid_number(ret_1m):
            if _is_valid_number(atr_14) and close and close > 0:
                atr_pct = atr_14 / close
                spike_threshold = atr_pct * settings.spike_atr_multiplier
                threshold_mode = "atr_dynamic"
            else:
                spike_threshold = settings.spike_fallback_threshold
                threshold_mode = "fallback_fixed"

            if ret_1m >= spike_threshold:
                alerts.append(
                    AlertCandidate(
                        symbol=symbol,
                        timeframe="1m",
                        ts=ts,
                        alert_type="PRICE_SPIKE_UP",
                        severity="WARNING",
                        reason=f"1m return {ret_1m:.4f} >= threshold {spike_threshold:.4f} ({threshold_mode})",
                        metrics={"ret_1m": ret_1m, "threshold": spike_threshold, "mode": threshold_mode},
                    )
                )
            elif ret_1m <= -spike_threshold:
                alerts.append(
                    AlertCandidate(
                        symbol=symbol,
                        timeframe="1m",
                        ts=ts,
                        alert_type="PRICE_SPIKE_DOWN",
                        severity="WARNING",
                        reason=f"1m return {ret_1m:.4f} <= -{spike_threshold:.4f} ({threshold_mode})",
                        metrics={"ret_1m": ret_1m, "threshold": -spike_threshold, "mode": threshold_mode},
                    )
                )
    except Exception as exc:
        logger.warning("PRICE_SPIKE rule failed for %s: %s", symbol, exc)

    # --- VOLUME_ANOMALY ---
    try:
        volume_z = float(metric.volume_zscore) if metric.volume_zscore is not None else None
        if _is_valid_number(volume_z) and volume_z >= settings.anomaly_volume_zscore_threshold:
            alerts.append(
                AlertCandidate(
                    symbol=symbol,
                    timeframe="1m",
                    ts=ts,
                    alert_type="VOLUME_ANOMALY",
                    severity="INFO",
                    reason=f"volume z-score {volume_z:.3f} >= {settings.anomaly_volume_zscore_threshold:.1f}",
                    metrics={"volume_zscore": volume_z},
                )
            )
    except Exception as exc:
        logger.warning("VOLUME_ANOMALY rule failed for %s: %s", symbol, exc)

    # --- VOLATILITY_SURGE ---
    try:
        current_vol = float(metric.rolling_vol_20) if metric.rolling_vol_20 is not None else None
        if _is_valid_number(current_vol):
            threshold = None
            rule_mode = ""
            if len(vol_history) >= settings.vol_p75_min_candles:
                threshold = float(np.percentile(vol_history[-settings.vol_p75_min_candles :], 75))
                rule_mode = "p75_7d"
            elif len(vol_history) >= settings.vol_fallback_min_candles:
                sample = np.array(vol_history, dtype=float)
                threshold = float(sample.mean() + settings.vol_fallback_k * sample.std(ddof=0))
                rule_mode = "fallback_mean_std"
            else:
                logger.info(
                    "VOLATILITY_SURGE warming up for %s (%d/%d)",
                    symbol,
                    len(vol_history),
                    settings.vol_fallback_min_candles,
                )

            if threshold is not None and current_vol > threshold:
                alerts.append(
                    AlertCandidate(
                        symbol=symbol,
                        timeframe="1m",
                        ts=ts,
                        alert_type="VOLATILITY_SURGE",
                        severity="INFO",
                        reason=f"rolling_vol_20 {current_vol:.6f} > {rule_mode} threshold {threshold:.6f}",
                        metrics={"rolling_vol_20": current_vol, "threshold": threshold, "mode": rule_mode},
                    )
                )
    except Exception as exc:
        logger.warning("VOLATILITY_SURGE rule failed for %s: %s", symbol, exc)

    # --- BREAKOUT: configurable lookback ---
    try:
        lookback_n = settings.breakout_lookback
        if len(recent_1m_candles) < lookback_n:
            logger.debug("BREAKOUT warming up for %s (%d/%d)", symbol, len(recent_1m_candles), lookback_n)
        else:
            lookback = recent_1m_candles[-lookback_n:]
            prior = lookback[:-1]
            if not prior:
                prior = lookback
            latest = lookback[-1]
            prev_high = max(row.high for row in prior)
            prev_low = min(row.low for row in prior)
            if latest.close > prev_high:
                alerts.append(
                    AlertCandidate(
                        symbol=symbol,
                        timeframe="1m",
                        ts=latest.ts,
                        alert_type="BREAKOUT_UP",
                        severity="INFO",
                        reason=f"close {latest.close:.6f} > prev{lookback_n}_high {prev_high:.6f}",
                        metrics={"close": latest.close, "prev_high": prev_high, "lookback": lookback_n},
                    )
                )
            elif latest.close < prev_low:
                alerts.append(
                    AlertCandidate(
                        symbol=symbol,
                        timeframe="1m",
                        ts=latest.ts,
                        alert_type="BREAKOUT_DOWN",
                        severity="INFO",
                        reason=f"close {latest.close:.6f} < prev{lookback_n}_low {prev_low:.6f}",
                        metrics={"close": latest.close, "prev_low": prev_low, "lookback": lookback_n},
                    )
                )
    except Exception as exc:
        logger.warning("BREAKOUT rule failed for %s: %s", symbol, exc)

    return alerts

