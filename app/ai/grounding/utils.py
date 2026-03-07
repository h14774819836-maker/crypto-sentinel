from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


METRIC_ALIASES: dict[str, str] = {
    "price": "close",
    "last_price": "close",
    "mark_price": "mark",
    "index_price": "index",
    "rsi": "rsi_14",
    "rsi14": "rsi_14",
    "rsi_14": "rsi_14",
    "stochrsi_k": "stoch_rsi_k",
    "stoch_rsi_k": "stoch_rsi_k",
    "stochrsi_d": "stoch_rsi_d",
    "stoch_rsi_d": "stoch_rsi_d",
    "atr": "atr_14",
    "atr14": "atr_14",
    "atr_14": "atr_14",
    "rolling_vol": "rolling_vol_20",
    "rolling_vol_20": "rolling_vol_20",
    "bbz": "bb_zscore",
    "bb_z": "bb_zscore",
    "bb_zscore": "bb_zscore",
    "bandwidth": "bb_bandwidth",
    "bb_bandwidth": "bb_bandwidth",
    "funding": "funding_rate",
    "funding_rate": "funding_rate",
    "momentum_alignment": "momentum_alignment",
    "momentum_alignment_score": "momentum_alignment",
    "range_position": "range_position",
    "snapshot_age_sec": "snapshot_age_sec",
}


def normalize_metric_key(name: str) -> str:
    token = str(name or "").strip().lower().replace("-", "_").replace(" ", "_")
    token = token.replace("__", "_")
    return METRIC_ALIASES.get(token, token)


def normalize_timeframe(value: Any) -> str:
    return str(value or "").strip().lower()


def parse_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        return float(value)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    pct = s.endswith("%")
    if pct:
        s = s[:-1]
    s = s.replace(",", "").strip()
    try:
        num = float(s)
    except ValueError:
        return None
    if not math.isfinite(num):
        return None
    return (num / 100.0) if pct else num


def is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list, tuple, set))


def scalar_ground_str(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return format(float(value), ".15g")
    if isinstance(value, str):
        return value
    return str(value)


def resolve_dot_path(payload: dict[str, Any], path: str) -> tuple[bool, Any, str]:
    if not isinstance(path, str) or not path.strip():
        return False, None, path

    def _resolve(full_path: str) -> tuple[bool, Any]:
        current: Any = payload
        for seg in _tokenize_dot_path(full_path):
            if isinstance(current, dict):
                if seg not in current:
                    return False, None
                current = current.get(seg)
                continue
            if isinstance(current, list):
                try:
                    idx = int(seg)
                except ValueError:
                    return False, None
                if idx < 0 or idx >= len(current):
                    return False, None
                current = current[idx]
                continue
            return False, None
        return True, current

    raw = path.strip()
    ok, value = _resolve(raw)
    if ok:
        return True, value, raw
    if not raw.startswith("facts."):
        full = f"facts.{raw}"
        ok, value = _resolve(full)
        if ok:
            return True, value, full
    return False, None, raw


def _tokenize_dot_path(path: str) -> list[str]:
    tokens: list[str] = []
    current = ""
    idx = 0
    while idx < len(path):
        ch = path[idx]
        if ch == ".":
            if current:
                tokens.append(current)
                current = ""
            idx += 1
            continue
        if ch == "[":
            if current:
                tokens.append(current)
                current = ""
            end = path.find("]", idx + 1)
            if end == -1:
                current += path[idx:]
                break
            bracket_token = path[idx + 1 : end].strip()
            if bracket_token:
                tokens.append(bracket_token)
            idx = end + 1
            continue
        current += ch
        idx += 1
    if current:
        tokens.append(current)
    return tokens


@dataclass(slots=True)
class Tolerance:
    soft_abs: float
    hard_abs: float
    soft_rel: float
    hard_rel: float


def metric_group(metric: str) -> str:
    key = normalize_metric_key(metric)
    if key in {"close", "open", "high", "low", "entry_price", "take_profit", "stop_loss", "mark", "index"}:
        return "price"
    if key in {"rsi_14", "stoch_rsi_k", "stoch_rsi_d"}:
        return "oscillator"
    if key in {"atr_14", "rolling_vol_20", "bb_bandwidth"}:
        return "volatility"
    if key in {"ret_1m", "ret_10m", "funding_rate", "funding_delta_24h", "snapshot_age_sec"}:
        return "rate"
    if key in {"bb_zscore", "zscore", "volume_zscore"}:
        return "zscore"
    return "default"


def tolerance_for_metric(
    metric: str,
    *,
    reference_price: float | None = None,
    atr: float | None = None,
) -> Tolerance:
    group = metric_group(metric)
    if group == "price":
        ref = abs(reference_price or 0.0)
        atr_val = abs(atr or 0.0)
        soft_abs = max(ref * 0.0003, atr_val * 0.1, 1e-8)
        hard_abs = max(ref * 0.001, atr_val * 0.35, 1e-8)
        return Tolerance(soft_abs=soft_abs, hard_abs=hard_abs, soft_rel=0.0015, hard_rel=0.0045)
    if group == "oscillator":
        return Tolerance(soft_abs=2.0, hard_abs=5.0, soft_rel=0.03, hard_rel=0.08)
    if group == "volatility":
        return Tolerance(soft_abs=0.0, hard_abs=0.0, soft_rel=0.12, hard_rel=0.35)
    if group == "rate":
        return Tolerance(soft_abs=0.00005, hard_abs=0.0002, soft_rel=0.15, hard_rel=0.5)
    if group == "zscore":
        return Tolerance(soft_abs=0.25, hard_abs=1.2, soft_rel=0.15, hard_rel=0.4)
    return Tolerance(soft_abs=1e-6, hard_abs=1e-5, soft_rel=0.03, hard_rel=0.1)


def relative_diff(actual: float, observed: float) -> float:
    denom = abs(actual)
    if denom <= 1e-12:
        return abs(observed - actual)
    return abs(observed - actual) / denom


def classify_diff(actual: float, observed: float, tol: Tolerance) -> str:
    abs_diff = abs(observed - actual)
    rel_diff = relative_diff(actual, observed)
    if abs_diff <= tol.soft_abs or rel_diff <= tol.soft_rel:
        return "pass"
    if abs_diff <= tol.hard_abs or rel_diff <= tol.hard_rel:
        return "warn"
    return "hard"


def nearest_value(candidates: list[float], observed: float) -> float | None:
    if not candidates:
        return None
    return min(candidates, key=lambda x: abs(float(x) - observed))
