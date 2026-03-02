from __future__ import annotations

import re
from typing import Any


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "")
        nums = re.findall(r"\d+(?:\.\d+)?", cleaned)
        if not nums:
            return None
        if len(nums) == 1:
            return float(nums[0])
        # "62000-63000" style range: take midpoint
        return (float(nums[0]) + float(nums[1])) / 2.0
    return None


_ENUM_MAP: dict[str, str] = {
    "BULLISH": "BULL",
    "BEARISH": "BEAR",
    "BULL": "BULL",
    "BEAR": "BEAR",
    "STRONG_BULLISH": "STRONG_BULL",
    "STRONG_BEARISH": "STRONG_BEAR",
    "STRONG_BULL": "STRONG_BULL",
    "STRONG_BEAR": "STRONG_BEAR",
    "STRONG BEAR": "STRONG_BEAR",
    "STRONG-BEAR": "STRONG_BEAR",
    "VERYHIGH": "VERY_HIGH",
    "COUNTER TREND": "COUNTER_TREND",
    "COUNTER-TREND": "COUNTER_TREND",
    # Chinese aliases (unicode escapes keep source stable across editors/encodings)
    "\u5f3a\u70c8\u770b\u7a7a": "STRONG_BEAR",  # 强烈看空
    "\u5f3a\u70c8\u770b\u591a": "STRONG_BULL",  # 强烈看多
    "\u6781\u5f3a": "VERY_HIGH",  # 极强
    "\u9ad8\u7f6e\u4fe1": "HIGH",  # 高置信
    "\u4e2d\u7b49": "MEDIUM",  # 中等
    "\u4f4e\u7f6e\u4fe1": "LOW",  # 低置信
    # Legacy input kept for compatibility with existing tests/data
    "\ud4fb\uc8e0\uc600\uc655": "STRONG_BEAR",
}


def _normalize_enum(value: Any, default: str | None = None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return default
    raw = value.strip()
    upper = raw.upper()
    normalized = upper.replace("-", "_").replace(" ", "_")
    if normalized in _ENUM_MAP:
        return _ENUM_MAP[normalized]
    if upper in _ENUM_MAP:
        return _ENUM_MAP[upper]
    if raw in _ENUM_MAP:
        return _ENUM_MAP[raw]
    for key, mapped in _ENUM_MAP.items():
        if key in upper:
            return mapped
    return normalized


def normalize_vta(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize weak model output into a predictable VTA structure."""
    if not isinstance(raw, dict):
        return {}

    vta = dict(raw)
    vta.setdefault("vta_version", "1.0")
    vta["meta"] = _safe_dict(vta.get("meta"))
    vta["market_view"] = _safe_dict(vta.get("market_view"))
    vta["market_state"] = _safe_dict(vta.get("market_state"))
    vta["key_points"] = _safe_list(vta.get("key_points"))
    vta["levels"] = _safe_dict(vta.get("levels"))
    vta["indicators"] = _safe_dict(vta.get("indicators"))
    vta["trade_plans"] = _safe_list(vta.get("trade_plans"))
    vta["risks"] = _safe_list(vta.get("risks"))
    vta["provenance"] = _safe_dict(vta.get("provenance"))

    levels = vta["levels"]
    levels["resistance"] = _safe_list(levels.get("resistance"))
    levels["support"] = _safe_list(levels.get("support"))
    levels["other"] = _safe_list(levels.get("other"))

    provenance = vta["provenance"]
    provenance["schema_errors"] = _safe_list(provenance.get("schema_errors"))
    provenance["normalized"] = True

    mv = vta["market_view"]
    mv["bias_1_7d"] = _normalize_enum(mv.get("bias_1_7d"), "NEUTRAL")
    mv["bias_1_4w"] = _normalize_enum(mv.get("bias_1_4w"), "NEUTRAL")
    mv["conviction"] = _normalize_enum(mv.get("conviction"), "MEDIUM")

    for level_type in ("resistance", "support", "other"):
        normalized_levels: list[dict[str, Any]] = []
        for item in _safe_list(levels.get(level_type)):
            if not isinstance(item, dict):
                continue
            cloned = dict(item)
            cloned["level"] = _safe_float(cloned.get("level"))
            normalized_levels.append(cloned)
        levels[level_type] = normalized_levels

    normalized_plans: list[dict[str, Any]] = []
    for plan in _safe_list(vta.get("trade_plans")):
        if not isinstance(plan, dict):
            continue
        cloned = dict(plan)
        cloned["direction"] = _normalize_enum(cloned.get("direction"))
        cloned["style"] = _normalize_enum(cloned.get("style"), "TREND")

        entry = _safe_dict(cloned.get("entry"))
        if entry:
            entry["price"] = _safe_float(entry.get("price"))
        cloned["entry"] = entry

        stop = _safe_dict(cloned.get("stop"))
        if stop:
            stop["price"] = _safe_float(stop.get("price"))
        cloned["stop"] = stop

        targets: list[dict[str, Any]] = []
        for target in _safe_list(cloned.get("targets")):
            if not isinstance(target, dict):
                continue
            copied_target = dict(target)
            copied_target["price"] = _safe_float(copied_target.get("price"))
            targets.append(copied_target)
        cloned["targets"] = targets
        cloned["rules"] = [r for r in _safe_list(cloned.get("rules")) if isinstance(r, str)]
        normalized_plans.append(cloned)
    vta["trade_plans"] = normalized_plans

    return vta


def validate_vta(vta: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate high-level bounds and required top-level keys."""
    errors: list[str] = []

    if len(_safe_list(vta.get("key_points"))) > 5:
        errors.append("key_points extends maximum length of 5")
    if len(_safe_list(vta.get("trade_plans"))) > 3:
        errors.append("trade_plans extends maximum length of 3")
    if len(_safe_list(vta.get("risks"))) > 5:
        errors.append("risks extends maximum length of 5")

    for key in ("meta", "market_view", "levels", "indicators", "provenance"):
        if key not in vta:
            errors.append(f"Missing required top-level key: {key}")

    return len(errors) == 0, errors


def compute_scores(vta: dict[str, Any]) -> dict[str, Any]:
    """Compute DC, PQ and VSI from normalized VTA payload."""
    mv = _safe_dict(vta.get("market_view"))
    bias_map = {"STRONG_BEAR": -2, "BEAR": -1, "NEUTRAL": 0, "BULL": 1, "STRONG_BULL": 2}
    conv_map = {"VERY_HIGH": 1.0, "HIGH": 0.75, "MEDIUM": 0.5, "LOW": 0.25}

    b7 = bias_map.get(str(mv.get("bias_1_7d", "NEUTRAL")), 0)
    b4 = bias_map.get(str(mv.get("bias_1_4w", "NEUTRAL")), 0)
    bias_score = 0.65 * b7 + 0.35 * b4
    conviction = conv_map.get(str(mv.get("conviction", "MEDIUM")), 0.5)

    dc = max(-100.0, min(100.0, 50.0 * bias_score * conviction))

    pq_completeness = 0.0
    pq_specificity = 0.0
    pq_consistency = 0.0

    trade_plans = _safe_list(vta.get("trade_plans"))
    if trade_plans:
        completeness_scores: list[float] = []
        specificity_scores: list[float] = []
        consistency_scores: list[float] = []

        for plan in trade_plans:
            if not isinstance(plan, dict):
                continue
            entry = _safe_dict(plan.get("entry"))
            stop = _safe_dict(plan.get("stop"))
            targets = _safe_list(plan.get("targets"))

            comp = 0.0
            if entry.get("type") or entry.get("condition"):
                comp += 6
            if stop.get("price") or stop.get("invalidation"):
                comp += 6
            if targets and isinstance(targets[0], dict) and targets[0].get("price") is not None:
                comp += 8
            if plan.get("alt_scenario"):
                comp += 5
            elif conviction >= 0.75:
                comp -= 5
            completeness_scores.append(max(0.0, min(25.0, comp)))

            valid_targets = [t for t in targets if isinstance(t, dict) and t.get("price") is not None]
            specificity_scores.append((len(valid_targets) / max(1, len(targets))) * 10.0)

            cons = 15.0
            direction = str(plan.get("direction") or "")
            style = str(plan.get("style") or "TREND")
            if direction and dc != 0:
                is_long = direction in {"LONG", "BULL"}
                is_short = direction in {"SHORT", "BEAR"}
                aligned = (is_long and dc > 0) or (is_short and dc < 0)
                if not aligned:
                    cons -= 15.0 if style == "TREND" else 5.0
            consistency_scores.append(cons)

        if completeness_scores:
            pq_completeness = sum(completeness_scores) / len(completeness_scores)
        if specificity_scores:
            pq_specificity = sum(specificity_scores) / len(specificity_scores)
        if consistency_scores:
            pq_consistency = sum(consistency_scores) / len(consistency_scores)

    levels = _safe_dict(vta.get("levels"))
    all_levels = _safe_list(levels.get("resistance")) + _safe_list(levels.get("support"))
    if all_levels:
        valid_levels = sum(1 for item in all_levels if isinstance(item, dict) and item.get("level") is not None)
        pq_specificity += (valid_levels / len(all_levels)) * 10.0
    pq_specificity = min(20.0, pq_specificity)

    key_points = _safe_list(vta.get("key_points"))
    pq_evidence = 0.0
    if len(key_points) >= 3:
        types = {item.get("type") for item in key_points if isinstance(item, dict) and item.get("type")}
        pq_evidence = 20.0 if len(types) >= 2 else 10.0
    elif len(key_points) > 0:
        pq_evidence = 5.0

    pq_risk = 0.0
    risks = _safe_list(vta.get("risks"))
    if risks:
        discipline_tokens = ("NO_CHASE", "WAIT", "CONFIRM", "DISCIPLINE", "\u4e0d\u8ffd", "\u786e\u8ba4")
        has_discipline = False
        for plan in trade_plans:
            if not isinstance(plan, dict):
                continue
            for rule in _safe_list(plan.get("rules")):
                if not isinstance(rule, str):
                    continue
                upper_rule = rule.upper()
                if any(token in upper_rule or token in rule for token in discipline_tokens):
                    has_discipline = True
                    break
            if has_discipline:
                break
        pq_risk = 20.0 if has_discipline else 10.0

    pq = max(0.0, min(100.0, pq_completeness + pq_specificity + pq_evidence + pq_consistency + pq_risk))
    vsi = max(-100.0, min(100.0, dc * (pq / 100.0)))

    return {
        "dc": round(dc, 2),
        "pq": round(pq, 2),
        "vsi": round(vsi, 2),
        "pq_breakdown": {
            "completeness": round(pq_completeness, 2),
            "specificity": round(pq_specificity, 2),
            "evidence": round(pq_evidence, 2),
            "consistency": round(pq_consistency, 2),
            "risk": round(pq_risk, 2),
        },
    }


def adapt_legacy(old_json: dict[str, Any]) -> dict[str, Any]:
    """Adapt legacy analyst payload format to VTA-JSON v1."""
    if "meta" in old_json and "market_view" in old_json:
        return normalize_vta(old_json)

    vta: dict[str, Any] = {
        "vta_version": "1.0",
        "meta": {
            "analyst": old_json.get("analyst", "Unknown"),
            "assets": ["BTC"],
        },
    }

    bias_str = str(old_json.get("bias", "neutral")).upper()
    conviction = "MEDIUM"
    conf = int(old_json.get("confidence", 50) or 50)
    if conf >= 80:
        conviction = "VERY_HIGH"
    elif conf >= 60:
        conviction = "HIGH"
    elif conf <= 20:
        conviction = "LOW"
    vta["market_view"] = {
        "bias_1_7d": bias_str,
        "bias_1_4w": bias_str,
        "conviction": conviction,
    }

    old_levels = _safe_dict(old_json.get("key_levels"))
    vta["levels"] = {
        "support": [{"level": level, "note": ""} for level in _safe_list(old_levels.get("support"))],
        "resistance": [{"level": level, "note": ""} for level in _safe_list(old_levels.get("resistance"))],
    }

    key_points: list[dict[str, Any]] = []
    for point in _safe_list(old_json.get("thesis_points")):
        if not isinstance(point, str):
            continue
        point_upper = point.upper()
        ptype = "OTHER"
        if any(token in point_upper for token in ("CPI", "NFP", "RATE", "FED", "MACRO")):
            ptype = "MACRO"
        elif any(token in point_upper for token in ("MA", "MACD", "RSI", "KDJ", "INDICATOR")):
            ptype = "INDICATOR"
        elif any(token in point_upper for token in ("VOLUME", "LIQUIDITY")):
            ptype = "VOLUME"
        elif any(token in point_upper for token in ("SUPPORT", "RESISTANCE", "BREAK")):
            ptype = "LEVEL"
        elif any(token in point_upper for token in ("PATTERN", "TRIANGLE", "FLAG")):
            ptype = "PATTERN"
        key_points.append({"point": point, "type": ptype})
    vta["key_points"] = key_points

    vta["risks"] = [
        {"text": risk, "severity": "MEDIUM"}
        for risk in _safe_list(old_json.get("risk_notes"))
        if isinstance(risk, str)
    ]

    return normalize_vta(vta)
