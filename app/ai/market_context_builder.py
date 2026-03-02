from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any


EXPECTED_TFS_DEFAULT = ["4h", "1h", "15m", "5m", "1m"]

YOUTUBE_RADAR_MAX_CHARS = 2600
ALERTS_DIGEST_MAX_CHARS = 1000


def build_market_analysis_context(
    *,
    symbol: str,
    snapshots: dict[str, dict[str, Any]],
    recent_alerts: list[dict[str, Any]],
    funding_current: dict[str, Any] | None = None,
    funding_history: list[Any] | None = None,
    youtube_consensus: Any | None = None,
    youtube_insights: list[Any] | None = None,
    now: datetime | None = None,
    expected_timeframes: list[str] | None = None,
) -> dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    expected_tfs = expected_timeframes or EXPECTED_TFS_DEFAULT
    derived_by_tf: dict[str, Any] = {}
    for tf in expected_tfs:
        snap = snapshots.get(tf)
        if not snap:
            continue
        derived_by_tf[tf] = _derive_tf_features(snap)

    cross_tf_summary = _build_cross_tf_summary(derived_by_tf)
    alerts_digest = _build_alerts_digest(recent_alerts, now=now)
    funding_deltas = _build_funding_deltas(funding_current, funding_history, now=now)
    youtube_radar = _build_youtube_radar(
        symbol=symbol,
        youtube_consensus=youtube_consensus,
        youtube_insights=youtube_insights or [],
        now=now,
    )
    input_budget_meta = _apply_context_clipping(alerts_digest, youtube_radar)
    data_quality = _build_data_quality(
        snapshots=snapshots,
        funding_current=funding_current,
        youtube_radar=youtube_radar,
        alerts_digest=alerts_digest,
        now=now,
        expected_timeframes=expected_tfs,
    )
    brief = _build_brief(
        symbol=symbol,
        derived_by_tf=derived_by_tf,
        cross_tf_summary=cross_tf_summary,
        data_quality=data_quality,
    )

    return {
        "brief": brief,
        "alerts_digest": alerts_digest,
        "youtube_radar": youtube_radar,
        "funding_deltas": funding_deltas,
        "data_quality": data_quality,
        "input_budget_meta": input_budget_meta,
    }


def _build_brief(
    *,
    symbol: str,
    derived_by_tf: dict[str, Any],
    cross_tf_summary: dict[str, Any],
    data_quality: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    tradeable = True

    alignment_score = int(cross_tf_summary.get("alignment_score", 0) or 0)
    if alignment_score < 45:
        tradeable = False
        reasons.append("多周期方向一致性不足")

    vol_flags = [
        feat.get("volatility_state")
        for feat in derived_by_tf.values()
        if isinstance(feat, dict)
    ]
    if vol_flags and all(v == "LOW_VOL" for v in vol_flags if v):
        tradeable = False
        reasons.append("整体波动偏低")

    if data_quality.get("overall") == "POOR":
        tradeable = False
        reasons.append("数据质量较差")

    one_m = derived_by_tf.get("1m") or {}
    range_position = (((one_m.get("range_stats") or {}).get("range_position")))
    if isinstance(range_position, (int, float)) and (range_position < 0.08 or range_position > 0.92):
        reasons.append("价格处于近端区间边缘，追价风险较高")

    return {
        "symbol": symbol,
        "timeframes": list(derived_by_tf.keys()),
        "derived_features_by_tf": derived_by_tf,
        "cross_tf_summary": cross_tf_summary,
        "tradeable_gate": {
            "tradeable": tradeable,
            "reasons": reasons or ["未发现明显交易性门控问题"],
        },
    }


def _derive_tf_features(snap: dict[str, Any]) -> dict[str, Any]:
    latest = snap.get("latest") or {}
    history = snap.get("history") or []
    closes = [_to_float(h.get("close")) for h in history if _to_float(h.get("close")) is not None]
    highs = [_to_float(h.get("high")) for h in history if _to_float(h.get("high")) is not None]
    lows = [_to_float(h.get("low")) for h in history if _to_float(h.get("low")) is not None]

    ema8 = _ema(closes, 8)
    ema21 = _ema(closes, 21)
    ema55 = _ema(closes, 55)
    close_val = _to_float(latest.get("close")) or (closes[-1] if closes else None)
    atr = _to_float(latest.get("atr_14"))
    bb_bw = _to_float(latest.get("bb_bandwidth"))
    rsi = _to_float(latest.get("rsi_14"))
    stoch_k = _to_float(latest.get("stoch_rsi_k"))
    stoch_d = _to_float(latest.get("stoch_rsi_d"))
    macd_hist = _to_float(latest.get("macd_hist"))
    obv = _to_float(latest.get("obv"))
    volume_zscore = _to_float(latest.get("volume_zscore"))

    ema_spread_pct = None
    if ema8 is not None and ema55 is not None and close_val not in (None, 0):
        ema_spread_pct = (ema8 - ema55) / close_val

    ema21_series = _ema_series(closes, 21)
    ema21_slope_pct = None
    if len(ema21_series) >= 6 and close_val not in (None, 0):
        prev = ema21_series[-6]
        curr = ema21_series[-1]
        if prev not in (None, 0) and curr is not None:
            ema21_slope_pct = (curr - prev) / close_val

    momentum_alignment = _momentum_alignment_score(rsi, stoch_k, stoch_d, macd_hist)
    vol_state = _volatility_state(atr, close_val, bb_bw)
    range_stats = _range_stats(closes, highs, lows)
    swing_levels = _swing_levels(closes)
    obv_div = _obv_price_divergence_flag(history, obv)

    return {
        "trend_strength": {
            "ema_spread_pct": _round_or_none(ema_spread_pct, 6),
            "ema21_slope_pct": _round_or_none(ema21_slope_pct, 6),
            "ema_ribbon_trend": latest.get("ema_ribbon_trend"),
        },
        "momentum_alignment": momentum_alignment,
        "volatility_state": vol_state,
        "range_stats": range_stats,
        "swing_levels": swing_levels,
        "obv_price_divergence_flag": obv_div,
        "raw_refs": {
            "close": _round_or_none(close_val, 6),
            "atr_14": _round_or_none(atr, 6),
            "bb_bandwidth": _round_or_none(bb_bw, 6),
            "volume_zscore": _round_or_none(volume_zscore, 4),
        },
    }


def _build_cross_tf_summary(derived_by_tf: dict[str, Any]) -> dict[str, Any]:
    tfs_order = ["4h", "1h", "15m", "5m", "1m"]
    trend_dirs: dict[str, str] = {}
    momentum_scores: dict[str, int] = {}
    votes: list[str] = []

    for tf in tfs_order:
        feat = derived_by_tf.get(tf)
        if not isinstance(feat, dict):
            continue
        trend = (((feat.get("trend_strength") or {}).get("ema_ribbon_trend")) or "UNKNOWN")
        trend_dirs[tf] = str(trend)
        mom_score = int((((feat.get("momentum_alignment") or {}).get("score_0_3")) or 0))
        momentum_scores[tf] = mom_score
        if trend in ("UP", "DOWN"):
            votes.append(trend)

    dominant_tf = "1m"
    for tf in tfs_order:
        if trend_dirs.get(tf) in ("UP", "DOWN"):
            dominant_tf = tf
            break

    up_count = sum(1 for v in votes if v == "UP")
    down_count = sum(1 for v in votes if v == "DOWN")
    total_votes = max(len(votes), 1)
    alignment_score = int(round(max(up_count, down_count) / total_votes * 100))
    dominant_direction = "MIXED"
    if up_count > down_count:
        dominant_direction = "UP"
    elif down_count > up_count:
        dominant_direction = "DOWN"

    return {
        "trend_dirs": trend_dirs,
        "momentum_alignment_scores": momentum_scores,
        "alignment_score": alignment_score,
        "dominant_tf": dominant_tf,
        "dominant_direction": dominant_direction,
    }


def _build_alerts_digest(recent_alerts: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    for alert in recent_alerts or []:
        ts = _coerce_datetime(alert.get("ts"))
        normalized.append({
            "symbol": alert.get("symbol"),
            "alert_type": str(alert.get("alert_type") or ""),
            "severity": str(alert.get("severity") or ""),
            "reason": _clean_text(str(alert.get("reason") or "")),
            "ts": ts,
        })

    count_1h = 0
    count_4h = 0
    count_15m = 0
    type_counts: dict[str, int] = {}
    top_events: list[dict[str, Any]] = []
    severity_rank = {"critical": 4, "high": 3, "medium": 2, "low": 1}

    for a in normalized:
        ts = a.get("ts")
        age_min = None
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_min = max(0, int((now - ts).total_seconds() // 60))
            if age_min <= 60:
                count_1h += 1
            if age_min <= 240:
                count_4h += 1
            if age_min <= 15:
                count_15m += 1
        a_type = a["alert_type"] or "UNKNOWN"
        type_counts[a_type] = type_counts.get(a_type, 0) + 1
        top_events.append({
            "alert_type": a_type,
            "severity": a["severity"] or "unknown",
            "reason_short": _truncate(a["reason"], 90),
            "age_min": age_min,
        })

    top_events.sort(
        key=lambda x: (
            -(severity_rank.get(str(x.get("severity") or "").lower(), 0)),
            x.get("age_min") if isinstance(x.get("age_min"), int) else 10**9,
        )
    )
    top_events = top_events[:3]
    dominant_types = [k for k, _ in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]
    alerts_burst = count_15m >= 4

    return {
        "count_1h": count_1h,
        "count_4h": count_4h,
        "top_events": top_events,
        "dominant_types": dominant_types,
        "alerts_burst": alerts_burst,
    }


def _build_funding_deltas(
    funding_current: dict[str, Any] | None,
    funding_history: list[Any] | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    result = {
        "funding_rate": _to_float((funding_current or {}).get("last_funding_rate")),
        "funding_delta_24h": None,
        "open_interest": _to_float((funding_current or {}).get("open_interest")),
        "oi_delta_1h": None,
        "oi_delta_4h": None,
        "oi_spike_flag": False,
    }
    history = funding_history or []
    if not history:
        return result

    rows: list[dict[str, Any]] = []
    for item in history:
        rows.append({
            "ts": _coerce_datetime(getattr(item, "ts", None) if not isinstance(item, dict) else item.get("ts")),
            "last_funding_rate": _to_float(getattr(item, "last_funding_rate", None) if not isinstance(item, dict) else item.get("last_funding_rate")),
            "open_interest": _to_float(getattr(item, "open_interest", None) if not isinstance(item, dict) else item.get("open_interest")),
        })
    rows = [r for r in rows if isinstance(r.get("ts"), datetime)]
    rows.sort(key=lambda r: r["ts"])
    if not rows:
        return result

    latest = rows[-1]
    if result["funding_rate"] is None:
        result["funding_rate"] = latest.get("last_funding_rate")
    if result["open_interest"] is None:
        result["open_interest"] = latest.get("open_interest")

    one_h_ref = _nearest_row_at_or_before(rows, now - timedelta(hours=1))
    four_h_ref = _nearest_row_at_or_before(rows, now - timedelta(hours=4))
    twentyfour_h_ref = _nearest_row_at_or_before(rows, now - timedelta(hours=24))

    latest_oi = _to_float(latest.get("open_interest"))
    if twentyfour_h_ref:
        result["funding_delta_24h"] = _diff(_to_float(latest.get("last_funding_rate")), _to_float(twentyfour_h_ref.get("last_funding_rate")))
    if one_h_ref:
        result["oi_delta_1h"] = _diff(latest_oi, _to_float(one_h_ref.get("open_interest")))
    if four_h_ref:
        result["oi_delta_4h"] = _diff(latest_oi, _to_float(four_h_ref.get("open_interest")))

    if latest_oi and latest_oi != 0 and isinstance(result["oi_delta_1h"], (int, float)):
        pct = abs(result["oi_delta_1h"]) / max(abs(latest_oi), 1e-12)
        result["oi_spike_flag"] = pct >= 0.05
    return result


def _build_youtube_radar(
    *,
    symbol: str,
    youtube_consensus: Any | None,
    youtube_insights: list[Any],
    now: datetime,
) -> dict[str, Any]:
    row = youtube_consensus
    if row is None:
        return {
            "available": False,
            "stale": True,
            "consensus_bias": None,
            "confidence": None,
            "consensus_levels": {"support": [], "resistance": []},
            "consensus_setups": [],
            "disagreements": [],
            "top_voices": [],
            "risk_notes": [],
            "source_count": 0,
            "updated_at": None,
            "consensus_updated_at": None,
            "latest_insight_at": None,
            "fresh_content_after_consensus": False,
            "fresh_content_lag_hours": None,
        }

    consensus_json = getattr(row, "consensus_json", None) if not isinstance(row, dict) else row.get("consensus_json", row)
    row_created_at = _coerce_datetime(getattr(row, "created_at", None) if not isinstance(row, dict) else row.get("created_at"))
    if not isinstance(consensus_json, dict):
        consensus_json = {}
    payload_updated_at = _coerce_datetime(consensus_json.get("updated_at"))
    created_at = _pick_latest_datetime(row_created_at, payload_updated_at)

    key_levels = consensus_json.get("key_levels") or {}
    support = [_to_number(x) for x in (key_levels.get("support") or [])]
    resistance = [_to_number(x) for x in (key_levels.get("resistance") or [])]
    support = [x for x in support if x is not None][:3]
    resistance = [x for x in resistance if x is not None][:3]

    scenarios = consensus_json.get("scenarios") or []
    consensus_setups: list[str] = []
    risk_notes: list[str] = []
    for sc in scenarios[:3]:
        if not isinstance(sc, dict):
            continue
        then = _clean_text(str(sc.get("then") or ""))
        risk = _clean_text(str(sc.get("risk") or ""))
        if then:
            consensus_setups.append(_truncate(then, 100))
        if risk:
            risk_notes.append(_truncate(risk, 100))

    disagreements = []
    for d in (consensus_json.get("disagreements") or [])[:2]:
        if not isinstance(d, dict):
            continue
        disagreements.append({
            "analyst": _truncate(_clean_text(str(d.get("analyst") or "")), 40),
            "view": _truncate(_clean_text(str(d.get("view") or "")), 100),
        })

    top_voices = _extract_top_voices(youtube_insights)
    latest_insight_at = _latest_created_at(youtube_insights)
    stale = True
    if created_at:
        stale = (now - created_at).total_seconds() > 12 * 3600
    fresh_content_after_consensus = False
    fresh_content_lag_hours = None
    if created_at and latest_insight_at:
        lag_hours = (latest_insight_at - created_at).total_seconds() / 3600.0
        # tolerate small clock/order jitter
        if lag_hours > (5.0 / 60.0):
            fresh_content_after_consensus = True
            fresh_content_lag_hours = round(lag_hours, 2)

    source_video_ids = None
    if not isinstance(row, dict):
        source_video_ids = getattr(row, "source_video_ids", None)
    else:
        source_video_ids = row.get("source_video_ids")
    source_count = len(source_video_ids or [])
    if not source_count:
        source_count = len(youtube_insights)

    return {
        "available": True,
        "stale": stale,
        "symbol": symbol,
        "consensus_bias": consensus_json.get("consensus_bias"),
        "confidence": consensus_json.get("confidence"),
        "consensus_levels": {
            "support": support,
            "resistance": resistance,
        },
        "consensus_setups": consensus_setups[:3],
        "disagreements": disagreements,
        "top_voices": top_voices[:2],
        "risk_notes": risk_notes[:3],
        "source_count": source_count,
        "updated_at": created_at.isoformat() if created_at else None,
        "consensus_updated_at": created_at.isoformat() if created_at else None,
        "latest_insight_at": latest_insight_at.isoformat() if latest_insight_at else None,
        "fresh_content_after_consensus": fresh_content_after_consensus,
        "fresh_content_lag_hours": fresh_content_lag_hours,
    }


def _apply_context_clipping(alerts_digest: dict[str, Any], youtube_radar: dict[str, Any]) -> dict[str, Any]:
    clip_steps: list[str] = []

    if isinstance(alerts_digest.get("top_events"), list):
        alerts_digest["top_events"] = alerts_digest["top_events"][:3]
    if isinstance(youtube_radar.get("top_voices"), list):
        youtube_radar["top_voices"] = youtube_radar["top_voices"][:2]
    levels = youtube_radar.get("consensus_levels") or {}
    if isinstance(levels, dict):
        levels["support"] = list(levels.get("support") or [])[:3]
        levels["resistance"] = list(levels.get("resistance") or [])[:3]
        youtube_radar["consensus_levels"] = levels

    alerts_chars = len(json.dumps(alerts_digest, ensure_ascii=False, separators=(",", ":")))
    if alerts_chars > ALERTS_DIGEST_MAX_CHARS and isinstance(alerts_digest.get("top_events"), list):
        for ev in alerts_digest["top_events"]:
            if isinstance(ev, dict) and isinstance(ev.get("reason_short"), str):
                ev["reason_short"] = _truncate(ev["reason_short"], 50)
        clip_steps.append("alerts:truncate_reason_short")
        alerts_chars = len(json.dumps(alerts_digest, ensure_ascii=False, separators=(",", ":")))
    if alerts_chars > ALERTS_DIGEST_MAX_CHARS:
        alerts_digest["top_events"] = (alerts_digest.get("top_events") or [])[:2]
        clip_steps.append("alerts:top_events_2")
        alerts_chars = len(json.dumps(alerts_digest, ensure_ascii=False, separators=(",", ":")))

    before = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    after = before
    if after > YOUTUBE_RADAR_MAX_CHARS:
        _truncate_youtube_radar_text_fields(youtube_radar, note_limit=80, voice_view_limit=70)
        clip_steps.append("youtube:truncate_long_text_fields")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    if after > YOUTUBE_RADAR_MAX_CHARS:
        youtube_radar["disagreements"] = [
            {"analyst": d.get("analyst"), "view": _truncate(str(d.get("view") or ""), 60)}
            for d in (youtube_radar.get("disagreements") or [])[:1]
            if isinstance(d, dict)
        ]
        clip_steps.append("youtube:disagreements_1")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    if after > YOUTUBE_RADAR_MAX_CHARS:
        youtube_radar["risk_notes"] = (youtube_radar.get("risk_notes") or [])[:2]
        clip_steps.append("youtube:risk_notes_2")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    if after > YOUTUBE_RADAR_MAX_CHARS:
        trimmed_voices = []
        for v in (youtube_radar.get("top_voices") or [])[:2]:
            if not isinstance(v, dict):
                continue
            trimmed_voices.append({
                "analyst": v.get("analyst"),
                "bias": v.get("bias"),
                "conviction": v.get("conviction"),
                "weight": v.get("weight"),
                "key_level": v.get("key_level"),
            })
        youtube_radar["top_voices"] = trimmed_voices
        clip_steps.append("youtube:top_voices_compact")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    if after > YOUTUBE_RADAR_MAX_CHARS:
        youtube_radar["disagreements"] = []
        clip_steps.append("youtube:drop_disagreements")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))
    if after > YOUTUBE_RADAR_MAX_CHARS:
        youtube_radar["risk_notes"] = []
        clip_steps.append("youtube:drop_risk_notes")
        after = len(json.dumps(youtube_radar, ensure_ascii=False, separators=(",", ":")))

    return {
        "youtube_radar_chars_before_clip": before,
        "youtube_radar_chars_after_clip": after,
        "clip_steps_applied": clip_steps,
        "alerts_digest_chars": alerts_chars,
    }


def _build_data_quality(
    *,
    snapshots: dict[str, dict[str, Any]],
    funding_current: dict[str, Any] | None,
    youtube_radar: dict[str, Any],
    alerts_digest: dict[str, Any],
    now: datetime,
    expected_timeframes: list[str],
) -> dict[str, Any]:
    snapshot_age_sec: dict[str, int | None] = {}
    missing: list[str] = []
    notes: list[str] = []
    stale_timeframes: list[str] = []

    freshness_thresholds_sec: dict[str, int] = {
        "1m": 10 * 60,
        "5m": 30 * 60,
        "15m": 90 * 60,
        "1h": 4 * 3600,
        "4h": 12 * 3600,
    }

    for tf in expected_timeframes:
        snap = snapshots.get(tf)
        if not snap:
            missing.append(tf)
            snapshot_age_sec[tf] = None
            continue
        latest = snap.get("latest") or {}
        ts = _coerce_datetime(latest.get("ts"))
        if not ts:
            snapshot_age_sec[tf] = None
            notes.append(f"{tf} 缺少时间戳")
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_sec = max(0, int((now - ts).total_seconds()))
        snapshot_age_sec[tf] = age_sec
        max_age = freshness_thresholds_sec.get(tf)
        if isinstance(max_age, int) and age_sec > max_age:
            stale_timeframes.append(tf)

    funding_ts = _coerce_datetime((funding_current or {}).get("ts"))
    funding_stale = True
    if funding_ts:
        funding_stale = (now - funding_ts).total_seconds() > 2 * 3600

    youtube_stale = bool(youtube_radar.get("stale", True))
    alerts_burst = bool(alerts_digest.get("alerts_burst", False))

    overall = "GOOD"
    critical_missing = any(tf in missing for tf in ("4h", "1h", "15m"))
    one_m_age = snapshot_age_sec.get("1m")
    stale_critical = any(tf in stale_timeframes for tf in ("1h", "4h", "15m"))
    if critical_missing or one_m_age is None or (isinstance(one_m_age, int) and one_m_age > 10 * 60):
        overall = "POOR"
    elif stale_critical and (
        (isinstance(snapshot_age_sec.get("1h"), int) and snapshot_age_sec["1h"] > 18 * 3600)
        or (isinstance(snapshot_age_sec.get("4h"), int) and snapshot_age_sec["4h"] > 24 * 3600)
    ):
        overall = "POOR"
    elif missing or funding_stale or youtube_stale or stale_timeframes:
        overall = "DEGRADED"

    if missing:
        notes.append(f"缺失周期: {', '.join(missing)}")
    if funding_stale:
        notes.append("资金费率/持仓数据较旧")
    if stale_timeframes:
        details = []
        for tf in stale_timeframes:
            age = snapshot_age_sec.get(tf)
            if isinstance(age, int):
                details.append(f"{tf}={age}s")
            else:
                details.append(tf)
        notes.append(f"周期数据延迟: {', '.join(details)}")
    if youtube_stale:
        if bool(youtube_radar.get("fresh_content_after_consensus")):
            lag = youtube_radar.get("fresh_content_lag_hours")
            lag_text = f"（已有较新洞察未纳入共识，领先约{lag}小时）" if isinstance(lag, (int, float)) else "（已有较新洞察未纳入共识）"
            notes.append(f"YouTube 共识较旧{lag_text}")
        else:
            notes.append("YouTube 共识较旧或缺失")
    if alerts_burst:
        notes.append("近期告警密集，噪声风险上升")

    return {
        "snapshot_age_sec": snapshot_age_sec,
        "stale_timeframes": stale_timeframes,
        "missing_timeframes": missing,
        "funding_stale": funding_stale,
        "youtube_stale": youtube_stale,
        "youtube_has_newer_insights": bool(youtube_radar.get("fresh_content_after_consensus")),
        "alerts_burst": alerts_burst,
        "overall": overall,
        "notes": notes or ["数据质量正常"],
    }


def _extract_top_voices(youtube_insights: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ins in youtube_insights:
        vta = getattr(ins, "analyst_view_json", None) if not isinstance(ins, dict) else ins.get("analyst_view_json")
        if not isinstance(vta, dict):
            continue
        meta = vta.get("meta") or {}
        mv = vta.get("market_view") or {}
        levels = vta.get("levels") or {}
        key_level = None
        res = levels.get("resistance") or []
        sup = levels.get("support") or []
        first_level = None
        if isinstance(res, list) and res:
            first_level = res[0]
        elif isinstance(sup, list) and sup:
            first_level = sup[0]
        if isinstance(first_level, dict):
            key_level = _to_number(first_level.get("level"))
        elif isinstance(first_level, (int, float, str)):
            key_level = _to_number(first_level)

        weight = _to_float(vta.get("computed_weight"))
        rows.append({
            "analyst": _truncate(_clean_text(str(meta.get("analyst") or "未知频道")), 40),
            "bias": mv.get("bias_1_7d") or mv.get("bias_1_4w"),
            "conviction": mv.get("conviction"),
            "one_liner": _truncate(_clean_text(str(mv.get("one_liner") or "")), 90),
            "weight": _round_or_none(weight, 4),
            "key_level": key_level,
        })
    rows.sort(key=lambda x: (-(x.get("weight") or 0), x.get("analyst") or ""))
    return rows[:2]


def _truncate_youtube_radar_text_fields(youtube_radar: dict[str, Any], *, note_limit: int, voice_view_limit: int) -> None:
    youtube_radar["consensus_setups"] = [_truncate(str(x), note_limit) for x in (youtube_radar.get("consensus_setups") or [])]
    youtube_radar["risk_notes"] = [_truncate(str(x), note_limit) for x in (youtube_radar.get("risk_notes") or [])]
    trimmed_disagreements = []
    for d in (youtube_radar.get("disagreements") or []):
        if not isinstance(d, dict):
            continue
        trimmed_disagreements.append({
            "analyst": _truncate(str(d.get("analyst") or ""), 40),
            "view": _truncate(str(d.get("view") or ""), note_limit),
        })
    youtube_radar["disagreements"] = trimmed_disagreements
    trimmed_voices = []
    for v in (youtube_radar.get("top_voices") or []):
        if not isinstance(v, dict):
            continue
        item = dict(v)
        if "one_liner" in item:
            item["one_liner"] = _truncate(str(item.get("one_liner") or ""), voice_view_limit)
        trimmed_voices.append(item)
    youtube_radar["top_voices"] = trimmed_voices


def _ema(values: list[float], span: int) -> float | None:
    series = _ema_series(values, span)
    return series[-1] if series else None


def _ema_series(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    out: list[float] = []
    ema_val = values[0]
    out.append(ema_val)
    for x in values[1:]:
        ema_val = alpha * x + (1.0 - alpha) * ema_val
        out.append(ema_val)
    return out


def _momentum_alignment_score(rsi: float | None, stoch_k: float | None, stoch_d: float | None, macd_hist: float | None) -> dict[str, Any]:
    score = 0
    rsi_dir = "NEUTRAL"
    if isinstance(rsi, (int, float)):
        if rsi >= 55:
            rsi_dir = "UP"
            score += 1
        elif rsi <= 45:
            rsi_dir = "DOWN"
            score += 1
    stoch_dir = "NEUTRAL"
    if isinstance(stoch_k, (int, float)) and isinstance(stoch_d, (int, float)):
        if stoch_k >= 50 and stoch_d >= 50:
            stoch_dir = "UP"
            score += 1
        elif stoch_k <= 50 and stoch_d <= 50:
            stoch_dir = "DOWN"
            score += 1
    macd_dir = "NEUTRAL"
    if isinstance(macd_hist, (int, float)):
        if macd_hist > 0:
            macd_dir = "UP"
            score += 1
        elif macd_hist < 0:
            macd_dir = "DOWN"
            score += 1
    return {
        "score_0_3": score,
        "rsi_dir": rsi_dir,
        "stoch_dir": stoch_dir,
        "macd_dir": macd_dir,
    }


def _volatility_state(atr: float | None, close_val: float | None, bb_bw: float | None) -> str:
    atr_pct = (atr / close_val) if (isinstance(atr, (int, float)) and isinstance(close_val, (int, float)) and close_val) else None
    if atr_pct is None and bb_bw is None:
        return "UNKNOWN"
    score = 0
    if atr_pct is not None:
        if atr_pct > 0.015:
            score += 2
        elif atr_pct > 0.008:
            score += 1
    if isinstance(bb_bw, (int, float)):
        if bb_bw > 0.08:
            score += 2
        elif bb_bw > 0.04:
            score += 1
    if score >= 3:
        return "HIGH_VOL"
    if score <= 1:
        return "LOW_VOL"
    return "NORMAL"


def _range_stats(closes: list[float], highs: list[float], lows: list[float]) -> dict[str, Any]:
    if not closes or not highs or not lows:
        return {
            "range_high": None,
            "range_low": None,
            "range_change_pct": None,
            "range_position": None,
            "max_favorable_move_pct": None,
            "max_adverse_move_pct": None,
        }
    range_high = max(highs)
    range_low = min(lows)
    first_close = closes[0]
    last_close = closes[-1]
    range_change_pct = ((last_close - first_close) / first_close) if first_close else None
    denom = (range_high - range_low) if range_high is not None and range_low is not None else None
    range_position = ((last_close - range_low) / denom) if denom and denom > 0 else None
    max_fav = ((max(highs) - first_close) / first_close) if first_close else None
    max_adv = ((min(lows) - first_close) / first_close) if first_close else None
    return {
        "range_high": _round_or_none(range_high, 6),
        "range_low": _round_or_none(range_low, 6),
        "range_change_pct": _round_or_none(range_change_pct, 6),
        "range_position": _round_or_none(range_position, 4),
        "max_favorable_move_pct": _round_or_none(max_fav, 6),
        "max_adverse_move_pct": _round_or_none(max_adv, 6),
    }


def _swing_levels(closes: list[float]) -> dict[str, list[float]]:
    if len(closes) < 5:
        return {"supports": [], "resistances": []}
    highs: list[float] = []
    lows: list[float] = []
    for i in range(2, len(closes) - 2):
        c = closes[i]
        left = closes[i - 2 : i]
        right = closes[i + 1 : i + 3]
        if all(c >= x for x in left + right):
            highs.append(c)
        if all(c <= x for x in left + right):
            lows.append(c)
    highs = _dedupe_levels(highs)[-2:]
    lows = _dedupe_levels(lows)[-2:]
    return {
        "supports": [round(x, 6) for x in lows],
        "resistances": [round(x, 6) for x in highs],
    }


def _obv_price_divergence_flag(history: list[dict[str, Any]], obv_latest: float | None) -> str:
    if len(history) < 8 or obv_latest is None:
        return "INSUFFICIENT_DATA"
    closes = [_to_float(h.get("close")) for h in history[-8:]]
    closes = [x for x in closes if x is not None]
    if len(closes) < 8:
        return "INSUFFICIENT_DATA"
    trend = closes[-1] - closes[0]
    if abs(trend) < (abs(closes[-1]) * 0.002):
        return "NONE"
    return "NONE"


def _nearest_row_at_or_before(rows: list[dict[str, Any]], target: datetime) -> dict[str, Any] | None:
    candidate = None
    for row in rows:
        ts = row.get("ts")
        if not isinstance(ts, datetime):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts <= target:
            candidate = row
        else:
            break
    return candidate


def _diff(a: float | None, b: float | None) -> float | None:
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return None
    return a - b


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_number(value: Any) -> float | None:
    return _to_float(value)


def _round_or_none(value: float | None, digits: int) -> float | None:
    if not isinstance(value, (int, float)) or math.isnan(float(value)) or math.isinf(float(value)):
        return None
    return round(float(value), digits)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _pick_latest_datetime(*values: datetime | None) -> datetime | None:
    valid = [v for v in values if isinstance(v, datetime)]
    if not valid:
        return None
    return max(valid)


def _latest_created_at(rows: list[Any]) -> datetime | None:
    latest: datetime | None = None
    for row in rows or []:
        if isinstance(row, dict):
            dt = _coerce_datetime(row.get("created_at"))
        else:
            dt = _coerce_datetime(getattr(row, "created_at", None))
        if dt and (latest is None or dt > latest):
            latest = dt
    return latest


_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_text(text: str) -> str:
    text = _CTRL_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 1:
        return text[:max_len]
    return text[: max_len - 1] + "…"


def _dedupe_levels(values: list[float]) -> list[float]:
    if not values:
        return []
    values = sorted(values)
    deduped = [values[0]]
    for v in values[1:]:
        base = deduped[-1]
        tol = max(abs(base), 1.0) * 0.003
        if abs(v - base) > tol:
            deduped.append(v)
    return deduped
