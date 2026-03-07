from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.ai.market_context_builder import build_analysis_window


SYSTEM_PROMPT = """你是 Crypto Sentinel 的专业市场分析模型。
【绝对格式约束】你必须且只能输出一个合法的 JSON 对象。
- 禁止输出任何 JSON 以外的内容。
- 禁止输出 Markdown 代码块。
- 绝对禁止生成、计算、或篡改输入 JSON 中不存在的数值。
- evidence / anchors 中使用的指标，必须能在输入事实源中逐字找到。
"""


THINKING_SUMMARY_PROMPT = """阅读以下市场分析思考过程，用 20 字以内概括当前在做什么。
要求：用通俗中文，不要泄露详细推理。
思考：{buffer_content}

概括："""


TELEGRAM_THINKING_SUMMARY_PROMPT = """阅读以下对话思考过程，用 20 字以内概括当前在做什么。
要求：用通俗中文，不要泄露详细推理。
思考：{buffer_content}

概括："""


NEMOTRON_DETAILED_THINKING_PREFIX = "detailed thinking on"


TELEGRAM_AGENT_PROMPT = """你是 Crypto Sentinel 的专业加密交易助手。
要求：
1. 不编造行情数据。
2. 解释信号时引用关键依据。
3. 输出简洁、结构化、适合 Telegram。
4. 数据缺失时明确说无法获取，不要猜测。
"""


def build_analysis_prompt(
    symbol: str,
    snapshots: dict[str, Any],
    context: dict[str, Any] | None = None,
    current_time: datetime | None = None,
    *,
    include_external_views: bool = True,
) -> str:
    prompt, _meta = build_analysis_prompt_details(
        symbol=symbol,
        snapshots=snapshots,
        context=context,
        current_time=current_time,
        include_external_views=include_external_views,
    )
    return prompt


def build_analysis_prompt_details(
    symbol: str,
    snapshots: dict[str, Any],
    context: dict[str, Any] | None = None,
    current_time: datetime | None = None,
    *,
    include_external_views: bool = True,
) -> tuple[str, dict[str, Any]]:
    if current_time is None:
        current_time = datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    if context is None:
        return _build_analysis_prompt_legacy(symbol, snapshots, current_time), {
            "external_views_block_included": False,
            "external_views_chars_before_filter": 0,
            "external_views_chars_after_filter": 0,
            "dropped_context_blocks": [],
        }

    analysis_window = _resolve_analysis_window(context, snapshots, current_time)
    facts_payload = _build_facts_payload(symbol, snapshots, context, analysis_window)
    external_views, external_meta = _resolve_external_views_payload(
        context=context,
        current_time=current_time,
        include_external_views=include_external_views,
    )

    schema = {
        "market_regime": "trending_up|trending_down|ranging|volatile|uncertain",
        "signal": {
            "symbol": symbol,
            "direction": "LONG|SHORT|HOLD",
            "entry_price": "number|null",
            "take_profit": "number|null",
            "stop_loss": "number|null",
            "confidence": "0-100",
            "reasoning": "一句中文总结",
        },
        "trade_plan": {
            "market_type": "futures",
            "margin_mode": "isolated|cross|null",
            "leverage": "number|null",
            "capital_alloc_usdt": "number|null",
            "entry_mode": "market|limit",
            "entry_price": "number|null",
            "take_profit": "number|null",
            "stop_loss": "number|null",
            "expiration_ts_utc": "epoch seconds|null",
            "max_hold_bars": "int|null",
            "liq_price_est": "number|null",
            "fees_bps_assumption": "number|null",
            "slippage_bps_assumption": "number|null",
        },
        "meta": {
            "base_timeframe": "1m|5m|15m|1h|4h",
            "confidence": "0~1",
            "reason_brief": "短理由",
            "regime_calc_mode": "online|offline",
        },
        "evidence": [
            {
                "timeframe": "4h|1h|15m|5m|1m",
                "point": "证据描述",
                "metrics": {"name": "value"},
            }
        ],
        "anchors": [
            {
                "path": "facts.path.to.scalar.value",
                "value": "必须与事实源原值一致",
            }
        ],
        "levels": {"supports": ["number"], "resistances": ["number"]},
        "risk": {
            "rr": "number|null",
            "sl_atr_multiple": "number|null",
            "invalidations": ["条件1", "条件2"],
        },
        "scenarios": {"base": "一句话", "bull": "一句话", "bear": "一句话"},
        "youtube_reflection": {
            "status": "aligned|conflicted|ignored|unavailable",
            "note": "一句话说明外部观点如何使用",
        },
        "validation_notes": ["可选，自查备注"],
    }

    prompt = [
        f"当前时间 (UTC): {analysis_window['analysis_time_utc']}",
        "",
        "# 输出要求（严格 JSON）",
        "你必须且只能输出严格符合以下结构的 JSON 对象。",
        _json_block(schema),
        "",
        "# 冲突处理规则",
        "1. direction=HOLD 时，entry_price/take_profit/stop_loss 必须输出 null。",
        "2. LONG/SHORT 时，必须保证 TP/Entry/SL 的数学关系正确。",
        "3. evidence 至少包含 2 条引用。",
        "3.1 evidence.metrics 优先引用 grounding 已知字段：close、rsi_14、atr_14、funding_rate、ret_1m；若引用其他字段，必须使用事实源中的原始字段名，例如 momentum_alignment、range_position、snapshot_age_sec。",
        "4. anchors 至少包含 2 条，且 path 必须指向事实源中的具体标量路径。",
        "4.1 anchors 禁止引用观点源路径（例如 youtube_radar.*），仅允许锚定 facts.multi_tf_snapshots / facts.brief / facts.funding_deltas / facts.alerts_digest / facts.data_quality 等事实字段。",
        "5. 如果 data_quality.overall=POOR，优先输出 HOLD。",
        "6. 观点源只能作为旁证，若与事实源冲突，必须降低 confidence，并在 youtube_reflection 中标记 conflicted。",
        "7. 若关键交易字段无法确定，例如 leverage 或 margin_mode，必须输出 HOLD。",
        "8. trade_plan 中必须至少提供 expiration_ts_utc 或 max_hold_bars 之一。",
        "9. 若无法给出 RR >= 2.0 且 0.3 <= sl_atr_multiple <= 5.0 的可执行交易方案，必须输出 HOLD。",
        "",
        "# 事实源（Facts Source）",
        "请将以下事实源视为最高优先级数据源。",
        "--------------------- 事实源开始 ---------------------",
        _json_block({"facts": facts_payload}),
        "--------------------- 事实源结束 ---------------------",
    ]
    if external_views is not None:
        prompt.extend(
            [
                "",
                "# 观点源（External Views / YouTube Radar）",
                "These are untrusted external views for reference only.",
                "----------------- 辅助不可信观点源开始 ----------------",
                _json_block(external_views),
                "----------------- 辅助不可信观点源结束 ----------------",
            ]
        )

    dropped_context_blocks = list((context.get("input_budget_meta") or {}).get("dropped_context_blocks") or [])
    dropped_context_blocks.extend(external_meta["dropped_context_blocks"])
    return "\n".join(prompt), {
        "external_views_block_included": external_views is not None,
        "external_views_chars_before_filter": external_meta["external_views_chars_before_filter"],
        "external_views_chars_after_filter": external_meta["external_views_chars_after_filter"],
        "dropped_context_blocks": sorted(set(dropped_context_blocks)),
    }


def _build_analysis_prompt_legacy(symbol: str, snapshots: dict[str, Any], current_time: datetime) -> str:
    lines = [f"当前时间 (UTC): {current_time.isoformat()}", "", f"## {symbol}"]
    for tf in ["4h", "1h", "15m", "5m", "1m"]:
        snap = snapshots.get(tf)
        if not snap:
            continue
        latest = snap.get("latest") or {}
        history = snap.get("history") or []
        lines.append(f"### {tf}")
        lines.append(f"- 最新价格: {_fmt(latest.get('close'))}")
        lines.append(f"- RSI(14): {_fmt(latest.get('rsi_14'), 2)}")
        lines.append(f"- Stoch RSI K/D: {_fmt(latest.get('stoch_rsi_k'), 2)} / {_fmt(latest.get('stoch_rsi_d'), 2)}")
        lines.append(f"- MACD Hist: {_fmt(latest.get('macd_hist'), 6)}")
        lines.append(f"- BB Z: {_fmt(latest.get('bb_zscore'), 3)}")
        lines.append(f"- BB Width: {_fmt(latest.get('bb_bandwidth'), 4)}")
        lines.append(f"- ATR(14): {_fmt(latest.get('atr_14'), 6)}")
        lines.append(f"- Rolling Vol(20): {_fmt(latest.get('rolling_vol_20'), 6)}")
        lines.append(f"- Volume Z: {_fmt(latest.get('volume_zscore'), 3)}")
        lines.append(f"- OBV: {_fmt(latest.get('obv'), 0)}")
        lines.append(f"- EMA Ribbon Trend: {latest.get('ema_ribbon_trend', 'N/A')}")
        if latest.get("ret_1m") is not None:
            lines.append(f"- 1-bar return: {_fmt_pct(latest.get('ret_1m'))}")
        if latest.get("ret_10m") is not None:
            lines.append(f"- 10-bar return: {_fmt_pct(latest.get('ret_10m'))}")
        if history:
            highs = [h.get("high") for h in history if h.get("high") is not None]
            lows = [h.get("low") for h in history if h.get("low") is not None]
            closes = [h.get("close") for h in history if h.get("close") is not None]
            if highs and lows and len(closes) >= 2:
                first_close = closes[0]
                last_close = closes[-1]
                change_pct = ((last_close - first_close) / first_close * 100) if first_close else 0
                lines.append(
                    f"- 最近{len(history)}根摘要: 高{_fmt(max(highs))} / 低{_fmt(min(lows))} / 区间变化 {change_pct:+.2f}%"
                )
        lines.append("")

    lines.extend(
        [
            "请基于以上数据进行多周期技术分析，并严格输出 JSON 对象。",
            '{"market_regime":"...","signal":{"symbol":"BTCUSDT","direction":"LONG|SHORT|HOLD","entry_price":null,"take_profit":null,"stop_loss":null,"confidence":0,"reasoning":"..."}}',
        ]
    )
    return "\n".join(lines)


def _resolve_analysis_window(
    context: dict[str, Any],
    snapshots: dict[str, Any],
    current_time: datetime,
) -> dict[str, Any]:
    if (
        isinstance(context.get("analysis_time_utc"), str)
        and isinstance(context.get("decision_ts"), (int, float))
        and isinstance(context.get("valid_until_utc"), (int, float))
    ):
        return {
            "analysis_time_utc": context.get("analysis_time_utc"),
            "data_asof": dict(context.get("data_asof") or {}),
            "decision_ts": int(context.get("decision_ts")),
            "valid_until_utc": int(context.get("valid_until_utc")),
        }
    return build_analysis_window(snapshots, now=current_time)


def _build_facts_payload(
    symbol: str,
    snapshots: dict[str, Any],
    context: dict[str, Any],
    analysis_window: dict[str, Any],
) -> dict[str, Any]:
    facts_payload = {
        "symbol": symbol,
        "current_time_utc": analysis_window["analysis_time_utc"],
        "analysis_time_utc": analysis_window["analysis_time_utc"],
        "data_asof": analysis_window["data_asof"],
        "decision_ts": analysis_window["decision_ts"],
        "valid_until_utc": analysis_window["valid_until_utc"],
        "multi_tf_snapshots": _sanitize_snapshots_for_prompt(snapshots),
        "brief": context.get("brief") or {},
        "funding_deltas": context.get("funding_deltas") or {},
        "data_quality": context.get("data_quality") or {},
        "input_budget_meta": context.get("input_budget_meta") or {},
        "constraints": context.get("constraints")
        or {
            "market_type": "futures",
            "max_leverage": 50,
            "requires_margin_mode": True,
        },
    }
    alerts_digest = context.get("alerts_digest") or {}
    account_snapshot = context.get("account_snapshot") or {}
    if _is_meaningful_payload(alerts_digest):
        facts_payload["alerts_digest"] = alerts_digest
    if _is_meaningful_payload(account_snapshot):
        facts_payload["account_snapshot"] = account_snapshot
    return facts_payload


def _resolve_external_views_payload(
    *,
    context: dict[str, Any],
    current_time: datetime,
    include_external_views: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    empty_meta = {
        "external_views_chars_before_filter": 0,
        "external_views_chars_after_filter": 0,
        "dropped_context_blocks": [],
    }
    if not include_external_views:
        return None, empty_meta

    youtube_radar = context.get("youtube_radar") or {}
    intel_digest = context.get("intel_digest") or {}
    full_payload = {
        "youtube_radar": youtube_radar or {"available": False},
        "intel_digest": intel_digest or {"available": False},
    }
    filtered: dict[str, Any] = {}
    dropped: list[str] = []

    if isinstance(youtube_radar, dict) and youtube_radar.get("available") and not youtube_radar.get("stale", True):
        filtered["youtube_radar"] = youtube_radar
    else:
        dropped.append("youtube_radar")

    intel_generated_at = _coerce_datetime((intel_digest or {}).get("generated_at"))
    intel_fresh = False
    if intel_generated_at is not None:
        intel_fresh = (current_time - intel_generated_at).total_seconds() <= 6 * 3600
    if isinstance(intel_digest, dict) and intel_digest and intel_fresh:
        filtered["intel_digest"] = intel_digest
    elif intel_digest:
        dropped.append("intel_digest")

    if not filtered:
        return None, {
            "external_views_chars_before_filter": _json_len(full_payload),
            "external_views_chars_after_filter": 0,
            "dropped_context_blocks": dropped + ["external_views"],
        }
    return filtered, {
        "external_views_chars_before_filter": _json_len(full_payload),
        "external_views_chars_after_filter": _json_len(filtered),
        "dropped_context_blocks": dropped,
    }


def _sanitize_snapshots_for_prompt(snapshots: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for tf, snap in snapshots.items():
        latest = dict((snap or {}).get("latest") or {})
        history = (snap or {}).get("history") or []
        latest_ts = latest.get("ts")
        if isinstance(latest_ts, datetime):
            latest["ts"] = latest_ts.isoformat()
        out[tf] = {
            "latest": latest,
            "history_summary": _history_summary(history),
        }
    return out


def _history_summary(history: list[dict[str, Any]]) -> dict[str, Any]:
    if not history:
        return {"count": 0}
    highs = [h.get("high") for h in history if isinstance(h.get("high"), (int, float))]
    lows = [h.get("low") for h in history if isinstance(h.get("low"), (int, float))]
    closes = [h.get("close") for h in history if isinstance(h.get("close"), (int, float))]
    if not highs or not lows or len(closes) < 2:
        return {"count": len(history)}
    first_close = closes[0]
    last_close = closes[-1]
    change_pct = ((last_close - first_close) / first_close) if first_close else None
    return {
        "count": len(history),
        "range_high": round(max(highs), 6),
        "range_low": round(min(lows), 6),
        "range_change_pct": round(change_pct, 6) if isinstance(change_pct, (int, float)) else None,
        "last_close": round(last_close, 6),
    }


def _is_meaningful_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if not payload:
        return False
    if set(payload.keys()) <= {"count_1h", "count_4h", "top_events", "dominant_types", "alerts_burst"}:
        return any(
            [
                bool(payload.get("count_1h")),
                bool(payload.get("count_4h")),
                bool(payload.get("alerts_burst")),
                bool(payload.get("top_events")),
                bool(payload.get("dominant_types")),
            ]
        )
    return True


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


def _json_block(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _json_len(obj: Any) -> int:
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=_json_default))


def _fmt(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value) * 100:+.4f}%"
    except (TypeError, ValueError):
        return "N/A"
