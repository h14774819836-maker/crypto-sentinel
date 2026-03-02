from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


SYSTEM_PROMPT = """你是 Crypto Sentinel 的专业市场分析模型。

【绝对格式禁止令】
你必须且只能输出一个合法的 JSON 对象。绝对禁止输出任何 JSON 以外的内容！
- 禁止包含任何自然语言解释、补充说明、免责声明。
- 禁止输出任何 Markdown 代码围栏符号（如 ```json 等）。
- 绝对禁止在输出一个完整的 {} 对象后，紧接着再输出多余的 JSON 或任何字符。
- 绝不允许“先文本分析再输出 JSON”的思维链泄露。你的整个回复必须能被原生 json.loads 解析。

【核心事实与业务规则】
1. 事实源优先：输入提供的数据（价格、指标、资金费率、OI、时间周期）是你唯一的客观依据。
2. 观点源降权：输入中的 YouTube 解析仅仅是外部参考观点，绝不能覆盖事实源。
3. 冲突降级法：若观点源与事实源出现冲突，必须在 youtube_reflection 写出 conflicted，并且自动降低 confidence，优先强制输出方向为 HOLD（或者带条件的保守计划）。
4. 数据真实性底线：禁止捏造、编造、强行计算输入 JSON 没有提供的数值、均线或结构位数据。
5. 数据关联要求：evidence / anchors 中使用的指标必须在输入中能逐字找到，数值必须严格字符级等价。
"""


TELEGRAM_AGENT_PROMPT = """你是 Crypto Sentinel 的专业加密交易助手。

行为规则：
1. 不编造行情数据；涉及行情/信号时优先调用工具或引用系统已给出的数据。
2. 解释告警或信号时要引用关键依据（例如 RSI / MACD / 趋势状态 / 告警原因）。
3. 输出尽量清晰、简洁、结构化；当涉及策略建议时优先给出风险提示。
4. 若缺少数据，请明确说明“当前无法获取最新数据”，不要猜测。
"""


def build_analysis_prompt(
    symbol: str,
    snapshots: dict[str, Any],
    context: dict[str, Any] | None = None,
    current_time: datetime | None = None,
) -> str:
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    if context is None:
        return _build_analysis_prompt_legacy(symbol, snapshots, current_time)

    facts_payload = {
        "symbol": symbol,
        "current_time_utc": current_time.isoformat(),
        "multi_tf_snapshots": _sanitize_snapshots_for_prompt(snapshots),
        "brief": context.get("brief") or {},
        "funding_deltas": context.get("funding_deltas") or {},
        "alerts_digest": context.get("alerts_digest") or {},
        "data_quality": context.get("data_quality") or {},
        "input_budget_meta": context.get("input_budget_meta") or {},
    }
    external_views = {
        "youtube_radar": context.get("youtube_radar") or {"available": False},
    }

    schema = {
        "market_regime": "trending_up|trending_down|ranging|volatile|uncertain",
        "signal": {
            "symbol": symbol,
            "direction": "LONG|SHORT|HOLD",
            "entry_price": "number|null",
            "take_profit": "number|null",
            "stop_loss": "number|null",
            "confidence": "0-100",
            "reasoning": "一句简洁中文总结（给首页展示）",
        },
        "evidence": [
            {
                "timeframe": "4h|1h|15m|5m|1m",
                "point": "证据描述，必须引用给定数值",
                "metrics": {"name": "value"},
            }
        ],
        "anchors": [
            {
                "path": "facts.path.to.scalar.value",
                "value": "必须与事实源原值字符级一致",
            }
        ],
        "levels": {
            "supports": ["number"],
            "resistances": ["number"],
        },
        "risk": {
            "rr": "number|null",
            "sl_atr_multiple": "number|null",
            "invalidations": ["条件1", "条件2"]
        },
        "scenarios": {
            "base": "一句话",
            "bull": "一句话",
            "bear": "一句话"
        },
        "youtube_reflection": {
            "status": "aligned|conflicted|ignored|unavailable",
            "note": "一句话说明 YouTube 观点如何被使用"
        },
        "validation_notes": ["可选，自查备注"]
    }

    prompt = [
        f"当前时间 (UTC): {current_time.isoformat()}",
        "",
        "# 输出要求（严格 JSON Schema）",
        "你必须且只能输出严格符合以下结构的 JSON 对象。每个字段必须满足对应注释类型的期望制约条件：",
        _json_block(schema),
        "",
        "# 关键冲突与业务约束处理准则",
        "1. direction=HOLD 时，entry_price/take_profit/stop_loss 这三个字段必须原样输出 null，禁止给 0 或占位符。",
        "2. 当 direction 是 LONG 或 SHORT 时，请务必检验你的止盈止损方向相对入场价数学关系合法。",
        "3. evidence 至少包含 2 条对系统传入指标数据的提炼引用。",
        "4. anchors 至少包含 2 条；锚定路径 path 必须指向具体参数路径（例如 'facts.brief.tradeable_gate.tradeable'）。",
        "5. 如果 context 中的 data_quality.overall 为 POOR，优先 HOLD 观望并输出低置信度（<40）。",
        "6. 在分析期间，将下面提供的“事实源”视为最高优先级数据来源，“观点源”仅作为旁证，发生冲突立即采取降低 confidence 和保守观望处理（同时标识 'conflicted'）。",
        "",
        "# 事实源与观点源数据 (巨量上下文，仔细阅读数值)",
        "--------------------- 事实源开始 ---------------------",
        _json_block(facts_payload),
        "--------------------- 事实源结束 ---------------------",
        "",
        "----------------- 辅助不可信观点源开始 ----------------",
        _json_block(external_views),
        "----------------- 辅助不可信观点源结束 ----------------",
    ]
    return "\n".join(prompt)


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
                    f"- 最近{len(history)}根摘要: 高 {_fmt(max(highs))} / 低 {_fmt(min(lows))} / 区间变化 {change_pct:+.2f}%"
                )
        lines.append("")

    lines.extend([
        "请基于以上数据进行多周期技术分析，并严格输出 JSON 对象：",
        "{\"market_regime\":\"...\",\"signal\":{\"symbol\":\"BTCUSDT\",\"direction\":\"LONG|SHORT|HOLD\",\"entry_price\":null,\"take_profit\":null,\"stop_loss\":null,\"confidence\":0,\"reasoning\":\"...\"}}",
    ])
    return "\n".join(lines)


def _sanitize_snapshots_for_prompt(snapshots: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for tf, snap in snapshots.items():
        latest = dict((snap or {}).get("latest") or {})
        # Keep prompt compact and serializable
        hist = (snap or {}).get("history") or []
        latest_ts = latest.get("ts")
        if isinstance(latest_ts, datetime):
            latest["ts"] = latest_ts.isoformat()
        out[tf] = {
            "latest": latest,
            "history_summary": _history_summary(hist),
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


def _json_block(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


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
