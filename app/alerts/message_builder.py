from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.config import get_settings

@dataclass(slots=True)
class TelegramMessage:
    text: str
    parse_mode: str = "HTML"
    reply_markup: dict | None = None
    disable_web_page_preview: bool = True
    kind: str = "system"
    source_id: int | str | None = None


def escape_html(s: str | Any) -> str:
    if s is None:
        return ""
    return html.escape(str(s))


def fmt_price(x: float | None) -> str:
    if x is None:
        return "N/A"
    if x < 1:
        return f"{x:.4f}"
    return f"{x:.2f}"


def fmt_pct(x: float | None) -> str:
    if x is None:
        return "N/A"
    return f"{x * 100:.2f}%"


def fmt_pct_signed(x: float | None) -> str:
    if x is None:
        return "N/A"
    return f"{x * 100:+.2f}%"


def fmt_dt_bjt(dt: datetime | None) -> str:
    if dt is None:
        return "N/A"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    bjt = dt.astimezone(timezone(timedelta(hours=8)))
    return bjt.strftime("%Y-%m-%d %H:%M 北京时间")


def truncate_safe_html(text: str, limit: int = 3800) -> str:
    if len(text) <= limit:
        return text

    suffix = "\n\n...(截断，详情见 Dashboard)"
    effective_limit = max(0, limit - len(suffix))
    tag_pattern = re.compile(r"</?([a-zA-Z]+)[^>]*>")

    current_len = 0
    pos = 0
    truncation_index = 0
    open_tags: list[str] = []

    while pos < len(text):
        if current_len >= effective_limit:
            truncation_index = pos
            break

        match = tag_pattern.search(text, pos)
        if match and match.start() == pos:
            tag_text = match.group(0)
            tag_name = match.group(1).lower()
            if tag_text.startswith("</"):
                if open_tags and open_tags[-1] == tag_name:
                    open_tags.pop()
            elif tag_name in {"b", "i", "code", "pre", "s", "u", "a"}:
                open_tags.append(tag_name)
            current_len += len(tag_text)
            pos = match.end()
            continue

        next_tag = tag_pattern.search(text, pos)
        next_pos = next_tag.start() if next_tag else len(text)
        chunk = text[pos:next_pos]
        if current_len + len(chunk) > effective_limit:
            truncation_index = pos + (effective_limit - current_len)
            break
        current_len += len(chunk)
        pos = next_pos

    if truncation_index == 0:
        truncation_index = effective_limit

    result = text[:truncation_index] + suffix
    for tag in reversed(open_tags):
        result += f"</{tag}>"
    return result


def _fmt_num(x: float | int | None, digits: int = 2) -> str:
    if x is None:
        return "N/A"
    if isinstance(x, int):
        return str(x)
    return f"{x:.{digits}f}"


def _alert_title_zh(alert_type: str | None, direction: str | None = None) -> str:
    mapping = {
        "PRICE_SPIKE_UP": "快速上冲",
        "PRICE_SPIKE_DOWN": "快速下挫",
        "BREAKOUT_UP": "向上突破",
        "BREAKOUT_DOWN": "向下跌破",
        "VOLATILITY_SURGE": "波动抬升",
        "VOLUME_ANOMALY": "量能异常",
        "MOMENTUM_ANOMALY_UP": "快速上冲",
        "MOMENTUM_ANOMALY_DOWN": "快速下挫",
        "MOMENTUM_ANOMALY_UP_ESCALATE": "快速上冲（加剧）",
        "MOMENTUM_ANOMALY_DOWN_ESCALATE": "快速下挫（加剧）",
    }
    if alert_type in mapping:
        return mapping[alert_type]
    if direction == "UP":
        return "快速上冲"
    if direction == "DOWN":
        return "快速下挫"
    return "异常波动"


def _regime_zh(regime: str | None) -> str:
    return {
        "TRENDING": "趋势",
        "RANGING": "震荡",
        "VOLATILE": "高波动",
        "NEUTRAL": "中性",
    }.get((regime or "").upper(), regime or "未知")


def _confirm_zh(status: str | None) -> str:
    return {
        "confirmed_5m": "5m 已确认",
        "confirmed_15m": "15m 已确认",
        "pending_mtf": "5m/15m 待确认",
        "insufficient_data": "多周期数据不足",
        "not_required": "无需多周期确认",
    }.get(status or "", status or "未知")


def _score_level_zh(score: int | None) -> str:
    if score is None:
        return "未知"
    if score >= 90:
        return "极端"
    if score >= 80:
        return "严重"
    if score >= 70:
        return "中等"
    return "轻微"


def _build_legacy_anomaly_message(alert_payload: dict) -> TelegramMessage:
    symbol = escape_html(alert_payload.get("symbol", "UNKNOWN"))
    alert_type = escape_html(alert_payload.get("alert_type", "Alert"))
    severity = escape_html(alert_payload.get("severity", "INFO"))
    reason_raw = alert_payload.get("reason", "")
    ts = alert_payload.get("ts")
    metrics = alert_payload.get("metrics_json") or {}
    metric_text = "\n".join(f"- {escape_html(k)}: {escape_html(v)}" for k, v in metrics.items())

    lines = [
        f"🚨 <b>{symbol} 异常：{alert_type}</b>｜严重度 <b>{severity}</b>",
        f"<b>发生：</b>{escape_html(reason_raw)}",
        "",
        "<b>关键数据：</b>",
        metric_text if metric_text else "- 暂无数据",
        "",
        f"⏰ 时间：{fmt_dt_bjt(ts)}｜ID: {escape_html(str(alert_payload.get('event_uid', '')))[:8]}…",
    ]
    return TelegramMessage(
        text=truncate_safe_html("\n".join(lines)),
        kind="anomaly",
        source_id=alert_payload.get("event_uid"),
    )


def build_anomaly_message(alert_payload: dict) -> TelegramMessage:
    metrics = alert_payload.get("metrics_json") or {}
    score_raw = metrics.get("score")
    if not isinstance(score_raw, (int, float)):
        return _build_legacy_anomaly_message(alert_payload)

    score = int(score_raw)
    symbol = str(alert_payload.get("symbol") or "UNKNOWN")
    alert_type = str(alert_payload.get("alert_type") or "Alert")
    reason = str(alert_payload.get("reason") or "")
    ts = alert_payload.get("ts")

    direction = str(metrics.get("direction") or "").upper()
    regime = str(metrics.get("regime") or "")
    confirm = metrics.get("confirm") or {}
    thresholds = metrics.get("thresholds") or {}
    observations = metrics.get("observations") or {}
    delivery = metrics.get("delivery") or {}
    debug = metrics.get("debug") or {}

    ret_1m = observations.get("ret_1m") if isinstance(observations.get("ret_1m"), (int, float)) else None
    thr = thresholds.get("price_threshold_ret") if isinstance(thresholds.get("price_threshold_ret"), (int, float)) else None
    multiple = observations.get("threshold_multiple") if isinstance(observations.get("threshold_multiple"), (int, float)) else None
    volume_z = observations.get("volume_zscore") if isinstance(observations.get("volume_zscore"), (int, float)) else None
    mode = str(thresholds.get("price_threshold_mode") or "")
    confirm_status = str(confirm.get("status") or "")

    title_zh = _alert_title_zh(alert_type, direction)
    level_zh = _score_level_zh(score)

    title_line = f"🚨 <b>{escape_html(symbol)} {escape_html(title_zh)}（1分钟）｜{escape_html(level_zh)}（Score {score}/100）</b>"
    summary_line = escape_html(reason or f"1分钟出现异常波动，当前状态：{_confirm_zh(confirm_status)}。")

    settings_obj = None
    try:
        settings_obj = get_settings()
    except Exception:
        settings_obj = None
    style = (getattr(settings_obj, "telegram_alert_template_style", "readable") or "readable").strip().lower()
    volume_z_alert_threshold = float(getattr(settings_obj, "anomaly_volume_zscore_threshold", 3.0))

    if confirm_status in {"confirmed_5m", "confirmed_15m"}:
        action_lines = [
            "<b>你可以怎么做？</b>",
            "- 已持仓：按计划管理止盈止损，避免情绪化追单。",
            "- 观望：优先等回踩关键位不破再考虑跟随，控制仓位。",
        ]
    else:
        action_lines = [
            "<b>你可以怎么做？</b>",
            "- 已持仓：优先收紧止损，防止冲高回落/急跌反抽。",
            "- 观望：建议等 5m 收盘确认后再判断方向，避免震荡噪声。",
        ]

    basis_lines = [
        "<b>为什么会发这条？</b>",
        f"- 幅度：{escape_html(fmt_pct_signed(ret_1m))}（阈值：{escape_html(fmt_pct_signed(thr))}）"
        + (f" ≥ {escape_html(_fmt_num(float(multiple), 2))}倍" if multiple is not None else ""),
        f"- 量能：{'异常' if volume_z is not None and volume_z >= volume_z_alert_threshold else '正常'}（Z={escape_html(_fmt_num(volume_z, 2))}）",
        f"- 市场状态：{escape_html(_regime_zh(regime))}",
        f"- 确认状态：{escape_html(_confirm_zh(confirm_status))}",
        f"- 阈值口径：{'动态（ATR）' if mode.lower() == 'atr_dynamic' else '固定'}",
    ]

    debug_lines = []
    include_debug = bool(getattr(settings_obj, "telegram_alert_include_debug", True))
    if include_debug:
        debug_lines = [
            "<b>调试信息</b>",
            f"- ret_1m={escape_html(str(ret_1m))}",
            f"- thr={escape_html(str(thr))}",
            f"- mode={escape_html(mode)}",
            f"- regime={escape_html(regime)}",
            f"- confirm={escape_html(confirm_status)}",
            f"- kind={escape_html(str(debug.get('event_kind') or 'ENTER'))}",
            f"- cooldown={escape_html(str(delivery.get('cooldown_seconds_applied')))}s",
            f"- id={escape_html(str(alert_payload.get('event_uid', '')) )[:8]}…",
        ]

    lines = [
        title_line,
        summary_line,
        "",
        *action_lines,
        "",
        *basis_lines,
        "",
        f"⏰ 时间：{fmt_dt_bjt(ts)}",
    ]
    if debug_lines:
        lines.extend(["", *debug_lines])

    if style == "compact":
        compact_lines = [
            f"🚨 <b>{escape_html(symbol)} {escape_html(title_zh)} {escape_html(fmt_pct_signed(ret_1m))}</b>｜{escape_html(level_zh)} {score}/100",
            f"{escape_html(_confirm_zh(confirm_status))}｜{escape_html(_regime_zh(regime))}｜阈值 {escape_html(fmt_pct_signed(thr))}"
            + (f"（{escape_html(_fmt_num(float(multiple), 2))}倍）" if multiple is not None else ""),
        ]
        if include_debug:
            compact_lines.append(f"ID: {escape_html(str(alert_payload.get('event_uid', '')) )[:8]}…")
        lines = compact_lines
    elif style == "pro":
        pro_lines = [
            f"🚨 <b>{escape_html(symbol)} 动量 {escape_html(direction or 'N/A')}｜评分 {score}/100</b>",
            f"涨跌幅={escape_html(fmt_pct_signed(ret_1m))} / 阈值={escape_html(fmt_pct_signed(thr))}"
            + (f" / {escape_html(_fmt_num(float(multiple), 2))}倍" if multiple is not None else ""),
            f"市场={escape_html(_regime_zh(regime))} | 确认={escape_html(_confirm_zh(confirm_status))} | 阈值模式={escape_html(mode or 'N/A')}",
            f"建议: {'等待确认后再跟随' if confirm_status not in {'confirmed_5m','confirmed_15m'} else '按计划管理仓位'}",
        ]
        if include_debug:
            pro_lines.append(f"调试: kind={escape_html(str(debug.get('event_kind') or 'ENTER'))}, id={escape_html(str(alert_payload.get('event_uid', '')) )[:8]}…")
        lines = pro_lines

    return TelegramMessage(
        text=truncate_safe_html("\n".join(lines)),
        kind="anomaly",
        source_id=alert_payload.get("event_uid"),
    )


def build_ai_signal_message(sig, source_id: int | None = None) -> TelegramMessage:
    direction_map = {"LONG": "做多", "SHORT": "做空", "HOLD": "观望"}
    direction_text = direction_map.get(sig.direction, escape_html(sig.direction))
    icon = "📈" if sig.direction == "LONG" else ("📉" if sig.direction == "SHORT" else "⏸️")

    lines = [f"{icon} <b>{escape_html(sig.symbol)}</b>｜<b>{direction_text}</b>｜置信度 <b>{getattr(sig, 'confidence', 0)}%</b>"]

    entry = getattr(sig, "entry_price", None)
    tp = getattr(sig, "take_profit", None)
    sl = getattr(sig, "stop_loss", None)
    if entry is not None and tp is not None and sl is not None:
        entry_low, entry_high = entry * 0.9985, entry * 1.0015
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr_str = f"1:{reward / risk:.1f}" if risk > 0 else "N/A"
        lines.extend(
            [
                "",
                "<b>计划（区间）</b>",
                f"- 入场：{fmt_price(entry_low)} ~ {fmt_price(entry_high)}",
                f"- 止损：{fmt_price(sl)}",
                f"- 止盈：{fmt_price(tp)}",
                f"- 风险回报：约 {rr_str}",
            ]
        )
    else:
        lines.extend(["", "<b>计划</b>", "- 暂无具体点位（观望或边缘信号）"])

    reasoning = getattr(sig, "reasoning", "")
    lines.extend(["", "<b>理由（人话）</b>", escape_html(reasoning)])

    ts = getattr(sig, "ts", datetime.now(timezone.utc))
    sid = source_id or getattr(sig, "id", None)
    sid_display = (str(sid)[:8] + "…") if sid else "N/A"
    lines.extend(["", f"⏰ 数据：{fmt_dt_bjt(ts)}｜模型：{escape_html(getattr(sig, 'model_name', ''))}｜ID: {sid_display}"])

    reply_markup = None
    if sid:
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "📊 打开 Dashboard", "url": "http://127.0.0.1:8000/"},
                    {"text": "🧾 解释这条信号", "callback_data": f"explain:{sid}"},
                ]
            ]
        }

    return TelegramMessage(
        text=truncate_safe_html("\n".join(lines)),
        kind="ai_signal",
        source_id=sid,
        reply_markup=reply_markup,
    )
