from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.alerts.message_builder import TelegramMessage, build_ai_diagnostic_alert
from app.db.repository import (
    get_alert_event_by_uid,
    get_latest_intel_digest,
    get_latest_youtube_consensus,
    get_recent_youtube_insights,
    list_alerts,
    update_alert_event_delivery,
)
from app.logging import logger
from app.utils.time import ensure_utc


def _clip_text(text: str, limit: int = 1200) -> str:
    s = (text or "").strip()
    if len(s) <= limit:
        return s
    return s[:limit] + "...(truncated)"


def _compact_json(value: Any, limit: int = 1800) -> str:
    try:
        dumped = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        dumped = str(value)
    return _clip_text(dumped, limit=limit)


def _pick_intel_items(digest_json: Any, *, min_severity: float = 50.0, limit: int = 6) -> list[dict[str, Any]]:
    if not isinstance(digest_json, dict):
        return []
    candidate = None
    for key in ("items", "events", "news", "headlines"):
        val = digest_json.get(key)
        if isinstance(val, list):
            candidate = val
            break
    if not isinstance(candidate, list):
        return []

    out: list[dict[str, Any]] = []
    for row in candidate:
        if not isinstance(row, dict):
            continue
        sev = row.get("severity")
        score = row.get("score")
        importance = row.get("importance")
        sev_num = None
        for x in (sev, score, importance):
            if isinstance(x, (int, float)):
                sev_num = float(x)
                break
        if sev_num is not None and sev_num < min_severity:
            continue
        out.append(
            {
                "title": row.get("title") or row.get("headline") or row.get("summary"),
                "severity": sev_num,
                "symbol": row.get("symbol"),
                "ts": row.get("ts") or row.get("published_at") or row.get("time"),
            }
        )
        if len(out) >= limit:
            break
    return out


def _pick_youtube_insights(rows: list[Any], *, limit: int = 3) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    for row in rows[:limit]:
        payload = getattr(row, "analyst_view_json", None)
        if not isinstance(payload, dict):
            continue
        picked.append(
            {
                "stance": payload.get("stance") or payload.get("bias"),
                "confidence": payload.get("confidence"),
                "summary": payload.get("summary") or payload.get("core_view"),
                "risk": payload.get("risk"),
            }
        )
    return picked


def _build_anomaly_diagnostic_prompt(
    *,
    symbol: str,
    alert_payload: dict[str, Any],
    intel_digest_json: Any,
    youtube_consensus_json: Any,
    youtube_insights: list[Any],
) -> str:
    metrics = alert_payload.get("metrics_json") or {}
    observations = metrics.get("observations") or {}
    thresholds = metrics.get("thresholds") or {}
    confirm = metrics.get("confirm") or {}
    ts = alert_payload.get("ts")
    ts_text = "unknown"
    if ts is not None:
        try:
            ts_text = ensure_utc(ts).astimezone(timezone.utc).isoformat()
        except Exception:
            ts_text = str(ts)

    intel_key_items = _pick_intel_items(intel_digest_json, min_severity=50.0, limit=6)
    yt_consensus_compact = {}
    if isinstance(youtube_consensus_json, dict):
        yt_consensus_compact = {
            "stance": youtube_consensus_json.get("stance") or youtube_consensus_json.get("bias"),
            "confidence": youtube_consensus_json.get("confidence"),
            "summary": youtube_consensus_json.get("summary") or youtube_consensus_json.get("consensus"),
        }
    yt_insight_compact = _pick_youtube_insights(youtube_insights, limit=3)

    return (
        "你是高频风险控制与交易执行顾问。请只输出简洁中文。\n"
        "请判断这次 1m 异动更接近：\n"
        "A) 顺势有效突破\n"
        "B) 逆势诱多/诱空\n\n"
        "并输出：\n"
        "1) 结论（A/B + 一句话）\n"
        "2) 关键依据（最多4条）\n"
        "3) 接下来 15 分钟执行建议（持仓/观望各一条）\n"
        "4) 风险提示（最多2条）\n\n"
        f"Symbol: {symbol}\n"
        f"Event Time UTC: {ts_text}\n"
        f"Reason: {_clip_text(str(alert_payload.get('reason') or ''), 400)}\n"
        f"Score: {metrics.get('score')} | Direction: {metrics.get('direction')} | Regime: {metrics.get('regime')}\n"
        f"Observations: {_compact_json(observations, 800)}\n"
        f"Thresholds: {_compact_json(thresholds, 600)}\n"
        f"Confirm: {_compact_json(confirm, 300)}\n"
        f"IntelKeyItems(Severity>=50): {_compact_json(intel_key_items, 1200)}\n"
        f"YoutubeConsensus: {_compact_json(yt_consensus_compact, 700)}\n"
        f"YoutubeInsightsTop3: {_compact_json(yt_insight_compact, 900)}\n"
    )


def _normalize_alert_for_batch(entry: dict[str, Any]) -> dict[str, Any]:
    payload = entry.get("alert_payload") or {}
    metrics = payload.get("metrics_json") or {}
    confirm = metrics.get("confirm") or {}
    delivery = metrics.get("delivery") or {}
    return {
        "event_uid": str(entry.get("event_uid") or payload.get("event_uid") or ""),
        "alert_ref": str(entry.get("alert_ref") or ""),
        "ts": payload.get("ts"),
        "reason": str(payload.get("reason") or ""),
        "score": metrics.get("score"),
        "direction": metrics.get("direction"),
        "regime": metrics.get("regime"),
        "confirm": confirm.get("status"),
        "source": delivery.get("source") or "anomaly",
    }


def _merge_batch_alerts(batch: list[dict[str, Any]], max_alerts: int) -> list[dict[str, Any]]:
    normalized = [_normalize_alert_for_batch(it) for it in batch]
    dedup: dict[str, dict[str, Any]] = {}
    for row in normalized:
        uid = row.get("event_uid") or ""
        key = uid if uid else f"{row.get('alert_ref')}:{row.get('reason')}"
        dedup[key] = row
    merged = list(dedup.values())
    merged.sort(key=lambda x: str(x.get("ts") or ""))
    if len(merged) > max_alerts:
        merged = merged[-max_alerts:]
    return merged


def _build_batched_prompt(
    *,
    symbol: str,
    alerts: list[dict[str, Any]],
    intel_digest_json: Any,
    youtube_consensus_json: Any,
    youtube_insights: list[Any],
) -> str:
    base_payload = {"symbol": symbol, "reason": alerts[-1].get("reason") if alerts else "", "metrics_json": {}}
    base = _build_anomaly_diagnostic_prompt(
        symbol=symbol,
        alert_payload=base_payload,
        intel_digest_json=intel_digest_json,
        youtube_consensus_json=youtube_consensus_json,
        youtube_insights=youtube_insights,
    )
    lines = ["最近短时告警序列："]
    for idx, a in enumerate(alerts, start=1):
        lines.append(
            f"{idx}. ref={a.get('alert_ref')} ts={a.get('ts')} score={a.get('score')} "
            f"dir={a.get('direction')} regime={a.get('regime')} confirm={a.get('confirm')} src={a.get('source')} "
            f"reason={_clip_text(str(a.get('reason') or ''), 180)}"
        )
    return (
        "你需要对以下短时连续告警做聚合判断，避免逐条孤立分析。\n"
        f"symbol={symbol}\n"
        f"batch_size={len(alerts)}\n\n"
        + "\n".join(lines)
        + "\n\n"
        + base
    )


def _schedule_batch_task(runtime, symbol: str) -> None:
    existing = runtime.anomaly_diag_tasks.get(symbol)
    if existing is not None and not existing.done():
        return
    delay = max(5, int(getattr(runtime.settings, "anomaly_ai_diagnostic_batch_window_seconds", 45) or 45))
    task = asyncio.create_task(_run_batched_diagnostic(runtime, symbol=symbol, delay_seconds=delay), name=f"anomaly_diag_batch_{symbol}")
    runtime.anomaly_diag_tasks[symbol] = task


async def enqueue_async_anomaly_diagnostic(
    runtime,
    *,
    alert_payload: dict[str, Any],
    event_uid: str,
    reply_to_message_id: int,
    alert_ref: str,
) -> None:
    symbol = str(alert_payload.get("symbol") or "").upper()
    if not symbol:
        return
    queue = runtime.anomaly_diag_pending.setdefault(symbol, [])
    queue.append(
        {
            "alert_payload": alert_payload,
            "event_uid": event_uid,
            "reply_to_message_id": int(reply_to_message_id),
            "alert_ref": alert_ref,
            "queued_at": datetime.now(timezone.utc),
        }
    )
    max_queue = max(4, int(getattr(runtime.settings, "anomaly_ai_diagnostic_max_alerts", 8) or 8) * 2)
    if len(queue) > max_queue:
        runtime.anomaly_diag_pending[symbol] = queue[-max_queue:]
    _schedule_batch_task(runtime, symbol)


async def _run_batched_diagnostic(runtime, *, symbol: str, delay_seconds: int) -> None:
    await asyncio.sleep(max(1, int(delay_seconds)))
    batch = runtime.anomaly_diag_pending.pop(symbol, [])
    if not batch:
        runtime.anomaly_diag_tasks.pop(symbol, None)
        return
    anchor = batch[-1]
    anchor_event_uid = str(anchor.get("event_uid") or "")
    anchor_reply_to = int(anchor.get("reply_to_message_id") or 0)
    anchor_ref = str(anchor.get("alert_ref") or "")
    if anchor_reply_to <= 0:
        runtime.anomaly_diag_tasks.pop(symbol, None)
        return

    if runtime.market_analyst is None:
        logger.info("Skip anomaly AI diagnostic: market_analyst unavailable symbol=%s", symbol)
        runtime.anomaly_diag_tasks.pop(symbol, None)
        return

    settings = runtime.settings
    timeout_sec = max(10, int(getattr(settings, "anomaly_ai_diagnostic_timeout_seconds", 120) or 120))
    lookback_min = max(1, int(getattr(settings, "anomaly_ai_diagnostic_lookback_minutes", 5) or 5))
    max_alerts = max(3, int(getattr(settings, "anomaly_ai_diagnostic_max_alerts", 8) or 8))
    intel_lookback = int(getattr(settings, "anomaly_ai_intel_lookback_hours", 4) or 4)
    yt_lookback = int(getattr(settings, "anomaly_ai_youtube_lookback_hours", 4) or 4)

    status_message_id: int | None = None
    try:
        with runtime.session_factory() as session:
            if anchor_event_uid and not anchor_event_uid.startswith("tick:"):
                event = get_alert_event_by_uid(session, anchor_event_uid)
                if event is None:
                    logger.info("Skip anomaly AI diagnostic: event missing uid=%s", anchor_event_uid)
                    runtime.anomaly_diag_tasks.pop(symbol, None)
                    return
                delivery = dict((event.metrics_json or {}).get("delivery") or {})
                flash_message_id = delivery.get("flash_message_id")
                if isinstance(flash_message_id, int) and flash_message_id != int(anchor_reply_to):
                    logger.warning(
                        "Skip anomaly AI diagnostic due to mismatched reply target uid=%s expected=%s got=%s",
                        anchor_event_uid,
                        flash_message_id,
                        anchor_reply_to,
                    )
                    runtime.anomaly_diag_tasks.pop(symbol, None)
                    return
            intel_digest = get_latest_intel_digest(session, symbol="GLOBAL", lookback_hours=intel_lookback)
            if intel_digest is None:
                intel_digest = get_latest_intel_digest(session, symbol=symbol, lookback_hours=intel_lookback)
            youtube_consensus = get_latest_youtube_consensus(session, symbol=symbol)
            youtube_insights = get_recent_youtube_insights(session, lookback_hours=yt_lookback, symbol=symbol)
            recent_alert_rows = list_alerts(session, limit=max_alerts * 2, symbol=symbol)

            intel_digest_json = getattr(intel_digest, "digest_json", None)
            youtube_consensus_json = getattr(youtube_consensus, "consensus_json", None)

        recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_min)
        recent_payloads: list[dict[str, Any]] = []
        for row in recent_alert_rows:
            ts = getattr(row, "ts", None)
            if ts is None or ensure_utc(ts) < recent_cutoff:
                continue
            recent_payloads.append(
                {
                    "alert_payload": {
                        "event_uid": row.event_uid,
                        "symbol": row.symbol,
                        "ts": ts,
                        "reason": row.reason,
                        "metrics_json": {
                            **(row.metrics_json or {}),
                            "delivery": {
                                **(((row.metrics_json or {}).get("delivery") or {})),
                                "source": "anomaly_event",
                            },
                        },
                    },
                    "event_uid": row.event_uid,
                    "alert_ref": str(((row.metrics_json or {}).get("delivery") or {}).get("alert_ref") or ""),
                }
            )

        merged_alerts = _merge_batch_alerts(batch + recent_payloads, max_alerts=max_alerts)
        prompt = _build_batched_prompt(
            symbol=symbol,
            alerts=merged_alerts,
            intel_digest_json=intel_digest_json,
            youtube_consensus_json=youtube_consensus_json,
            youtube_insights=youtube_insights,
        )

        messages = [
            {"role": "system", "content": "你是严谨的加密市场风控分析师。禁止编造未提供事实。"},
            {"role": "user", "content": prompt},
        ]
        status_msg = TelegramMessage(
            text="⏳ 正在等待 AI 诊断...",
            reply_to_message_id=int(anchor_reply_to),
        )
        status_res = await runtime.telegram.send_message_with_result(status_msg)
        status_message_id = status_res.message_id if status_res.ok else None

        provider = runtime.market_analyst.provider
        response = await asyncio.wait_for(
            provider.generate_response(
                messages=messages,
                max_tokens=1200,
                temperature=0.1,
            ),
            timeout=timeout_sec,
        )
        raw_content = str(response.get("content") or "").strip()
        diagnosis_text = raw_content or "模型未返回有效内容。"
        if not raw_content:
            logger.warning(
                "Anomaly AI diagnostic empty content symbol=%s keys=%s reasoning_len=%d",
                symbol,
                list(response.keys()) if isinstance(response, dict) else type(response).__name__,
                len(str(response.get("reasoning_content") or "")),
            )
        msg = build_ai_diagnostic_alert(
            symbol=symbol,
            alert_ref=anchor_ref,
            diagnosis_text=diagnosis_text,
            summary_reason=f"最近{lookback_min}分钟 {len(merged_alerts)} 条告警已聚合分析",
        )
        msg.reply_to_message_id = int(anchor_reply_to)
        send_res = await runtime.telegram.send_message_with_result(msg)
        if send_res.ok:
            with runtime.session_factory() as session:
                for item in batch:
                    uid = str(item.get("event_uid") or "")
                    if not uid:
                        continue
                    update_alert_event_delivery(
                        session,
                        event_uid=uid,
                        updates={
                            "diagnostic_message_id": send_res.message_id,
                            "diagnostic_sent_at": datetime.now(timezone.utc).isoformat(),
                            "diagnostic_batch_size": len(merged_alerts),
                            "diagnostic_anchor_ref": anchor_ref,
                        },
                    )
    except TimeoutError:
        fallback = build_ai_diagnostic_alert(
            symbol=symbol,
            alert_ref=anchor_ref,
            diagnosis_text="AI 诊断超时。建议先按原风控计划执行，等待下一轮确认。",
            summary_reason=f"最近{lookback_min}分钟告警聚合诊断超时",
        )
        fallback.reply_to_message_id = int(anchor_reply_to)
        await runtime.telegram.send_message(fallback)
    except Exception as exc:
        logger.warning("Anomaly AI diagnostic failed symbol=%s err=%s", symbol, exc)
        fallback = build_ai_diagnostic_alert(
            symbol=symbol,
            alert_ref=anchor_ref,
            diagnosis_text="AI 诊断暂不可用。请以第一条快讯与风控规则为准。",
            summary_reason=f"最近{lookback_min}分钟告警聚合诊断失败",
        )
        fallback.reply_to_message_id = int(anchor_reply_to)
        await runtime.telegram.send_message(fallback)
    finally:
        if status_message_id is not None:
            await runtime.telegram.delete_message(status_message_id)
        runtime.anomaly_diag_tasks.pop(symbol, None)
        if runtime.anomaly_diag_pending.get(symbol):
            _schedule_batch_task(runtime, symbol)
