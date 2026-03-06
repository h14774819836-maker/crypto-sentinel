import logging
import asyncio
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from app.config import get_settings
from app.db.session import SessionLocal
from app.alerts.telegram import TelegramClient
from app.alerts.message_builder import TelegramMessage, build_ai_signal_message, build_anomaly_message
from app.db.models import AiSignal, AlertEvent
from app.utils.time import ensure_utc

logger = logging.getLogger(__name__)

# Basic in-memory deduplication cache for Update IDs to prevent replay processing
_processed_updates: set[int] = set()
_MAX_CACHE_SIZE = 5000
_TELEGRAM_TEXT_LIMIT = 4096


def _tg_trace(msg: str, *args) -> None:
    # 用 WARNING 保证在终端里足够醒目，便于排查 Telegram 交互链路
    logger.warning("[TG交互追踪][dispatcher] " + msg, *args)


def _clean_cache():
    if len(_processed_updates) > _MAX_CACHE_SIZE:
        _processed_updates.clear() # Simplistic ring-flush for MVP


def _extract_message_id(resp: dict | None) -> int | None:
    """Extract message_id from Telegram sendMessage response."""
    if not resp or not resp.get("ok"):
        return None
    r = resp.get("result")
    if isinstance(r, dict):
        mid = r.get("message_id")
        return int(mid) if isinstance(mid, int) else None
    return None


def _truncate_telegram_text(text: str) -> str:
    if len(text) <= _TELEGRAM_TEXT_LIMIT:
        return text
    logger.warning(
        "[telegram_dispatcher] AI reply length %d exceeds Telegram limit %d, truncating",
        len(text),
        _TELEGRAM_TEXT_LIMIT,
    )
    return text[: _TELEGRAM_TEXT_LIMIT - 3] + "..."


async def _send_ai_reply(client: TelegramClient, chat_id: int, text: str) -> None:
    """
    Send AI reply to Telegram.
    Prefer Markdown formatting, but fall back to plain text if Telegram rejects entity parsing.
    """
    safe_text = _truncate_telegram_text(text or "")
    if not safe_text:
        safe_text = "(empty response)"

    payload = {
        "chat_id": chat_id,
        "text": safe_text,
        "parse_mode": "Markdown",
    }
    _tg_trace(
        "准备发送AI回复（Markdown）chat_id=%s 文本长度=%d 预览=%r",
        chat_id,
        len(safe_text),
        safe_text[:120] + ("..." if len(safe_text) > 120 else ""),
    )
    resp = await client._post("sendMessage", payload)
    if resp and resp.get("ok", True):
        _tg_trace("AI回复发送成功（Markdown）chat_id=%s", chat_id)
        return

    logger.warning(
        "[telegram_dispatcher] Markdown reply failed for chat_id=%s, retrying as plain text",
        chat_id,
    )
    _tg_trace("Markdown发送失败，回退纯文本重试 chat_id=%s", chat_id)
    fallback_resp = await client._post("sendMessage", {"chat_id": chat_id, "text": safe_text})
    if fallback_resp and fallback_resp.get("ok", True):
        _tg_trace("AI回复发送成功（纯文本回退）chat_id=%s", chat_id)
    else:
        _tg_trace("AI回复发送失败（纯文本回退后仍失败）chat_id=%s", chat_id)

async def process_telegram_update(update_data: dict[str, Any]):
    """Entry point for all background webhook events"""
    update_id = update_data.get("update_id")
    if not update_id:
        _tg_trace("跳过处理：payload没有update_id")
        return
    _tg_trace("开始处理 update_id=%s 顶层keys=%s", update_id, list(update_data.keys())[:10])
        
    _clean_cache()
    if update_id in _processed_updates:
        _tg_trace("重复update被忽略 update_id=%s", update_id)
        logger.debug("[telegram_dispatcher] Ignored duplicate update_id: %s", update_id)
        return
    _processed_updates.add(update_id)

    settings = get_settings()
    if not settings.telegram_enabled or not settings.telegram_bot_token:
        _tg_trace(
            "跳过处理：Telegram未启用或Token缺失 enabled=%s token_present=%s",
            settings.telegram_enabled,
            bool(settings.telegram_bot_token),
        )
        return

    # Initialize client to send replies
    client = TelegramClient(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id # Default chat used for broadcasting, but we reply to specific senders
    )

    # Route Update Types
    if "message" in update_data:
        _tg_trace("路由到 message 处理器 update_id=%s", update_id)
        await _handle_message(client, update_data["message"], settings)
    elif "edited_message" in update_data:
        _tg_trace("路由到 edited_message 处理器 update_id=%s", update_id)
        await _handle_message(client, update_data["edited_message"], settings)
    elif "callback_query" in update_data:
        _tg_trace("路由到 callback_query 处理器 update_id=%s", update_id)
        await _handle_callback_query(client, update_data["callback_query"], settings)
    else:
        _tg_trace("未处理的 update 类型 update_id=%s", update_id)

async def _handle_message(client: TelegramClient, message: dict[str, Any], settings):
    chat_id = message.get("chat", {}).get("id")
    if not chat_id:
        _tg_trace("message处理跳过：缺少chat_id")
        return

    # RBAC Validation
    allowed_chats = settings.telegram_allowed_chats
    if allowed_chats and chat_id not in allowed_chats:
        _tg_trace("消息被白名单拦截 chat_id=%s allowed=%s", chat_id, allowed_chats)
        logger.warning("[telegram_dispatcher] Unauthorized access attempt from chat_id: %s", chat_id)
        # Optional: Send rejection message or just drop
        await client._post("sendMessage", {
            "chat_id": chat_id,
            "text": "🚫 无权限访问该系统。Unauthorized access.",
            "parse_mode": "HTML"
        })
        return

    text: str = message.get("text", "")
    if not text:
        _tg_trace("message处理跳过：非文本消息 chat_id=%s keys=%s", chat_id, list(message.keys())[:15])
        return
    _tg_trace(
        "收到消息 chat_id=%s 长度=%d 是否命令=%s 预览=%r",
        chat_id,
        len(text),
        text.startswith("/"),
        text[:120] + ("..." if len(text) > 120 else ""),
    )

    if not text.startswith("/"):
        # This is a natural language interactive chat with the AI
        llm_task = "telegram_chat"
        try:
            llm_config = settings.resolve_llm_config(llm_task)
            llm_profile_name = settings.resolve_llm_profile_name(llm_task)
        except Exception as cfg_exc:
            _tg_trace("拒绝进入LLM对话：LLM配置解析失败 chat_id=%s error=%s", chat_id, cfg_exc)
            await client._post("sendMessage", {
                "chat_id": chat_id,
                "text": f"⚠️ 当前 AI 对话引擎配置无效: {cfg_exc}",
            })
            return

        if not llm_config.enabled:
            _tg_trace("拒绝进入LLM对话：profile未启用 chat_id=%s", chat_id)
            await client._post("sendMessage", {
                "chat_id": chat_id, "text": "⚠️ 当前未开启 AI 对话引擎（telegram_chat profile enabled=false）。"
            })
            return
        if not llm_config.api_key:
            _tg_trace("拒绝进入LLM对话：缺少API Key chat_id=%s", chat_id)
            await client._post("sendMessage", {
                "chat_id": chat_id, "text": "⚠️ 当前 AI 对话引擎缺少 API Key（telegram_chat profile）。"
            })
            return

        _tg_trace(
            "进入LLM对话 chat_id=%s task=%s profile=%s provider=%s model=%s base_url=%s api_key_present=%s",
            chat_id,
            llm_task,
            llm_profile_name,
            llm_config.provider,
            llm_config.model,
            llm_config.base_url,
            bool(llm_config.api_key),
        )
        status_resp = await client._post("sendMessage", {
            "chat_id": chat_id,
            "text": "⏳ 正在思考...",
            "parse_mode": "HTML",
        })
        status_msg_id = _extract_message_id(status_resp)

        typing_task: asyncio.Task | None = None

        async def _keep_typing():
            """Repeat typing every 5s (Telegram shows ~5s per call) until cancelled."""
            while True:
                await asyncio.sleep(5)
                await client.send_chat_action(chat_id, "typing")

        try:
            await client.send_chat_action(chat_id, "typing")
            typing_task = asyncio.create_task(_keep_typing())

            from app.ai.openai_provider import OpenAICompatibleProvider
            from app.alerts.telegram_agent import TelegramAgent
            provider = OpenAICompatibleProvider(llm_config)
            agent = TelegramAgent(provider=provider, max_history=10)

            _tg_trace("准备调用 TelegramAgent.chat chat_id=%s", chat_id)
            stream_status = (client, chat_id, status_msg_id) if status_msg_id is not None else None
            response = await agent.chat(chat_id, text, stream_status_context=stream_status)
            _tg_trace(
                "TelegramAgent.chat返回 chat_id=%s 回复长度=%d model=%s",
                chat_id,
                len(response.get("text", "") or ""),
                response.get("model", ""),
            )
            await _send_ai_reply(client, chat_id, response.get("text", ""))
            
        except Exception as e:
            _tg_trace("LLM交互异常 chat_id=%s error=%s", chat_id, e)
            logger.exception("[telegram_dispatcher] Error handling interactive chat message: %s", e)
            await client._post("sendMessage", {"chat_id": chat_id, "text": f"❌ Agent 运行时故障: {e}"})
        finally:
            if typing_task is not None:
                typing_task.cancel()
                try:
                    await typing_task
                except asyncio.CancelledError:
                    pass
            if status_msg_id is not None:
                await client.delete_message(status_msg_id, chat_id=chat_id)

        return

        
    parts = text.split()
    cmd = parts[0].lower()
    args = parts[1:]
    _tg_trace("命令消息 chat_id=%s cmd=%s args=%s", chat_id, cmd, args)

    status_msg_id: int | None = None
    try:
        status_resp = await client._post("sendMessage", {
            "chat_id": chat_id,
            "text": "⏳ 正在处理...",
            "parse_mode": "HTML",
        })
        status_msg_id = _extract_message_id(status_resp)

        if cmd == "/help":
            await _cmd_help(client, chat_id)
        elif cmd == "/status":
            await _cmd_status(client, chat_id, settings)
        elif cmd == "/last":
            await _cmd_last(client, chat_id, args)
        elif cmd == "/analyze":
            await _cmd_analyze(client, chat_id, args, settings)
        else:
            await client._post("sendMessage", {
                "chat_id": chat_id,
                "text": "❓ 未知命令。发送 /help 获取帮助。",
            })
    except Exception as e:
        logger.exception("[telegram_dispatcher] Error executing command %s: %s", cmd, e)
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"❌ 执行命令时出错: {e}"})
    finally:
        if status_msg_id is not None:
            await client.delete_message(status_msg_id, chat_id=chat_id)


async def _handle_callback_query(client: TelegramClient, callback_query: dict[str, Any], settings):
    """Handle Inline Keyboard button clicks"""
    cb_id = callback_query.get("id")
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
    data: str = callback_query.get("data", "")
    _tg_trace("收到 callback_query chat_id=%s cb_id=%s data=%r", chat_id, cb_id, data)
    
    # Needs to answer the callback query quickly to remove loading state
    # Acknowledge first
    asyncio.create_task(client._post("answerCallbackQuery", {"callback_query_id": cb_id}))

    if not chat_id:
        _tg_trace("callback处理跳过：缺少chat_id")
        return

    allowed_chats = settings.telegram_allowed_chats
    if allowed_chats and chat_id not in allowed_chats:
        _tg_trace("callback被白名单拦截 chat_id=%s allowed=%s", chat_id, allowed_chats)
        return

    if data.startswith("explain:"):
        source_id = data.split(":", 1)[1]
        status_resp = await client._post("sendMessage", {
            "chat_id": chat_id,
            "text": f"⏳ 正在请求大模型分析信号源 `{source_id}` 的深度逻辑...",
            "parse_mode": "Markdown"
        })
        status_msg_id = _extract_message_id(status_resp)
        asyncio.create_task(_process_explain_signal(client, chat_id, source_id, settings, status_msg_id))

async def _process_explain_signal(
    client: TelegramClient, chat_id: int, source_id: str, settings: Any,
    status_message_id: int | None = None,
):
    from sqlalchemy import select
    from app.db.session import SessionLocal
    from app.db.models import AiSignal
    
    with SessionLocal() as db:
        try:
            signal_id = int(source_id)
            signal: AiSignal | None = db.scalar(select(AiSignal).where(AiSignal.id == signal_id))
        except ValueError:
            await client._post("sendMessage", {"chat_id": chat_id, "text": "❌ 无效的信号 ID。"})
            if status_message_id is not None:
                await client.delete_message(status_message_id, chat_id=chat_id)
            return
            
    if not signal:
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"❌ 未在数据库中找到 ID=`{source_id}` 的信号记录。"})
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)
        return
        
    prompt = (
        f"请用简洁的中文解释以下交易信号的逻辑，不要使用过于复杂的术语，重点说明为什么会得出这个结论。\n\n"
        f"币种: {signal.symbol}\n"
        f"方向: {signal.direction}\n"
        f"给出的理由摘要: {signal.reasoning}\n\n"
        f"如果合适，请简要评估这个信号的风险和建议。"
    )
    
    llm_task = "telegram_chat"
    try:
        llm_config = settings.resolve_llm_config(llm_task)
    except Exception as e:
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"⚠️ 配置加载失败: {e}"})
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)
        return
        
    if not llm_config.enabled or not llm_config.api_key:
         await client._post("sendMessage", {"chat_id": chat_id, "text": "⚠️ 当前未开启 AI 或缺少 API Key。"})
         if status_message_id is not None:
             await client.delete_message(status_message_id, chat_id=chat_id)
         return
         
    try:
        from app.ai.openai_provider import OpenAICompatibleProvider
        provider = OpenAICompatibleProvider(llm_config)
        messages = [
            {"role": "system", "content": "你是专业的加密市场助手，请用简洁清晰的中文解释交易信号与风险。"},
            {"role": "user", "content": prompt}
        ]
        
        response = await provider.generate_response(messages=messages, max_tokens=1000)
        content = response.get("content", "")
        if not content:
            content = "抱歉，模型未能生成解释。"
            
        await _send_ai_reply(client, chat_id, f"🧾 **信号 {source_id} 深度解读**:\n\n{content}")
    except Exception as e:
        logger.exception("[telegram_dispatcher] Error explaining signal %s: %s", source_id, e)
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"❌ 解读信号时发生错误: {e}"})
    finally:
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)

# --- Commands ---

async def _cmd_help(client: TelegramClient, chat_id: int):
    lines = [
        "🤖 <b>Crypto Sentinel v0.1.0</b>\n",
        "<b>可用命令清单：</b>",
        "👉 /status : 查看系统状态与关注列表",
        "👉 /last <code>&lt;symbol&gt;</code> : 获取指定币种最新一条有效 AI 信号",
        "👉 /analyze <code>&lt;symbol&gt; [1m|5m|1h]</code> : 立即触发一次强制 AI 推理",
        "👉 /help : 显示帮助信息\n",
        "<i>当前系统已挂载 Webhook 双向通信引擎（Phase 1）。</i>"
    ]
    await client._post("sendMessage", {"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML"})

async def _cmd_status(client: TelegramClient, chat_id: int, settings):
    from app.services.health_probe import quick_db_health_and_worker
    db_ok, worker_last_seen = quick_db_health_and_worker(settings.database_url, settings.worker_id)
    
    from app.alerts.message_builder import fmt_dt_bjt
    seen_str = fmt_dt_bjt(worker_last_seen) if worker_last_seen else "未知 / 离线"
    db_status = "在线 ✅" if db_ok else "故障 ❌"
    
    # Calculate uptime relative
    worker_status = "在线 ✅"
    if worker_last_seen:
        diff_secs = (datetime.now(timezone.utc) - ensure_utc(worker_last_seen)).total_seconds()
        if diff_secs > settings.worker_heartbeat_seconds * 3:
            worker_status = "失联 ⚠️"

    lines = [
        f"📊 <b>系统状态</b>",
        f"数据库连接: {db_status}",
        f"后台进程: <code>{settings.worker_id}</code> ({worker_status})",
        f"最后心跳: {seen_str}",
        f"",
        f"👀 <b>监听列表 ({len(settings.watchlist_symbols)})</b>:",
        f"<code>{', '.join(settings.watchlist_symbols)}</code>",
    ]
    await client._post("sendMessage", {"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML"})

async def _cmd_last(client: TelegramClient, chat_id: int, args: list[str]):
    if not args:
        await client._post("sendMessage", {"chat_id": chat_id, "text": "⚠️ 请指定币种。用法：`/last BTCUSDT`", "parse_mode": "Markdown"})
        return
        
    symbol = args[0].upper()
    
    with SessionLocal() as db:
        # Get safest existing AiSignal
        signal = db.scalar(
            select(AiSignal)
            .where(AiSignal.symbol == symbol)
            .where(AiSignal.direction != "HOLD")
            .order_by(desc(AiSignal.created_at))
            .limit(1)
        )
        
        if not signal:
            await client._post("sendMessage", {"chat_id": chat_id, "text": f"📥 数据库中没有找到 `{symbol}` 的近期交易信号。", "parse_mode": "Markdown"})
            return
            
        tm = build_ai_signal_message(signal, source_id=signal.id)
        
        # Manually construct inline payload since client.send_message only supports default chat right now
        payload = {
            "chat_id": chat_id,
            "text": tm.text,
            "parse_mode": tm.parse_mode,
            "disable_web_page_preview": tm.disable_web_page_preview,
        }
        
        if tm.reply_markup:
            import json
            payload["reply_markup"] = json.dumps(tm.reply_markup)
            
        await client._post("sendMessage", payload)

async def _cmd_analyze(client: TelegramClient, chat_id: int, args: list[str], settings: Any):
    if not args:
        await client._post("sendMessage", {"chat_id": chat_id, "text": "⚠️ 请指定币种。用法：`/analyze BTCUSDT`", "parse_mode": "Markdown"})
        return
        
    symbol = args[0].upper()
    status_resp = await client._post("sendMessage", {
        "chat_id": chat_id,
        "text": "⏳ 正在分析，请稍候...",
        "parse_mode": "Markdown",
    })
    status_msg_id = _extract_message_id(status_resp)
    asyncio.create_task(_process_analyze_signal(client, chat_id, symbol, settings, status_msg_id))

async def _process_analyze_signal(
    client: TelegramClient, chat_id: int, symbol: str, settings: Any,
    status_message_id: int | None = None,
):
    from sqlalchemy import select
    from app.db.session import SessionLocal
    from app.ai.market_context_builder import build_market_analysis_context
    from app.db.repository import (
        get_latest_market_metric,
        get_recent_market_metrics,
        list_recent_ohlcv,
        get_latest_funding_snapshots,
        get_latest_futures_account_snapshot,
        get_latest_margin_account_snapshot,
        list_alerts,
        insert_ai_signal
    )
    from app.services.metric_utils import metric_to_dict
    from app.alerts.message_builder import build_ai_signal_message
    from app.ai.analyst import MarketAnalyst, attach_context_digest_to_analysis_json
    from app.ai.openai_provider import OpenAICompatibleProvider

    try:
        llm_config = settings.resolve_llm_config("market")
    except Exception as e:
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"⚠️ 配置加载失败: {e}"})
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)
        return
        
    if not llm_config.enabled or not llm_config.api_key:
         await client._post("sendMessage", {"chat_id": chat_id, "text": "⚠️ 当前未开启市场 AI（market profile）或缺少 API Key。"})
         if status_message_id is not None:
             await client.delete_message(status_message_id, chat_id=chat_id)
         return

    provider = OpenAICompatibleProvider(llm_config)
    analyst = MarketAnalyst(settings, provider, llm_config)
    
    tf_data: dict[str, Any] = {}
    account_snapshot: dict[str, Any] | None = None
    with SessionLocal() as session:
        all_tfs = ["1m"] + settings.multi_tf_interval_list
        for tf in all_tfs:
            latest_metric = get_latest_market_metric(session, symbol=symbol, timeframe=tf)
            if latest_metric is None:
                continue
            history_rows = get_recent_market_metrics(session, symbol=symbol, timeframe=tf, limit=settings.ai_history_candles)
            history = []
            for h in history_rows:
                history.append({
                    "close": h.close,
                    "high": h.close * 1.001 if h.close else 0,
                    "low": h.close * 0.999 if h.close else 0,
                })
            recent_candles = list_recent_ohlcv(session, symbol=symbol, timeframe=tf, limit=settings.ai_history_candles)
            candle_history = [
                {"close": c.close, "high": c.high, "low": c.low, "open": c.open}
                for c in recent_candles
            ]
            tf_data[tf] = {
                "latest": metric_to_dict(latest_metric),
                "history": candle_history,
            }
            
        funding_rows = get_latest_funding_snapshots(session, symbols=[symbol])
        funding_current = None
        if funding_rows:
            f = funding_rows[0]
            funding_current = {
                "symbol": f.symbol,
                "ts": f.ts,
                "mark_price": f.mark_price,
                "index_price": f.index_price,
                "last_funding_rate": f.last_funding_rate,
                "open_interest": f.open_interest,
                "open_interest_value": f.open_interest_value,
            }

        recent_alerts_rows = list_alerts(session, limit=10)
        recent_alerts = [
            {
                "symbol": a.symbol,
                "alert_type": a.alert_type,
                "severity": a.severity,
                "reason": a.reason,
                "ts": a.ts.isoformat() if a.ts else "",
            }
            for a in recent_alerts_rows
            if a.symbol == symbol
        ]
        futures_row = get_latest_futures_account_snapshot(session)
        margin_row = get_latest_margin_account_snapshot(session)

        def _to_float(value: Any) -> float | None:
            try:
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        def _liq_distance(mark_price: float, liq_price: float, position_amt: float) -> float | None:
            if mark_price <= 0 or liq_price <= 0 or position_amt == 0:
                return None
            if position_amt > 0:
                val = (mark_price - liq_price) / mark_price
            else:
                val = (liq_price - mark_price) / mark_price
            return (val * 100.0) if val >= 0 else None

        futures_payload = {
            "total_margin_balance": _to_float(getattr(futures_row, "total_margin_balance", None)),
            "available_balance": _to_float(getattr(futures_row, "available_balance", None)),
            "total_maint_margin": _to_float(getattr(futures_row, "total_maint_margin", None)),
            "position_amt": _to_float(getattr(futures_row, "btc_position_amt", None)),
            "mark_price": _to_float(getattr(futures_row, "btc_mark_price", None)),
            "liquidation_price": _to_float(getattr(futures_row, "btc_liquidation_price", None)),
            "unrealized_pnl": _to_float(getattr(futures_row, "btc_unrealized_pnl", None)),
        }
        if (
            futures_payload["mark_price"] is not None
            and futures_payload["liquidation_price"] is not None
            and futures_payload["position_amt"] is not None
        ):
            futures_payload["liq_distance_pct"] = _liq_distance(
                float(futures_payload["mark_price"]),
                float(futures_payload["liquidation_price"]),
                float(futures_payload["position_amt"]),
            )
        margin_payload = {
            "margin_level": _to_float(getattr(margin_row, "margin_level", None)),
            "margin_call_bar": _to_float(getattr(margin_row, "margin_call_bar", None)),
            "force_liquidation_bar": _to_float(getattr(margin_row, "force_liquidation_bar", None)),
            "total_liability_of_btc": _to_float(getattr(margin_row, "total_liability_of_btc", None)),
        }
        as_of = getattr(futures_row, "ts", None) or getattr(margin_row, "ts", None)
        account_snapshot = {
            "watch_symbol": settings.account_watch_symbol.upper(),
            "as_of_utc": as_of,
            "futures": futures_payload,
            "margin": margin_payload,
            "risk_flags": {
                "available_balance_low": (
                    futures_payload.get("available_balance") is not None
                    and float(futures_payload["available_balance"]) < float(settings.account_alert_min_available_balance)
                ),
                "margin_near_call": (
                    margin_payload.get("margin_level") is not None
                    and margin_payload.get("margin_call_bar") is not None
                    and float(margin_payload["margin_level"]) <= float(margin_payload["margin_call_bar"])
                ),
            },
        }
        
    if not tf_data:
        await client._post("sendMessage", {"chat_id": chat_id, "text": f"❌ 数据库中没有 `{symbol}` 的行情数据，无法分析。", "parse_mode": "Markdown"})
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)
        return
    
    try:
        context = build_market_analysis_context(
            symbol=symbol,
            snapshots=tf_data,
            recent_alerts=recent_alerts,
            funding_current=funding_current,
            funding_history=None,
            youtube_consensus=None,
            youtube_insights=None,
            account_snapshot=account_snapshot,
            expected_timeframes=["4h", "1h", "15m", "5m", "1m"],
        )
        sigs, llm_meta = await analyst.analyze(symbol, tf_data, context=context)
        
        if not sigs:
            await client._post("sendMessage", {"chat_id": chat_id, "text": "⚠️ 模型未能生成有效的交易信号。"})
            return
            
        sig = sigs[0]
        sig.analysis_json = attach_context_digest_to_analysis_json(sig.analysis_json, context)
        
        with SessionLocal() as session:
            signal_ts = datetime.now(timezone.utc)
            payload = {
                "symbol": sig.symbol,
                "timeframe": "1m",
                "ts": signal_ts,
                "direction": sig.direction,
                "entry_price": sig.entry_price,
                "take_profit": sig.take_profit,
                "stop_loss": sig.stop_loss,
                "confidence": sig.confidence,
                "reasoning": sig.reasoning,
                "model_name": sig.model_name,
                "model_requested": sig.model_requested,
                "prompt_tokens": sig.prompt_tokens,
                "completion_tokens": sig.completion_tokens,
                "market_regime": sig.market_regime,
                "analysis_json": sig.analysis_json,
                "sent_to_telegram": True
            }
            row = insert_ai_signal(session, payload)
            source_id = row.id if row else None
            
        tm = build_ai_signal_message(sig, source_id=source_id)
        
        send_payload = {
            "chat_id": chat_id,
            "text": tm.text,
            "parse_mode": tm.parse_mode,
            "disable_web_page_preview": tm.disable_web_page_preview,
        }
        if tm.reply_markup:
            import json
            send_payload["reply_markup"] = json.dumps(tm.reply_markup)
            
        await client._post("sendMessage", send_payload)
        
    except Exception as e:
        logger.exception("[telegram_dispatcher] AI analysis failed for %s", symbol)
        await client._post("sendMessage", {"chat_id": chat_id, "text": "❌ 分析时发生错误（内部处理失败）。请稍后重试。"})
    finally:
        if status_message_id is not None:
            await client.delete_message(status_message_id, chat_id=chat_id)
