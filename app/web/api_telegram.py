from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, BackgroundTasks

from app.config import get_settings

import logging
logger = logging.getLogger(__name__)

router = APIRouter()
settings = get_settings()


def _tg_trace(msg: str, *args) -> None:
    # 用 WARNING 提高终端可见性，方便排查 Telegram 交互链路
    logger.warning("[TG交互追踪][webhook] " + msg, *args)

@router.post("/api/telegram/webhook")
async def telegram_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    if settings.telegram_inbound_mode_normalized == "polling":
        _tg_trace("当前为polling模式，忽略webhook请求")
        return {"status": "ignored", "reason": "telegram_inbound_mode_polling"}

    _tg_trace("收到Webhook请求 secret_header_present=%s", bool(x_telegram_bot_api_secret_token))

    if not settings.telegram_enabled:
        _tg_trace("忽略Webhook：TELEGRAM_ENABLED=false")
        return {"status": "ignored", "reason": "telegram_disabled"}
        
    secret = settings.telegram_webhook_secret
    if secret and x_telegram_bot_api_secret_token != secret:
        _tg_trace("拒绝Webhook：secret不匹配")
        raise HTTPException(status_code=401, detail="Unauthorized webhook source")
        
    try:
        update_data = await request.json()
    except Exception:
        _tg_trace("拒绝Webhook：JSON无效")
        raise HTTPException(status_code=400, detail="Invalid JSON format")
        
    update_id = update_data.get("update_id")
    if not update_id:
        _tg_trace("忽略Webhook：payload缺少update_id top_keys=%s", list(update_data.keys())[:10])
        return {"status": "ignored", "reason": "no_update_id"}

    msg = update_data.get("message") or {}
    cb = update_data.get("callback_query") or {}
    msg_chat_id = msg.get("chat", {}).get("id")
    msg_text = msg.get("text", "")
    cb_chat_id = cb.get("message", {}).get("chat", {}).get("id")
    _tg_trace(
        "解析成功 update_id=%s has_message=%s has_callback=%s msg_chat_id=%s cb_chat_id=%s text_preview=%r",
        update_id,
        "message" in update_data,
        "callback_query" in update_data,
        msg_chat_id,
        cb_chat_id,
        (msg_text[:120] + "...") if isinstance(msg_text, str) and len(msg_text) > 120 else msg_text,
    )
        
    # Schedule processing safely in the background
    # Importing inline to avoid circular dependencies where possible
    from app.alerts.telegram_dispatcher import process_telegram_update
    background_tasks.add_task(process_telegram_update, update_data)
    _tg_trace("已加入后台处理队列 update_id=%s", update_id)
    
    # Fast ACK to Telegram server
    _tg_trace("Webhook快速ACK完成 update_id=%s", update_id)
    return {"status": "ok"}
