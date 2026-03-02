from __future__ import annotations

import asyncio

import httpx
import json

from app.config import Settings
from app.logging import logger
from app.alerts.message_builder import TelegramMessage


def _tg_trace(msg: str, *args) -> None:
    logger.warning("[TG交互追踪][tg_api] " + msg, *args)


class TelegramClient:
    def __init__(self, settings: Settings = None, bot_token: str = None, chat_id: str = None):
        if settings:
            self.enabled = settings.telegram_enabled
            self.bot_token = settings.telegram_bot_token
            self.chat_id = settings.telegram_chat_id
        else:
            self.enabled = bool(bot_token)
            self.bot_token = bot_token
            self.chat_id = chat_id

    async def send_message(self, message: TelegramMessage) -> bool:
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info("Telegram disabled. Message: %s", message.text)
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        # Hard cap to prevent Telegram API failure
        safe_text = message.text
        if len(safe_text) > 4096:
            logger.warning("Telegram message length %d exceeds 4096 char limit, truncating", len(safe_text))
            safe_text = safe_text[:4090] + "..."

        payload = {
            "chat_id": self.chat_id, 
            "text": safe_text,
            "parse_mode": message.parse_mode,
            "disable_web_page_preview": message.disable_web_page_preview
        }
        
        if message.reply_markup:
            payload["reply_markup"] = json.dumps(message.reply_markup)

        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                return True
            except Exception as exc:
                wait_seconds = attempt
                logger.warning("Telegram send failed (attempt=%d): %s", attempt, exc)
                await asyncio.sleep(wait_seconds)

        return False

    async def send_text(self, text: str) -> bool:
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info("Telegram disabled. Message: %s", text)
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}

        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                return True
            except Exception as exc:
                wait_seconds = attempt
                logger.warning("Telegram send failed (attempt=%d): %s", attempt, exc)
                await asyncio.sleep(wait_seconds)

        return False

    async def _post(self, endpoint: str, payload: dict) -> dict:
        """Generic POST wrapper used by interactive Agent Dispatcher."""
        if not self.enabled or not self.bot_token:
            _tg_trace("跳过请求：Telegram未启用或bot_token缺失 endpoint=%s", endpoint)
            return {}

        url = f"https://api.telegram.org/bot{self.bot_token}/{endpoint}"
        payload = dict(payload)
        if endpoint in {"sendMessage", "sendChatAction", "answerCallbackQuery"}:
            txt = payload.get("text")
            preview = ""
            if isinstance(txt, str):
                preview = txt[:120] + ("..." if len(txt) > 120 else "")
            _tg_trace(
                "请求Telegram API endpoint=%s chat_id=%s parse_mode=%s text_len=%s text_preview=%r",
                endpoint,
                payload.get("chat_id"),
                payload.get("parse_mode"),
                len(txt) if isinstance(txt, str) else None,
                preview,
            )

        # Telegram sendMessage hard limit: 4096 chars. Prevent silent 400s for AI replies.
        if endpoint == "sendMessage":
            text = payload.get("text")
            if isinstance(text, str) and len(text) > 4096:
                logger.warning(
                    "Telegram _post sendMessage text length %d exceeds 4096 char limit, truncating",
                    len(text),
                )
                payload["text"] = text[:4090] + "..."

        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    if endpoint in {"sendMessage", "sendChatAction", "answerCallbackQuery"}:
                        _tg_trace(
                            "Telegram API成功 endpoint=%s http=%s ok_field=%s",
                            endpoint,
                            resp.status_code,
                            data.get("ok") if isinstance(data, dict) else None,
                        )
                    return data
            except Exception as exc:
                wait_seconds = attempt
                extra = ""
                if isinstance(exc, httpx.HTTPStatusError):
                    try:
                        extra = f" | body={exc.response.text}"
                    except Exception:
                        extra = ""
                logger.warning(
                    "Telegram _post %s failed (attempt=%d): %s%s",
                    endpoint,
                    attempt,
                    exc,
                    extra,
                )
                if endpoint in {"sendMessage", "sendChatAction", "answerCallbackQuery"}:
                    _tg_trace("Telegram API失败 endpoint=%s 第%d次 error=%s", endpoint, attempt, exc)
                await asyncio.sleep(wait_seconds)

        return {}
