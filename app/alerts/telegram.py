from __future__ import annotations

import asyncio

import httpx
import json
from dataclasses import dataclass

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
        result = await self.send_message_with_result(message)
        return result.ok

    async def send_message_with_result(self, message: TelegramMessage) -> "TelegramSendResult":
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info("Telegram disabled. Message: %s", message.text)
            return TelegramSendResult(ok=False, message_id=None, raw=None)

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
        if message.reply_to_message_id is not None:
            payload["reply_to_message_id"] = int(message.reply_to_message_id)
        
        if message.reply_markup:
            payload["reply_markup"] = json.dumps(message.reply_markup)

        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    message_id = None
                    if isinstance(data, dict):
                        result_obj = data.get("result") or {}
                        if isinstance(result_obj, dict):
                            raw_id = result_obj.get("message_id")
                            if isinstance(raw_id, int):
                                message_id = raw_id
                return TelegramSendResult(ok=True, message_id=message_id, raw=data if isinstance(data, dict) else None)
            except Exception as exc:
                wait_seconds = attempt
                logger.warning("Telegram send failed (attempt=%d): %s", attempt, exc)
                await asyncio.sleep(wait_seconds)

        return TelegramSendResult(ok=False, message_id=None, raw=None)

    async def delete_message(self, message_id: int, chat_id: str | int | None = None) -> bool:
        """Delete a message. chat_id: target chat (defaults to self.chat_id)."""
        cid = chat_id if chat_id is not None else self.chat_id
        if not self.enabled or not self.bot_token or not cid:
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/deleteMessage"
        payload = {"chat_id": cid, "message_id": int(message_id)}
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                return bool(data.get("ok"))
        except Exception as exc:
            logger.warning("Telegram delete_message failed message_id=%s: %s", message_id, exc)
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


@dataclass(slots=True)
class TelegramSendResult:
    ok: bool
    message_id: int | None
    raw: dict | None
