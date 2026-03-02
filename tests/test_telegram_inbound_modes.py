from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.main import app
import app.web.api_telegram as api_telegram
import app.alerts.telegram_dispatcher as telegram_dispatcher


def test_webhook_route_ignored_early_in_polling_mode(monkeypatch):
    monkeypatch.setattr(api_telegram.settings, "telegram_inbound_mode", "polling")
    client = TestClient(app)

    resp = client.post("/api/telegram/webhook")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ignored", "reason": "telegram_inbound_mode_polling"}


async def _dummy_handler(*_args, **_kwargs):
    return None


def test_webhook_route_still_accepts_in_webhook_mode(monkeypatch):
    monkeypatch.setattr(api_telegram.settings, "telegram_inbound_mode", "webhook")
    monkeypatch.setattr(api_telegram.settings, "telegram_enabled", True)
    monkeypatch.setattr(api_telegram.settings, "telegram_webhook_secret", "")

    import app.alerts.telegram_dispatcher as dispatcher_module

    monkeypatch.setattr(dispatcher_module, "process_telegram_update", _dummy_handler)
    client = TestClient(app)

    resp = client.post("/api/telegram/webhook", json={"update_id": 12345, "message": {"chat": {"id": 1}, "text": "hi"}})

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_dispatcher_routes_edited_message_to_handle_message(monkeypatch):
    telegram_dispatcher._processed_updates.clear()

    fake_settings = SimpleNamespace(
        telegram_enabled=True,
        telegram_bot_token="test-token",
        telegram_chat_id="123",
    )

    class _DummyClient:
        def __init__(self, *args, **kwargs):
            pass

    handle_message = AsyncMock()
    handle_callback = AsyncMock()

    monkeypatch.setattr(telegram_dispatcher, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(telegram_dispatcher, "TelegramClient", _DummyClient)
    monkeypatch.setattr(telegram_dispatcher, "_handle_message", handle_message)
    monkeypatch.setattr(telegram_dispatcher, "_handle_callback_query", handle_callback)

    import asyncio

    asyncio.run(
        telegram_dispatcher.process_telegram_update(
            {"update_id": 777, "edited_message": {"chat": {"id": 1}, "text": "edited hello"}}
        )
    )

    assert handle_message.await_count == 1
    assert handle_callback.await_count == 0
    args = handle_message.await_args.args
    assert args[1]["text"] == "edited hello"
