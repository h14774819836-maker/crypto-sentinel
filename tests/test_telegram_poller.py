from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import app.alerts.telegram_poller as poller_mod
from app.alerts.telegram_poller import TelegramPoller


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.status_code = 200
        self.text = json.dumps(payload, ensure_ascii=False)

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, responses: list[_FakeResponse], captured_params: list[dict] | None = None):
        self._responses = list(responses)
        self._captured_params = captured_params if captured_params is not None else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None):
        self._captured_params.append(dict(params or {}))
        if not self._responses:
            return _FakeResponse({"ok": True, "result": []})
        return self._responses.pop(0)

    async def post(self, url, params=None, json=None):
        return _FakeResponse({"ok": True, "result": True})


def _make_poller(tmp_path: Path, handler):
    return TelegramPoller(
        token="test-token",
        handle_update=handler,
        timeout_seconds=1,
        interval_seconds=0,
        state_file=str(tmp_path / "state" / "telegram_poller_state.json"),
        auto_delete_webhook=False,
        drop_pending_updates=True,
    )


def test_state_file_save_and_load_next_offset(tmp_path):
    async def _noop(_update):
        return None

    poller = _make_poller(tmp_path, _noop)
    poller._save_next_offset(42)

    state_path = Path(poller.state_file)
    assert state_path.exists()
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["next_offset"] == 42
    assert "updated_at" in payload
    assert poller._load_next_offset() == 42


@pytest.mark.anyio
async def test_run_forever_advances_and_persists_offset_before_handle(monkeypatch, tmp_path):
    events: list[tuple[str, int]] = []
    captured_params: list[dict] = []

    async def handler(update: dict):
        events.append(("handle", update["update_id"]))
        poller.stop()

    poller = _make_poller(tmp_path, handler)

    async def _bootstrap_noop(_client):
        return None

    monkeypatch.setattr(poller, "_bootstrap_webhook_mode", _bootstrap_noop)

    def _save_spy(next_offset: int):
        events.append(("save", next_offset))

    monkeypatch.setattr(poller, "_save_next_offset", _save_spy)

    fake_client = _FakeAsyncClient(
        responses=[
            _FakeResponse(
                {
                    "ok": True,
                    "result": [{"update_id": 5, "edited_message": {"chat": {"id": 1}, "text": "edited"}}],
                }
            )
        ],
        captured_params=captured_params,
    )
    monkeypatch.setattr(poller_mod.httpx, "AsyncClient", lambda *args, **kwargs: fake_client)

    await poller.run_forever()

    assert events[:2] == [("save", 6), ("handle", 5)]
    assert captured_params, "expected getUpdates request params to be captured"
    params = captured_params[0]
    assert "offset" not in params
    assert json.loads(params["allowed_updates"]) == ["message", "edited_message", "callback_query"]


@pytest.mark.anyio
async def test_run_forever_handles_cancelled_error_cleanly(monkeypatch, tmp_path):
    async def _noop(_update):
        return None

    poller = _make_poller(tmp_path, _noop)

    async def _bootstrap_noop(_client):
        return None

    monkeypatch.setattr(poller, "_bootstrap_webhook_mode", _bootstrap_noop)

    class _CancelClient(_FakeAsyncClient):
        async def get(self, url, params=None):
            raise asyncio.CancelledError()

    monkeypatch.setattr(poller_mod.httpx, "AsyncClient", lambda *args, **kwargs: _CancelClient([]))

    await poller.run_forever()
