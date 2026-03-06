from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.ai.llm_runtime_reload import read_llm_reload_ack, write_llm_reload_signal
from app.config import get_settings
import app.worker.llm_hot_reload as worker_reload


def test_worker_maybe_reload_applies_new_revision_and_writes_ack(tmp_path, monkeypatch):
    monkeypatch.setenv("LLM_HOT_RELOAD_USE_REDIS", "false")
    get_settings.cache_clear()
    signal_file = tmp_path / "llm_hot_reload_signal.json"
    ack_file = tmp_path / "llm_hot_reload_ack.json"
    revision = write_llm_reload_signal(str(signal_file), source="pytest", reason="worker_reload")

    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            llm_hot_reload_signal_file=str(signal_file),
            llm_hot_reload_ack_file=str(ack_file),
        ),
        llm_reload_revision_applied="",
        llm_reload_last_check_ts=None,
    )

    applied_calls: list[str] = []

    def _fake_apply(rt):
        applied_calls.append("called")
        rt.settings = SimpleNamespace(
            llm_hot_reload_signal_file=str(signal_file),
            llm_hot_reload_ack_file=str(ack_file),
        )
        return {"market": {"provider": "openrouter", "model": "google/gemini-3.1-pro-preview"}}

    monkeypatch.setattr(worker_reload, "apply_llm_config_to_worker_runtime", _fake_apply)

    asyncio.run(worker_reload.maybe_reload_llm_runtime_from_signal(runtime))

    assert applied_calls == ["called"]
    assert runtime.llm_reload_revision_applied == revision
    assert runtime.llm_reload_last_check_ts is not None

    ack = read_llm_reload_ack(str(ack_file))
    assert ack is not None
    assert ack["revision"] == revision
    assert ack["status"] == "ok"
    assert ack["details"]["market"]["provider"] == "openrouter"


def test_split_worker_subscriber_rejects_file_fallback():
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            worker_role_normalized="core",
            llm_hot_reload_use_redis=False,
            llm_hot_reload_poll_seconds=1,
            redis_url="redis://localhost:6379/0",
        ),
        llm_reload_revision_applied="",
    )

    with pytest.raises(RuntimeError):
        asyncio.run(worker_reload.run_llm_reload_subscriber(runtime))


def test_split_worker_subscriber_requires_redis_available(monkeypatch):
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            worker_role_normalized="ai",
            llm_hot_reload_use_redis=True,
            llm_hot_reload_poll_seconds=1,
            redis_url="redis://localhost:6379/0",
        ),
        llm_reload_revision_applied="",
    )

    monkeypatch.setattr(worker_reload, "_redis_available", lambda _url: False)
    with pytest.raises(RuntimeError):
        asyncio.run(worker_reload.run_llm_reload_subscriber(runtime))
