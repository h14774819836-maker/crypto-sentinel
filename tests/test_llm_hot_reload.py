from __future__ import annotations

from types import SimpleNamespace

import app.web.api_telegram as api_telegram
import app.web.router as web_router
import app.web.views as views
from app.ai.llm_runtime_reload import (
    apply_llm_config_in_api_process,
    read_llm_reload_ack,
    read_llm_reload_signal,
    write_llm_reload_ack,
    write_llm_reload_signal,
)
from app.config import get_settings


def test_signal_and_ack_file_roundtrip(tmp_path):
    signal_file = tmp_path / "signals" / "llm_hot_reload_signal.json"
    ack_file = tmp_path / "signals" / "llm_hot_reload_ack.json"

    revision = write_llm_reload_signal(str(signal_file), source="pytest", reason="roundtrip")
    signal = read_llm_reload_signal(str(signal_file))
    assert signal is not None
    assert signal["revision"] == revision
    assert signal["source"] == "pytest"
    assert signal["reason"] == "roundtrip"

    write_llm_reload_ack(str(ack_file), revision=revision, status="ok", details={"market": {"provider": "openrouter"}})
    ack = read_llm_reload_ack(str(ack_file))
    assert ack is not None
    assert ack["revision"] == revision
    assert ack["status"] == "ok"
    assert ack["details"]["market"]["provider"] == "openrouter"


def test_apply_llm_config_in_api_process_refreshes_module_settings(monkeypatch):
    old_views_settings = views.settings
    old_web_router_settings = web_router.settings
    old_api_tg_settings = api_telegram.settings

    monkeypatch.setenv("APP_VERSION", "pytest-hot-reload-version")
    refreshed = apply_llm_config_in_api_process()

    assert refreshed.app_version == "pytest-hot-reload-version"
    assert views.settings is refreshed
    assert web_router.settings is refreshed
    assert api_telegram.settings is refreshed
    assert hasattr(views, "router")

    # keep test isolation for later tests
    get_settings.cache_clear()
    views.settings = old_views_settings
    web_router.settings = old_web_router_settings
    api_telegram.settings = old_api_tg_settings
