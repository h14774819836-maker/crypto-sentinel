from __future__ import annotations

import os
from types import SimpleNamespace

import app.web.api_telegram as api_telegram
import app.web.router as web_router
import app.web.views as views
from app.ai.llm_runtime_reload import (
    apply_llm_config_in_api_process,
    refresh_llm_env_vars_from_dotenv,
    read_llm_reload_ack,
    read_llm_reload_signal,
    write_llm_reload_ack,
    write_llm_reload_signal,
)
from app.config import get_settings


def test_signal_and_ack_file_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("LLM_HOT_RELOAD_USE_REDIS", "false")
    get_settings.cache_clear()
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
    tracked_env_keys = {
        "DEEPSEEK_API_KEY",
        "OPENROUTER_API_KEY",
        "OPENAI_API_KEY",
        "ARK_API_KEY",
        "NVIDIA_NIM_API_KEY",
        "LLM_PROFILES_JSON",
        "LLM_TASK_ROUTING_JSON",
        "LLM_ALLOWED_MODELS",
        "LLM_HOT_RELOAD_SIGNAL_FILE",
        "LLM_HOT_RELOAD_ACK_FILE",
        "TELEGRAM_ALERT_TEMPLATE_STYLE",
        "TELEGRAM_ALERT_INCLUDE_DEBUG",
    }
    tracked_env_keys.update({k for k in os.environ.keys() if k.startswith("LLM_")})
    env_snapshot = {k: os.environ.get(k) for k in tracked_env_keys}

    try:
        monkeypatch.setenv("APP_VERSION", "pytest-hot-reload-version")
        refreshed = apply_llm_config_in_api_process()

        assert refreshed.app_version == "pytest-hot-reload-version"
        assert views.settings is refreshed
        assert web_router.settings is refreshed
        assert api_telegram.settings is refreshed
        assert hasattr(views, "router")
    finally:
        for key, original in env_snapshot.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original

    # keep test isolation for later tests
    get_settings.cache_clear()
    views.settings = old_views_settings
    web_router.settings = old_web_router_settings
    api_telegram.settings = old_api_tg_settings


def test_redis_publish_and_read_acks(monkeypatch):
    """Test Redis backend: publish signal and read ACKs via mock."""
    import json as _json

    published: list[tuple[str, str]] = []
    ack_store: dict[str, str] = {}

    class MockRedis:
        def publish(self, channel: str, message: str):
            published.append((channel, message))

        def hgetall(self, key: str):
            return {k.encode(): v.encode() if isinstance(v, str) else v for k, v in ack_store.items()}

        def hset(self, key: str, field: str, value: str):
            ack_store[field] = value

    def mock_from_url(url: str):
        return MockRedis()

    import redis as _redis_module
    monkeypatch.setattr(_redis_module, "from_url", mock_from_url)
    monkeypatch.setenv("LLM_HOT_RELOAD_USE_REDIS", "true")
    get_settings.cache_clear()

    from app.ai.llm_runtime_reload import (
        _publish_llm_reload_signal_redis,
        read_llm_reload_acks_redis,
        write_llm_reload_ack_redis,
    )

    revision = _publish_llm_reload_signal_redis("redis://localhost:6379/0", source="pytest", reason="redis_test")
    assert len(published) == 1
    assert published[0][0] == "llm:reload"
    payload = _json.loads(published[0][1])
    assert payload["revision"] == revision
    assert payload["source"] == "pytest"

    write_llm_reload_ack_redis("redis://localhost:6379/0", "worker-1", revision, "ok", {"market": {"provider": "openrouter"}})
    acks = read_llm_reload_acks_redis("redis://localhost:6379/0")
    assert "worker-1" in acks
    assert acks["worker-1"]["revision"] == revision
    assert acks["worker-1"]["status"] == "ok"
    assert acks["worker-1"]["details"]["market"]["provider"] == "openrouter"


def test_refresh_llm_env_vars_from_dotenv_includes_nvidia_nim_key(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text("NVIDIA_NIM_API_KEY=nim_test_key\nLLM_PROFILES_JSON={}\n", encoding="utf-8")
    snapshot = {
        "NVIDIA_NIM_API_KEY": os.environ.get("NVIDIA_NIM_API_KEY"),
        "LLM_PROFILES_JSON": os.environ.get("LLM_PROFILES_JSON"),
    }
    monkeypatch.delenv("NVIDIA_NIM_API_KEY", raising=False)

    try:
        refresh_llm_env_vars_from_dotenv(str(env_path))
        assert os.environ.get("NVIDIA_NIM_API_KEY") == "nim_test_key"
        assert os.environ.get("LLM_PROFILES_JSON") == "{}"
    finally:
        for key, original in snapshot.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original
