from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
import app.config as config
import app.web.views as views

# All tests that call admin-protected endpoints need a valid ADMIN_TOKEN.
_TEST_ADMIN_TOKEN = "test-token-for-llm-config"


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_TEST_ADMIN_TOKEN}"}


def _ensure_admin_token(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", _TEST_ADMIN_TOKEN)
    config.get_settings.cache_clear()


def test_llm_config_get_includes_new_ui_and_hot_reload_fields(monkeypatch):
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)

    resp = client.get("/api/llm/config", headers=_auth_headers())
    assert resp.status_code == 200
    data = resp.json()

    assert data["ok"] is True
    for key in ("common_keys", "task_routing", "telegram_chat", "market", "youtube", "tasks", "profiles", "model_presets", "hot_reload_capability", "ui_meta"):
        assert key in data
    assert data["needs_restart"] is False
    assert data["hot_reload_capability"]["api"] is True
    assert data["hot_reload_capability"]["worker"] is True
    assert "tier_templates" in data["model_presets"]
    assert "selfcheck" in data["tasks"]
    assert any(
        item.get("id") == "doubao-seed-2-0-pro-260215"
        for item in (data.get("model_presets", {}).get("ark_recommended", []) or [])
    )
    tier_templates = data.get("model_presets", {}).get("tier_templates", {})
    for tier_id in ("cheap_ark", "balanced_ark", "premium_ark"):
        assert tier_id in tier_templates


def test_llm_config_post_routing_returns_hot_apply_metadata(monkeypatch):
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)

    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr("dotenv.set_key", lambda path, key, value, *args, **kwargs: calls.append((str(path), key, str(value))))
    monkeypatch.setattr(
        views,
        "apply_llm_config_in_api_process",
        lambda: SimpleNamespace(worker_heartbeat_seconds=15, llm_hot_reload_signal_file="data/test_signal.json"),
    )
    monkeypatch.setattr(views, "write_llm_reload_signal", lambda *args, **kwargs: "rev-test-001")

    resp = client.post(
        "/api/llm/config",
        json={"task": "routing", "apply_now": True, "routing": {"telegram_chat": "general", "market": "market"}},
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["ok"] is True
    assert data["applied"]["api_reloaded"] is True
    assert data["applied"]["worker_signal_sent"] is True
    assert data["applied"]["worker_expected_apply_within_seconds"] == 15
    assert data["applied"]["signal_revision"] == "rev-test-001"
    assert any(key == "LLM_TASK_ROUTING_JSON" for _, key, _ in calls)


def test_llm_config_post_common_keys_supports_ark(monkeypatch):
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)

    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr("dotenv.set_key", lambda path, key, value, *args, **kwargs: calls.append((str(path), key, str(value))))
    monkeypatch.setattr(
        views,
        "apply_llm_config_in_api_process",
        lambda: SimpleNamespace(worker_heartbeat_seconds=12, llm_hot_reload_signal_file="data/test_signal.json"),
    )
    monkeypatch.setattr(views, "write_llm_reload_signal", lambda *args, **kwargs: "rev-test-ark-001")

    resp = client.post(
        "/api/llm/config",
        json={
            "task": "common_keys",
            "apply_now": True,
            "keys": {"ark": "ark-demo-key", "openai": "openai-demo-key"},
        },
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["ok"] is True
    assert data["applied"]["api_reloaded"] is True
    assert data["applied"]["worker_signal_sent"] is True
    assert data["applied"]["worker_expected_apply_within_seconds"] == 12
    assert data["applied"]["signal_revision"] == "rev-test-ark-001"
    assert any(key == "ARK_API_KEY" and value == "ark-demo-key" for _, key, value in calls)


def test_llm_config_post_common_keys_supports_nvidia_nim(monkeypatch):
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)

    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr("dotenv.set_key", lambda path, key, value, *args, **kwargs: calls.append((str(path), key, str(value))))
    monkeypatch.setattr(
        views,
        "apply_llm_config_in_api_process",
        lambda: SimpleNamespace(worker_heartbeat_seconds=12, llm_hot_reload_signal_file="data/test_signal.json"),
    )
    monkeypatch.setattr(views, "write_llm_reload_signal", lambda *args, **kwargs: "rev-test-nim-001")

    resp = client.post(
        "/api/llm/config",
        json={
            "task": "common_keys",
            "apply_now": True,
            "keys": {"nvidia_nim": "nim-demo-key"},
        },
        headers=_auth_headers(),
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["ok"] is True
    assert data["applied"]["api_reloaded"] is True
    assert data["applied"]["worker_signal_sent"] is True
    assert data["applied"]["worker_expected_apply_within_seconds"] == 12
    assert data["applied"]["signal_revision"] == "rev-test-nim-001"
    assert any(key == "NVIDIA_NIM_API_KEY" and value == "nim-demo-key" for _, key, value in calls)


def test_models_api_includes_doubao_2():
    client = TestClient(app)
    resp = client.get("/api/models")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data.get("items"), list)
    doubao = next((x for x in data["items"] if x.get("id") == "doubao-seed-2-0-pro-260215"), None)
    assert doubao is not None
    assert doubao.get("label") == "豆包2.0"


def test_llm_profiles_auto_heal_missing_model(monkeypatch):
    """Settings.llm_profiles should auto-fill missing 'model' with provider default."""
    import app.config as config

    bad_json = '{"general": {"provider": "deepseek"}}'
    monkeypatch.setenv("LLM_PROFILES_JSON", bad_json)
    config.get_settings.cache_clear()
    s = config.get_settings()
    profiles = s.llm_profiles
    assert "general" in profiles
    assert profiles["general"].model != ""
    assert profiles["general"].provider == "deepseek"


def test_llm_config_get_returns_model_catalog_and_tiers(monkeypatch):
    """GET /api/llm/config should include model_catalog (list) and model_tiers (dict with tiers)."""
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)
    resp = client.get("/api/llm/config", headers=_auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True

    # model_catalog is a list of items with 'id'
    catalog = data.get("model_catalog")
    assert isinstance(catalog, list)
    assert len(catalog) > 0
    assert all("id" in item for item in catalog)

    # model_tiers has at least premium/balanced/cheap keys
    tiers = data.get("model_tiers")
    assert isinstance(tiers, dict)
    for tier_key in ("premium", "balanced", "cheap"):
        assert tier_key in tiers
        assert isinstance(tiers[tier_key], list)

    # at least one tier should be non-empty
    assert any(len(tiers[k]) > 0 for k in ("premium", "balanced", "cheap"))


def test_llm_config_get_returns_warnings_for_bad_profile():
    """_collect_profile_auto_heal_warnings should return warnings when a profile has no model."""
    from app.web.views import _collect_profile_auto_heal_warnings

    bad_json = '{"general": {"provider": "deepseek"}, "market": {}}'
    warnings = _collect_profile_auto_heal_warnings(bad_json)
    assert isinstance(warnings, list)
    assert len(warnings) > 0
    assert any("general" in w for w in warnings)
    assert any("market" in w for w in warnings)

def test_llm_hot_reload_workers_alias_endpoint(monkeypatch):
    _ensure_admin_token(monkeypatch)
    client = TestClient(app)

    monkeypatch.setenv("LLM_HOT_RELOAD_USE_REDIS", "true")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    config.get_settings.cache_clear()

    monkeypatch.setattr(views, "refresh_llm_env_vars_from_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        views,
        "read_llm_reload_acks_redis",
        lambda _url: {"worker-core-1": {"revision": "r1", "status": "ok"}},
    )

    resp = client.get("/api/llm/status.hot_reload.workers", headers=_auth_headers())
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["mode"] == "redis"
    assert data["count"] == 1
    assert "worker-core-1" in data["workers"]
