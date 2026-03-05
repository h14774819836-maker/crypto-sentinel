"""Tests for admin authentication enforcement and ASR concurrency guard."""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
import app.config as config
import app.web.views as views


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_ADMIN_TOKEN = "test-secret-token-12345"

# All management endpoints that require admin auth.
ADMIN_ENDPOINTS: list[tuple[str, str]] = [
    ("GET", "/api/llm/config"),
    ("POST", "/api/llm/config"),
    ("POST", "/api/llm/selfcheck"),
    ("POST", "/api/youtube/sync"),
    ("GET", "/api/youtube/asr/model"),
    ("POST", "/api/youtube/asr/model"),
    ("POST", "/api/youtube/asr/test-video-123"),
    ("POST", "/api/youtube/channels"),
    ("DELETE", "/api/youtube/channels/test-channel"),
    ("POST", "/api/youtube/analyze/test-video-123"),
    ("POST", "/api/ai-analyze"),
    ("GET", "/api/ai-analyze/stream"),
]

# Public endpoints that should remain accessible without token.
PUBLIC_ENDPOINTS: list[tuple[str, str]] = [
    ("GET", "/api/health"),
    ("GET", "/api/models"),
]


def _set_admin_token(monkeypatch, token: str = TEST_ADMIN_TOKEN):
    """Inject ADMIN_TOKEN into settings for tests."""
    monkeypatch.setenv("ADMIN_TOKEN", token)
    config.get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Auth enforcement tests
# ---------------------------------------------------------------------------

class TestAdminAuth:
    """Verify management endpoints are protected by Bearer token auth."""

    def test_admin_endpoints_return_403_without_token(self, monkeypatch):
        _set_admin_token(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)

        for method, path in ADMIN_ENDPOINTS:
            resp = getattr(client, method.lower())(path)
            assert resp.status_code == 403, (
                f"{method} {path} should return 403 without token, got {resp.status_code}"
            )

    def test_admin_endpoints_return_403_with_wrong_token(self, monkeypatch):
        _set_admin_token(monkeypatch)
        client = TestClient(app, raise_server_exceptions=False)
        headers = {"Authorization": "Bearer wrong-token"}

        for method, path in ADMIN_ENDPOINTS:
            resp = getattr(client, method.lower())(path, headers=headers)
            assert resp.status_code == 403, (
                f"{method} {path} should reject wrong token, got {resp.status_code}"
            )

    def test_llm_config_get_accepts_valid_token(self, monkeypatch):
        """With correct token, GET /api/llm/config should return 200."""
        _set_admin_token(monkeypatch)
        client = TestClient(app)
        headers = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}

        resp = client.get("/api/llm/config", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True

    def test_admin_endpoints_locked_when_token_not_configured(self, monkeypatch):
        """If ADMIN_TOKEN is empty, all admin endpoints should return 403."""
        monkeypatch.setenv("ADMIN_TOKEN", "")
        config.get_settings.cache_clear()
        client = TestClient(app, raise_server_exceptions=False)

        for method, path in ADMIN_ENDPOINTS:
            resp = getattr(client, method.lower())(path)
            assert resp.status_code == 403, (
                f"{method} {path} should be locked when ADMIN_TOKEN is empty, got {resp.status_code}"
            )

    def test_public_endpoints_remain_accessible(self):
        """Public endpoints should work without any auth token."""
        client = TestClient(app)

        for method, path in PUBLIC_ENDPOINTS:
            resp = getattr(client, method.lower())(path)
            assert resp.status_code == 200, (
                f"{method} {path} should remain public, got {resp.status_code}"
            )


# ---------------------------------------------------------------------------
# ASR semaphore test
# ---------------------------------------------------------------------------

class TestASRSemaphore:
    """Verify ASR concurrent limiter returns 429 when slots are exhausted."""

    def test_asr_semaphore_rejects_when_full(self, monkeypatch):
        from app.web import shared

        _set_admin_token(monkeypatch)
        monkeypatch.setattr(shared.settings, "asr_max_concurrent", 1)
        monkeypatch.setattr(shared.settings, "asr_enabled", True)

        # Reset the global semaphore so it picks up max_concurrent=1
        monkeypatch.setattr(shared, "_asr_semaphore", None)

        client = TestClient(app, raise_server_exceptions=False)
        headers = {"Authorization": f"Bearer {TEST_ADMIN_TOKEN}"}

        # Pre-acquire the single semaphore slot
        sem = shared.get_asr_semaphore()
        loop = asyncio.new_event_loop()
        loop.run_until_complete(sem.acquire())

        try:
            resp = client.get("/api/youtube/asr/model", headers=headers)
            # asr model status doesn't use semaphore, just needs auth
            # test the POST endpoint for semaphore
            resp = client.post("/api/youtube/asr/model", headers=headers)
            assert resp.status_code == 429, (
                f"POST /api/youtube/asr/model should return 429 when semaphore full, got {resp.status_code}"
            )
        finally:
            sem.release()
            monkeypatch.setattr(shared, "_asr_semaphore", None)
            loop.close()
