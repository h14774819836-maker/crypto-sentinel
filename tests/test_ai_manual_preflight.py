from __future__ import annotations

import asyncio
from types import SimpleNamespace

from fastapi import HTTPException

from app.web.routers import api_ai


class _DummySessionCtx:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


def _dummy_session_factory():
    return _DummySessionCtx()


def test_refresh_preflight_stale_guarded_skips_when_fresh(monkeypatch):
    monkeypatch.setattr(api_ai, "SessionLocal", _dummy_session_factory)
    monkeypatch.setattr(
        api_ai,
        "settings",
        SimpleNamespace(
            ai_manual_preflight_mode="stale_guarded",
            ai_manual_preflight_stale_seconds=300,
            ai_manual_preflight_lock_seconds=120,
            watchlist_symbols=["BTCUSDT"],
            redis_url="redis://localhost:6379/0",
        ),
    )
    monkeypatch.setattr(
        api_ai,
        "_assess_market_data_freshness",
        lambda _db, stale_seconds: {
            "fresh": True,
            "missing_symbols": [],
            "stale_symbols": [],
            "latest_data_ts": None,
            "stale_seconds": stale_seconds,
        },
    )

    result = asyncio.run(api_ai._refresh_market_data_before_ai_analysis())
    assert result["ok"] is True
    assert result["preflight"] == "skipped_fresh"


def test_refresh_preflight_stale_guarded_skips_on_lock_held(monkeypatch):
    monkeypatch.setattr(api_ai, "SessionLocal", _dummy_session_factory)
    monkeypatch.setattr(
        api_ai,
        "settings",
        SimpleNamespace(
            ai_manual_preflight_mode="stale_guarded",
            ai_manual_preflight_stale_seconds=300,
            ai_manual_preflight_lock_seconds=120,
            watchlist_symbols=["BTCUSDT"],
            redis_url="redis://localhost:6379/0",
        ),
    )
    monkeypatch.setattr(
        api_ai,
        "_assess_market_data_freshness",
        lambda _db, stale_seconds: {
            "fresh": False,
            "missing_symbols": [],
            "stale_symbols": ["BTCUSDT"],
            "latest_data_ts": None,
            "stale_seconds": stale_seconds,
        },
    )

    async def _fake_lock(*_args, **_kwargs):
        return False, None, None

    monkeypatch.setattr(api_ai, "_try_acquire_manual_preflight_lock", _fake_lock)
    result = asyncio.run(api_ai._refresh_market_data_before_ai_analysis())
    assert result["ok"] is True
    assert result["preflight"] == "skipped_lock_held"


def test_refresh_preflight_returns_core_data_not_ready(monkeypatch):
    monkeypatch.setattr(api_ai, "SessionLocal", _dummy_session_factory)
    monkeypatch.setattr(
        api_ai,
        "settings",
        SimpleNamespace(
            ai_manual_preflight_mode="stale_guarded",
            ai_manual_preflight_stale_seconds=300,
            ai_manual_preflight_lock_seconds=120,
            watchlist_symbols=["BTCUSDT"],
            redis_url="redis://localhost:6379/0",
        ),
    )

    calls = {"n": 0}

    def _freshness(_db, stale_seconds):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "fresh": False,
                "missing_symbols": [],
                "stale_symbols": ["BTCUSDT"],
                "latest_data_ts": None,
                "stale_seconds": stale_seconds,
            }
        return {
            "fresh": False,
            "missing_symbols": ["BTCUSDT"],
            "stale_symbols": [],
            "latest_data_ts": None,
            "stale_seconds": stale_seconds,
        }

    monkeypatch.setattr(api_ai, "_assess_market_data_freshness", _freshness)

    async def _fake_lock(*_args, **_kwargs):
        return True, "tok-1", None

    released = {"called": False}

    async def _fake_release(*_args, **_kwargs):
        released["called"] = True

    monkeypatch.setattr(api_ai, "_try_acquire_manual_preflight_lock", _fake_lock)
    monkeypatch.setattr(api_ai, "_release_manual_preflight_lock", _fake_release)

    result = asyncio.run(api_ai._refresh_market_data_before_ai_analysis())
    assert result["ok"] is False
    assert result["code"] == "core_data_not_ready"
    assert released["called"] is True


def test_refresh_report_maps_core_data_not_ready_to_503():
    exc = api_ai._refresh_report_to_http_exception({"code": "core_data_not_ready", "error": "core_data_not_ready"})
    assert isinstance(exc, HTTPException)
    assert exc.status_code == 503
