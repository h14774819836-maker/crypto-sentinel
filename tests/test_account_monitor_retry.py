from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from app.scheduler._jobs_impl import account_monitor_job
from app.scheduler.runtime import WorkerRuntime


def _runtime() -> WorkerRuntime:
    settings = SimpleNamespace(
        account_monitor_enabled=True,
        binance_api_key="k",
        binance_api_secret="s",
    )
    provider = SimpleNamespace(
        _parse_binance_error=lambda _resp: (None, ""),
        _is_retryable_http_status=lambda _status: False,
    )
    return WorkerRuntime(
        settings=settings,  # type: ignore[arg-type]
        session_factory=SimpleNamespace(),  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        telegram=SimpleNamespace(),  # type: ignore[arg-type]
        started_at=datetime.now(timezone.utc),
        version="test",
    )


def test_account_monitor_transient_request_error_sets_next_retry(monkeypatch):
    runtime = _runtime()

    async def _raise(_runtime):
        raise httpx.RequestError("network down", request=httpx.Request("GET", "https://example.com"))

    monkeypatch.setattr("app.scheduler._jobs_impl._collect_and_store_account_snapshots", _raise)

    result = __import__("asyncio").run(account_monitor_job(runtime))
    assert result["rows_written"] == 0
    assert runtime.account_monitor_failed is False
    assert runtime.account_monitor_failure_count == 1
    assert runtime.account_monitor_next_retry_at is not None


def test_account_monitor_auth_error_disables_retries(monkeypatch):
    runtime = _runtime()

    async def _raise(_runtime):
        req = httpx.Request("GET", "https://example.com")
        resp = httpx.Response(401, json={"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."}, request=req)
        raise httpx.HTTPStatusError("unauthorized", request=req, response=resp)

    monkeypatch.setattr("app.scheduler._jobs_impl._collect_and_store_account_snapshots", _raise)
    monkeypatch.setattr(runtime.provider, "_parse_binance_error", lambda r: (-2015, "Invalid API-key, IP, or permissions for action."))

    result = __import__("asyncio").run(account_monitor_job(runtime))
    assert result["rows_written"] == 0
    assert runtime.account_monitor_failed is True
