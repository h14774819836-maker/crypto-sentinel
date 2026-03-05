from __future__ import annotations

import asyncio
from urllib.parse import parse_qsl, urlencode

import httpx

from app.config import Settings
from app.providers.binance_provider import BinanceProvider


def test_signed_get_adds_signature_and_headers():
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = dict(request.headers)
        seen["query_items"] = parse_qsl(request.url.query.decode("utf-8"), keep_blank_values=True)
        return httpx.Response(200, json=[{"ok": True}])

    settings = Settings(
        _env_file=None,
        binance_api_key="test-key",
        binance_api_secret="test-secret",
        binance_futures_url="https://fapi.example.com",
    )
    provider = BinanceProvider(settings)
    transport = httpx.MockTransport(handler)
    async def _run() -> list[dict[str, object]]:
        async with httpx.AsyncClient(transport=transport) as client:
            return await provider.get_futures_positions(client=client, symbol="BTCUSDT")

    rows = asyncio.run(_run())

    assert rows == [{"ok": True}]
    headers = seen["headers"]
    assert isinstance(headers, dict)
    assert headers.get("x-mbx-apikey") == "test-key"

    query_items = seen["query_items"]
    assert isinstance(query_items, list)
    query_dict = {k: v for k, v in query_items}
    assert query_dict["symbol"] == "BTCUSDT"
    assert "timestamp" in query_dict
    assert query_dict["recvWindow"] == str(settings.binance_recv_window)
    assert "signature" in query_dict

    raw_items = [(k, v) for k, v in query_items if k != "signature"]
    expected_query = urlencode(sorted(raw_items))
    expected_signature = provider._sign(settings.binance_api_secret, expected_query)
    assert query_dict["signature"] == expected_signature


def test_signed_get_requires_credentials():
    settings = Settings(_env_file=None, binance_api_key="", binance_api_secret="")
    provider = BinanceProvider(settings)
    try:
        provider._ensure_api_credentials()
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_signed_get_retries_after_1021_with_time_sync():
    call_log: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        call_log.append(path)
        if path == "/fapi/v1/time":
            return httpx.Response(200, json={"serverTime": 2_000_000})
        if call_log.count("/fapi/v2/balance") == 1:
            return httpx.Response(400, json={"code": -1021, "msg": "Timestamp for this request was outside of the recvWindow."})
        return httpx.Response(200, json=[{"asset": "USDT", "availableBalance": "123.4"}])

    settings = Settings(
        _env_file=None,
        binance_api_key="test-key",
        binance_api_secret="test-secret",
        binance_futures_url="https://fapi.example.com",
    )
    provider = BinanceProvider(settings)
    transport = httpx.MockTransport(handler)

    async def _run() -> list[dict[str, object]]:
        async with httpx.AsyncClient(transport=transport) as client:
            return await provider.get_futures_balance(client=client)

    rows = asyncio.run(_run())
    assert rows and rows[0]["asset"] == "USDT"
    assert call_log.count("/fapi/v2/balance") == 2
    assert "/fapi/v1/time" in call_log
