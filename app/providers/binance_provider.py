from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from app.config import Settings
from app.logging import logger
from app.providers.exchange_base import Candle, CandleCallback, ExchangeProvider, FundingRateData, PriceCallback


def floor_to_minute(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


# Binance interval string -> milliseconds
_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


class BinanceProvider(ExchangeProvider):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.rest_base = settings.binance_rest_url.rstrip("/")
        self.ws_base = settings.binance_ws_url.rstrip("/")
        self.futures_base = settings.binance_futures_url.rstrip("/")
        self._http_timeout = 15.0
        self._time_offset_ms = 0

    @staticmethod
    def _ts_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _sign(secret: str, query: str) -> str:
        return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

    def _ensure_api_credentials(self) -> tuple[str, str]:
        api_key = (self.settings.binance_api_key or "").strip()
        api_secret = (self.settings.binance_api_secret or "").strip()
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY/BINANCE_API_SECRET are required for USER_DATA endpoints")
        return api_key, api_secret

    def _api_key_header(self) -> dict[str, str]:
        api_key = (self.settings.binance_api_key or "").strip()
        if not api_key:
            raise ValueError("BINANCE_API_KEY is required for private endpoints")
        return {"X-MBX-APIKEY": api_key}

    async def _sync_server_time(self, base_url: str, *, client: httpx.AsyncClient | None = None) -> None:
        path = "/fapi/v1/time" if "/fapi" in base_url or "fstream" in base_url else "/api/v3/time"
        url = f"{base_url}{path}"
        if client is not None:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        else:
            async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as own_client:
                resp = await own_client.get(url)
                resp.raise_for_status()
                data = resp.json()
        server_time = int((data or {}).get("serverTime") or 0)
        if server_time > 0:
            self._time_offset_ms = server_time - self._ts_ms()

    @staticmethod
    def _parse_binance_error(response: httpx.Response | None) -> tuple[int | None, str]:
        if response is None:
            return None, ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                return int(payload.get("code")) if payload.get("code") is not None else None, str(payload.get("msg") or "")
        except Exception:
            pass
        return None, response.text if response is not None else ""

    @staticmethod
    def _is_retryable_http_status(status_code: int) -> bool:
        return status_code in {418, 429, 500, 502, 503, 504}

    async def _signed_request(
        self,
        method: str,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Any:
        api_key, api_secret = self._ensure_api_credentials()
        url = f"{base_url}{path}"
        max_attempts = 4
        attempt = 0
        need_time_sync = self._time_offset_ms == 0

        while True:
            if need_time_sync:
                try:
                    await self._sync_server_time(base_url, client=client)
                except Exception as exc:
                    logger.warning("binance time sync failed before request: %s", exc)
                need_time_sync = False

            request_params = dict(params or {})
            request_params.setdefault("timestamp", self._ts_ms() + self._time_offset_ms)
            request_params.setdefault("recvWindow", self.settings.binance_recv_window)
            canonical_items = sorted(request_params.items())
            query = urlencode(canonical_items)
            request_params = dict(canonical_items)
            request_params["signature"] = self._sign(api_secret, query)
            headers = {"X-MBX-APIKEY": api_key}

            try:
                if client is not None:
                    resp = await client.request(method.upper(), url, params=request_params, headers=headers)
                else:
                    async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as own_client:
                        resp = await own_client.request(method.upper(), url, params=request_params, headers=headers)
                resp.raise_for_status()
                return resp.json() if resp.text else {}
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                code, msg = self._parse_binance_error(exc.response)
                # Binance -1021: timestamp drift. Re-sync and retry once quickly.
                if code == -1021 and attempt < max_attempts - 1:
                    await self._sync_server_time(base_url, client=client)
                    attempt += 1
                    continue
                if self._is_retryable_http_status(status) and attempt < max_attempts - 1:
                    backoff = min(8.0, (2 ** attempt) * 0.5)
                    logger.warning("binance private request retry status=%s code=%s msg=%s", status, code, msg)
                    await asyncio.sleep(backoff)
                    attempt += 1
                    continue
                raise
            except httpx.RequestError:
                if attempt < max_attempts - 1:
                    backoff = min(8.0, (2 ** attempt) * 0.5)
                    await asyncio.sleep(backoff)
                    attempt += 1
                    continue
                raise

    async def _signed_get(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Any:
        return await self._signed_request("GET", base_url, path, params=params, client=client)

    async def _signed_post(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Any:
        return await self._signed_request("POST", base_url, path, params=params, client=client)

    async def _signed_put(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Any:
        return await self._signed_request("PUT", base_url, path, params=params, client=client)

    @staticmethod
    def parse_ws_kline(payload: dict[str, Any]) -> Candle | None:
        kline = payload.get("k") or {}
        if not kline.get("x"):
            return None
        symbol = (payload.get("s") or kline.get("s") or "").upper()
        open_time = datetime.fromtimestamp(int(kline["t"]) / 1000, tz=timezone.utc)
        return Candle(
            symbol=symbol,
            timeframe="1m",
            ts=floor_to_minute(open_time),
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            source="binance_ws",
        )

    @staticmethod
    def _parse_miniticker(payload: dict[str, Any]) -> tuple[str, float, datetime] | None:
        symbol = payload.get("s")
        close = payload.get("c")
        event_ts = payload.get("E")
        if not symbol or close is None or event_ts is None:
            return None
        return symbol.upper(), float(close), datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc)

    @staticmethod
    def _interval_ms(interval: str) -> int:
        ms = _INTERVAL_MS.get(interval)
        if ms is None:
            raise ValueError(f"Unsupported interval: {interval}")
        return ms

    def _build_stream_url(self, symbols: list[str]) -> str:
        streams = [f"{symbol.lower()}@kline_1m" for symbol in symbols]
        if self.settings.enable_miniticker or getattr(self.settings, "anomaly_tick_flash_enabled", False):
            streams.extend(f"{symbol.lower()}@miniTicker" for symbol in symbols)
        stream_path = "/".join(streams)
        return f"{self.ws_base}?streams={stream_path}"

    async def consume_kline_stream(self, symbols: list[str], on_candle: CandleCallback, on_price: PriceCallback | None = None) -> None:
        backoff = 1
        while True:
            stream_url = self._build_stream_url(symbols)
            try:
                logger.info("Connecting Binance stream: %s", stream_url)
                async with websockets.connect(stream_url, ping_interval=20, ping_timeout=20, max_queue=1024) as ws:
                    backoff = 1
                    async for raw_message in ws:
                        payload = json.loads(raw_message)
                        event = payload.get("data", {})
                        event_type = event.get("e")
                        if event_type == "kline":
                            candle = self.parse_ws_kline(event)
                            if candle:
                                await on_candle(candle)
                        elif event_type == "24hrMiniTicker" and on_price:
                            parsed = self._parse_miniticker(event)
                            if parsed:
                                symbol, price, ts = parsed
                                await on_price(symbol, price, ts)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("WebSocket disconnected, retry in %ss: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ---- Generalized REST kline fetch (supports any interval) ----

    async def fetch_klines(self, symbol: str, interval: str, start_ts: datetime, end_ts: datetime) -> list[Candle]:
        if end_ts < start_ts:
            return []

        interval_ms = self._interval_ms(interval)
        start_ms = int(start_ts.timestamp() * 1000)
        end_ms = int(end_ts.timestamp() * 1000)
        current = start_ms
        candles: list[Candle] = []

        async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as client:
            while current <= end_ms:
                params = {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "startTime": current,
                    "endTime": end_ms + interval_ms - 1,
                    "limit": 1000,
                }
                response = await client.get(f"{self.rest_base}/api/v3/klines", params=params)
                response.raise_for_status()
                rows = response.json()
                if not rows:
                    break

                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                for row in rows:
                    open_ms = int(row[0])
                    close_ms = int(row[6])
                    if close_ms >= now_ms:
                        continue
                    candle_ts = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
                    if candle_ts < start_ts or candle_ts > end_ts:
                        continue
                    candles.append(
                        Candle(
                            symbol=symbol.upper(),
                            timeframe=interval,
                            ts=floor_to_minute(candle_ts) if interval == "1m" else candle_ts,
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5]),
                            source="binance_rest",
                        )
                    )

                last_open_ms = int(rows[-1][0])
                next_open = last_open_ms + interval_ms
                if next_open <= current:
                    break
                current = next_open
                await asyncio.sleep(0.05)

        candles.sort(key=lambda c: c.ts)
        return candles

    async def fetch_1m_klines(self, symbol: str, start_ts: datetime, end_ts: datetime) -> list[Candle]:
        return await self.fetch_klines(symbol, "1m", start_ts, end_ts)

    async def backfill_recent_days(self, symbol: str, days: int) -> list[Candle]:
        now_floor = floor_to_minute(datetime.now(timezone.utc))
        end_ts = now_floor - timedelta(minutes=1)
        start_ts = end_ts - timedelta(days=days) + timedelta(minutes=1)
        return await self.fetch_1m_klines(symbol, start_ts, end_ts)

    async def fill_missing_since(self, symbol: str, last_ts: datetime | None) -> list[Candle]:
        now_floor = floor_to_minute(datetime.now(timezone.utc))
        end_ts = now_floor - timedelta(minutes=1)
        if last_ts is None:
            return []
        start_ts = floor_to_minute(last_ts) + timedelta(minutes=1)
        if start_ts > end_ts:
            return []
        return await self.fetch_1m_klines(symbol, start_ts, end_ts)

    # ---- Binance Futures: Premium Index (mark price + funding rate) ----

    async def fetch_premium_index(self, symbol: str) -> FundingRateData | None:
        """GET /fapi/v1/premiumIndex — returns mark price, last funding rate, etc."""
        url = f"{self.futures_base}/fapi/v1/premiumIndex"
        try:
            async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as client:
                resp = await client.get(url, params={"symbol": symbol.upper()})
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            logger.warning("fetch_premium_index failed for %s: %s", symbol, exc)
            return None

        nft = data.get("nextFundingTime")
        next_funding_time = None
        if nft and int(nft) > 0:
            next_funding_time = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)

        return FundingRateData(
            symbol=symbol.upper(),
            mark_price=_safe_float(data.get("markPrice")),
            index_price=_safe_float(data.get("indexPrice")),
            last_funding_rate=_safe_float(data.get("lastFundingRate")),
            next_funding_time=next_funding_time,
            interest_rate=_safe_float(data.get("interestRate")),
        )

    async def fetch_open_interest(self, symbol: str) -> float | None:
        """GET /fapi/v1/openInterest — returns current open interest."""
        url = f"{self.futures_base}/fapi/v1/openInterest"
        try:
            async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as client:
                resp = await client.get(url, params={"symbol": symbol.upper()})
                resp.raise_for_status()
                data = resp.json()
            return _safe_float(data.get("openInterest"))
        except Exception as exc:
            logger.warning("fetch_open_interest failed for %s: %s", symbol, exc)
            return None

    async def get_margin_account(self, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
        return await self._signed_get(
            self.rest_base,
            "/sapi/v1/margin/account",
            {},
            client=client,
        )

    async def get_margin_trade_coeff(self, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
        return await self._signed_get(
            self.rest_base,
            "/sapi/v1/margin/tradeCoeff",
            {},
            client=client,
        )

    async def get_futures_account(self, client: httpx.AsyncClient | None = None) -> dict[str, Any]:
        return await self._signed_get(
            self.futures_base,
            "/fapi/v2/account",
            {},
            client=client,
        )

    async def get_futures_balance(self, client: httpx.AsyncClient | None = None) -> list[dict[str, Any]]:
        data = await self._signed_get(
            self.futures_base,
            "/fapi/v2/balance",
            {},
            client=client,
        )
        return data if isinstance(data, list) else []

    async def get_futures_positions(
        self,
        client: httpx.AsyncClient | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else {}
        data = await self._signed_get(
            self.futures_base,
            "/fapi/v3/positionRisk",
            params,
            client=client,
        )
        return data if isinstance(data, list) else []

    async def create_futures_listen_key(self, client: httpx.AsyncClient | None = None) -> str:
        headers = self._api_key_header()
        url = f"{self.futures_base}/fapi/v1/listenKey"
        response: dict[str, Any] = {}
        for attempt in range(4):
            try:
                if client is not None:
                    resp = await client.post(url, headers=headers)
                else:
                    async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as own_client:
                        resp = await own_client.post(url, headers=headers)
                resp.raise_for_status()
                response = resp.json()
                break
            except httpx.HTTPStatusError as exc:
                if attempt >= 3 or not self._is_retryable_http_status(exc.response.status_code):
                    raise
                await asyncio.sleep(min(8.0, (2 ** attempt) * 0.5))
        listen_key = str((response or {}).get("listenKey") or "").strip()
        if not listen_key:
            raise ValueError("Failed to create futures listenKey")
        return listen_key

    async def keepalive_futures_listen_key(self, listen_key: str, client: httpx.AsyncClient | None = None) -> None:
        if not listen_key:
            return
        headers = self._api_key_header()
        url = f"{self.futures_base}/fapi/v1/listenKey"
        for attempt in range(4):
            try:
                if client is not None:
                    resp = await client.put(url, params={"listenKey": listen_key}, headers=headers)
                else:
                    async with httpx.AsyncClient(timeout=self._http_timeout, trust_env=False) as own_client:
                        resp = await own_client.put(url, params={"listenKey": listen_key}, headers=headers)
                resp.raise_for_status()
                return
            except httpx.HTTPStatusError as exc:
                if attempt >= 3 or not self._is_retryable_http_status(exc.response.status_code):
                    raise
                await asyncio.sleep(min(8.0, (2 ** attempt) * 0.5))

    async def consume_futures_user_stream(
        self,
        listen_key: str,
        on_event,
    ) -> None:
        if not listen_key:
            raise ValueError("listen_key is required")
        ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, max_queue=1024) as ws:
            async for raw_message in ws:
                payload = json.loads(raw_message)
                await on_event(payload)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
