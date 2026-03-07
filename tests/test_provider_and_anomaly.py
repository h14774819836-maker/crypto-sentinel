from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx

from app.config import Settings
from app.providers.binance_provider import BinanceProvider
from app.signals.anomaly import evaluate_anomalies


def test_parse_ws_kline_only_accepts_final():
    not_final = {
        "e": "kline",
        "s": "BTCUSDT",
        "k": {
            "x": False,
            "t": 1700000000000,
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100.5",
            "v": "123",
        },
    }
    assert BinanceProvider.parse_ws_kline(not_final) is None

    final = {
        "e": "kline",
        "s": "BTCUSDT",
        "k": {
            "x": True,
            "t": 1700000000000,
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100.5",
            "v": "123",
        },
    }
    candle = BinanceProvider.parse_ws_kline(final)
    assert candle is not None
    assert candle.symbol == "BTCUSDT"
    assert candle.timeframe == "1m"
    assert candle.close == 100.5


def test_volatility_rule_warmup_and_fallback():
    settings = Settings(
        _env_file=None,
        vol_p75_min_candles=10,
        vol_fallback_min_candles=3,
        vol_fallback_k=1.0,
    )

    metric = SimpleNamespace(
        ts=datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc),
        ret_1m=None,
        volume_zscore=None,
        rolling_vol_20=0.5,
    )

    candles = []
    base_ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    for i in range(25):
        candles.append(
            SimpleNamespace(
                ts=base_ts + timedelta(minutes=i),
                high=100 + i,
                low=90 - i,
                close=95 + i,
            )
        )

    warmup_alerts = evaluate_anomalies(
        symbol="BTCUSDT",
        metric=metric,
        recent_1m_candles=candles,
        vol_history=[0.1, 0.11],
        settings=settings,
    )
    assert all(a.alert_type != "VOLATILITY_SURGE" for a in warmup_alerts)

    fallback_alerts = evaluate_anomalies(
        symbol="BTCUSDT",
        metric=metric,
        recent_1m_candles=candles,
        vol_history=[0.1, 0.11, 0.12, 0.09, 0.1],
        settings=settings,
    )
    assert any(a.alert_type == "VOLATILITY_SURGE" for a in fallback_alerts)


def test_summarize_request_failure_includes_url_status_and_response_text():
    provider = BinanceProvider(Settings(_env_file=None))
    request = httpx.Request("GET", "https://fapi.binance.test/fapi/v1/openInterest?symbol=BTCUSDT")
    response = httpx.Response(
        503,
        request=request,
        text='{"code":-1003,"msg":"Too many requests"}',
    )
    exc = httpx.HTTPStatusError("503 Service Unavailable", request=request, response=response)

    summary = provider._summarize_request_failure(exc, response=response)

    assert summary["url"] == "https://fapi.binance.test/fapi/v1/openInterest?symbol=BTCUSDT"
    assert summary["status"] == 503
    assert summary["code"] == -1003
    assert summary["msg"] == "Too many requests"
    assert summary["exception"] == "HTTPStatusError"
    assert "Too many requests" in summary["response_text"]
