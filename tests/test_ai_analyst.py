from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from app.ai.analyst import MarketAnalyst, _extract_json
from app.ai.provider import LLMCapabilities
from app.config import LLMConfig


class _DummyProvider:
    capabilities = LLMCapabilities(
        supports_json=True,
        supports_tools=False,
        supports_reasoning=False,
        supports_stream=False,
    )


class _RetryProvider:
    capabilities = LLMCapabilities(
        supports_json=True,
        supports_tools=False,
        supports_reasoning=True,
        supports_stream=False,
    )

    def __init__(self, responses: list[dict]):
        self._responses = responses
        self.calls: list[dict] = []

    async def generate_response(self, **kwargs):
        self.calls.append(kwargs)
        if self._responses:
            return self._responses.pop(0)
        return {"content": "", "model": kwargs.get("model_override", "unknown")}


def _analyst() -> MarketAnalyst:
    cfg = LLMConfig(
        enabled=True,
        provider="deepseek",
        api_key="x",
        base_url="https://api.deepseek.com",
        model="deepseek-chat",
        use_reasoning="false",
        max_concurrency=1,
        max_retries=1,
    )
    return MarketAnalyst(SimpleNamespace(), _DummyProvider(), cfg)


def _snapshots(atr: float = 1.0):
    return {
        "1m": {
            "latest": {"atr_14": atr, "close": 100.0, "ts": "2026-02-24T12:00:00+00:00"},
            "history": [],
        }
    }


def test_parse_old_schema_compatible_and_hold_prices_cleared():
    analyst = _analyst()
    content = json.dumps(
        {
            "market_regime": "ranging",
            "signal": {
                "symbol": "BTCUSDT",
                "direction": "HOLD",
                "entry_price": 100,
                "take_profit": 110,
                "stop_loss": 90,
                "confidence": 60,
                "reasoning": "观望为主",
            },
        },
        ensure_ascii=False,
    )
    signals = analyst._parse_response(content, symbol="BTCUSDT", snapshots=_snapshots(), context={"data_quality": {"overall": "GOOD"}})
    assert len(signals) == 1
    sig = signals[0]
    assert sig.direction == "HOLD"
    assert sig.entry_price is None and sig.take_profit is None and sig.stop_loss is None
    assert isinstance(sig.analysis_json, dict)
    assert sig.analysis_json["signal"]["direction"] == "HOLD"


def test_validate_and_sanitize_downgrades_low_rr_and_marks_validation():
    analyst = _analyst()
    content = json.dumps(
        {
            "market_regime": "trending_up",
            "signal": {
                "symbol": "BTCUSDT",
                "direction": "LONG",
                "entry_price": 100.0,
                "take_profit": 101.0,
                "stop_loss": 99.0,
                "confidence": 82,
                "reasoning": "测试低RR",
            },
            "evidence": [{"timeframe": "1m", "point": "价格站上短均线", "metrics": {"close": 100.0}}],
        },
        ensure_ascii=False,
    )
    signals = analyst._parse_response(content, symbol="BTCUSDT", snapshots=_snapshots(atr=1.0), context={"data_quality": {"overall": "GOOD"}})
    sig = signals[0]
    assert sig.direction == "HOLD"
    assert sig.confidence <= 45
    validation = sig.analysis_json.get("validation", {})
    assert validation.get("status") == "downgraded"
    assert "RR" in (validation.get("downgrade_reason") or "") or "风险收益比" in (validation.get("downgrade_reason") or "")


def test_validate_and_sanitize_safe_swaps_tp_sl_for_long():
    analyst = _analyst()
    content = json.dumps(
        {
            "market_regime": "trending_up",
            "signal": {
                "symbol": "BTCUSDT",
                "direction": "LONG",
                "entry_price": 100.0,
                "take_profit": 95.0,
                "stop_loss": 110.0,
                "confidence": 70,
                "reasoning": "tp/sl 反了",
            },
        },
        ensure_ascii=False,
    )
    signals = analyst._parse_response(content, symbol="BTCUSDT", snapshots=_snapshots(atr=2.0), context={"data_quality": {"overall": "GOOD"}})
    sig = signals[0]
    assert sig.direction == "LONG"
    assert sig.take_profit == 110.0
    assert sig.stop_loss == 95.0
    validation = sig.analysis_json.get("validation", {})
    assert "swap_tp_sl_for_long" in (validation.get("auto_fixes") or [])


def test_analyze_retries_once_on_schema_failure_then_succeeds():
    cfg = LLMConfig(
        enabled=True,
        provider="deepseek",
        api_key="x",
        base_url="https://api.deepseek.com",
        model="deepseek-reasoner",
        use_reasoning="auto",
        max_concurrency=1,
        max_retries=1,
    )
    first_bad = {
        "content": json.dumps(
            {
                "market_regime": "trending_down",
                "signal": {
                    "symbol": "BTCUSDT",
                    "direction": "HOLD",
                    "entry_price": None,
                    "take_profit": None,
                    "stop_loss": None,
                    "confidence": 60,
                    "reasoning": "第一次缺 anchors",
                },
                "evidence": [
                    {"timeframe": "1m", "point": "close=100", "metrics": {"close": 100.0}},
                    {"timeframe": "1m", "point": "atr=1", "metrics": {"atr_14": 1.0}},
                ],
            },
            ensure_ascii=False,
        ),
        "model": "deepseek-reasoner",
        "prompt_tokens": 10,
        "completion_tokens": 20,
    }
    second_ok = {
        "content": json.dumps(
            {
                "market_regime": "trending_down",
                "signal": {
                    "symbol": "BTCUSDT",
                    "direction": "HOLD",
                    "entry_price": None,
                    "take_profit": None,
                    "stop_loss": None,
                    "confidence": 62,
                    "reasoning": "重试后成功",
                },
                "evidence": [
                    {"timeframe": "1m", "point": "close=100", "metrics": {"close": 100.0}},
                    {"timeframe": "1m", "point": "atr=1", "metrics": {"atr_14": 1.0}},
                ],
                "anchors": [
                    {"path": "facts.multi_tf_snapshots.1m.latest.close", "value": "100"},
                    {"path": "facts.multi_tf_snapshots.1m.latest.atr_14", "value": "1"},
                ],
            },
            ensure_ascii=False,
        ),
        "model": "deepseek-chat",
        "prompt_tokens": 12,
        "completion_tokens": 18,
    }
    provider = _RetryProvider([first_bad, second_ok])
    analyst = MarketAnalyst(SimpleNamespace(), provider, cfg)

    signals, meta = asyncio.run(
        analyst.analyze(
            "BTCUSDT",
            _snapshots(atr=1.0),
            context={"data_quality": {"overall": "GOOD"}},
        )
    )
    assert len(signals) == 1
    assert signals[0].direction == "HOLD"
    assert meta is not None and meta.get("status") == "ok"
    assert len(provider.calls) == 2
    assert provider.calls[1].get("model_override") == "deepseek-chat"


def test_analyze_returns_failed_hold_when_grounding_fails_after_retry():
    cfg = LLMConfig(
        enabled=True,
        provider="deepseek",
        api_key="x",
        base_url="https://api.deepseek.com",
        model="deepseek-reasoner",
        use_reasoning="auto",
        max_concurrency=1,
        max_retries=1,
    )
    bad_grounding = {
        "content": json.dumps(
            {
                "market_regime": "trending_down",
                "signal": {
                    "symbol": "BTCUSDT",
                    "direction": "HOLD",
                    "entry_price": None,
                    "take_profit": None,
                    "stop_loss": None,
                    "confidence": 65,
                    "reasoning": "坏 grounding",
                },
                "evidence": [
                    {"timeframe": "1m", "point": "close=100", "metrics": {"close": 100.0}},
                    {"timeframe": "1m", "point": "fake=123", "metrics": {"fake_metric": 123.0}},
                ],
                "anchors": [
                    {"path": "facts.multi_tf_snapshots.1m.latest.close", "value": "999"},
                    {"path": "facts.multi_tf_snapshots.1m.latest.atr_14", "value": "999"},
                ],
            },
            ensure_ascii=False,
        ),
        "model": "deepseek-reasoner",
        "prompt_tokens": 10,
        "completion_tokens": 20,
    }
    provider = _RetryProvider([bad_grounding, bad_grounding])
    analyst = MarketAnalyst(SimpleNamespace(), provider, cfg)

    signals, meta = asyncio.run(
        analyst.analyze(
            "BTCUSDT",
            _snapshots(atr=1.0),
            context={"data_quality": {"overall": "GOOD"}},
        )
    )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.direction == "HOLD"
    assert isinstance(sig.analysis_json, dict)
    validation = sig.analysis_json.get("validation", {})
    assert validation.get("status") == "failed"
    assert validation.get("phase") in {"grounding", "schema", "exhausted"}
    assert meta is not None
    assert meta.get("status") == "error"
    assert isinstance(meta.get("failure_events"), list)
    assert len(meta.get("failure_events")) >= 2


def test_extract_json_handles_preface_and_two_json_objects():
    raw = (
        "前置信息\n"
        "{\"a\":1,\"b\":{\"x\":2}}\n"
        "{\"c\":3}\n"
        "--- Analysis Completed ---"
    )
    got = _extract_json(raw)
    assert got == "{\"a\":1,\"b\":{\"x\":2}}"


def test_parse_response_strict_allows_anchor_without_facts_prefix_and_categorical_metric():
    analyst = _analyst()
    content = json.dumps(
        {
            "market_regime": "volatile",
            "signal": {
                "symbol": "BTCUSDT",
                "direction": "HOLD",
                "entry_price": None,
                "take_profit": None,
                "stop_loss": None,
                "confidence": 50,
                "reasoning": "test",
            },
            "evidence": [
                {
                    "timeframe": "4h",
                    "point": "趋势向下",
                    "metrics": {"ema_ribbon_trend": "DOWN", "rsi_14": 34.65593054880884},
                },
                {
                    "timeframe": "15m",
                    "point": "趋势向上",
                    "metrics": {"ema_ribbon_trend": "UP", "rsi_14": 100.0},
                },
            ],
            "anchors": [
                {"path": "multi_tf_snapshots.4h.latest.ema_ribbon_trend", "value": "DOWN"},
                {"path": "multi_tf_snapshots.1m.latest.close", "value": "100"},
            ],
            "levels": {"supports": [99.0], "resistances": [101.0]},
            "risk": {"rr": None, "sl_atr_multiple": None, "invalidations": []},
            "scenarios": {"base": "", "bull": "", "bear": ""},
            "youtube_reflection": {"status": "conflicted", "note": "x"},
            "validation_notes": [],
        },
        ensure_ascii=False,
    )
    snapshots = {
        "1m": {"latest": {"close": 100.0, "ema_ribbon_trend": "MIXED", "atr_14": 1.0}, "history": []},
        "4h": {"latest": {"close": 90.0, "ema_ribbon_trend": "DOWN", "rsi_14": 34.65593054880884}, "history": []},
    }
    signals, failure = analyst._parse_response_strict(
        content,
        symbol="BTCUSDT",
        snapshots=snapshots,
        context={"brief": {}, "data_quality": {"overall": "GOOD"}},
    )
    assert failure is None
    assert len(signals) == 1
