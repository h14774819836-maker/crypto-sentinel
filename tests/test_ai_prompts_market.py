from __future__ import annotations

from datetime import datetime, timezone

from app.ai.prompts import SYSTEM_PROMPT, build_analysis_prompt, build_analysis_prompt_details


def _snapshots():
    return {
        "1m": {
            "latest": {"ts": datetime(2026, 2, 24, 12, 0, tzinfo=timezone.utc), "close": 100.0, "rsi_14": 55.0},
            "history": [{"close": 99.0, "high": 100.0, "low": 98.0, "open": 99.2}, {"close": 100.0, "high": 101.0, "low": 99.0, "open": 99.5}],
        }
    }


def test_market_prompt_contains_sections_and_youtube_untrusted_rule():
    prompt = build_analysis_prompt(
        symbol="BTCUSDT",
        snapshots=_snapshots(),
        context={
            "brief": {"tradeable_gate": {"tradeable": True, "reasons": []}},
            "alerts_digest": {"count_1h": 1, "count_4h": 2, "top_events": [], "dominant_types": [], "alerts_burst": False},
            "youtube_radar": {"available": True, "stale": False, "consensus_bias": "BULL", "confidence": 80},
            "funding_deltas": {"funding_rate": 0.0001},
            "data_quality": {"overall": "GOOD", "notes": []},
            "input_budget_meta": {},
        },
    )

    assert "事实源（Facts Source）" in prompt
    assert "观点源（External Views / YouTube Radar）" in prompt
    assert "untrusted external views" in prompt
    assert "冲突处理规则" in prompt
    assert "输出要求（严格 JSON）" in prompt
    assert "close、rsi_14、atr_14、funding_rate、ret_1m" in prompt
    assert "绝对禁止生成、计算、或篡改输入 JSON 中不存在的数值" in SYSTEM_PROMPT
    assert "\"anchors\"" in prompt


def test_market_prompt_prefers_context_window_and_skips_stale_external_views():
    prompt = build_analysis_prompt(
        symbol="BTCUSDT",
        snapshots=_snapshots(),
        context={
            "analysis_time_utc": "2026-03-07T06:46:40+00:00",
            "data_asof": {"1m": 1772865900},
            "decision_ts": 1772865900,
            "valid_until_utc": 1772869500,
            "brief": {"tradeable_gate": {"tradeable": True, "reasons": []}},
            "alerts_digest": {"count_1h": 0, "count_4h": 0, "top_events": [], "dominant_types": [], "alerts_burst": False},
            "youtube_radar": {"available": True, "stale": True, "consensus_bias": "BULL", "confidence": 80},
            "intel_digest": {"generated_at": "2026-03-06T00:00:00+00:00"},
            "funding_deltas": {"funding_rate": 0.0001},
            "data_quality": {"overall": "GOOD", "notes": []},
            "input_budget_meta": {"dropped_context_blocks": ["alerts_digest"]},
        },
    )

    assert "\"decision_ts\": 1772865900" in prompt
    assert "\"valid_until_utc\": 1772869500" in prompt
    assert "观点源（External Views / YouTube Radar）" not in prompt


def test_prompt_details_carries_dropped_context_blocks_from_context_layer():
    _prompt, meta = build_analysis_prompt_details(
        symbol="BTCUSDT",
        snapshots=_snapshots(),
        context={
            "analysis_time_utc": "2026-03-07T06:46:40+00:00",
            "data_asof": {"1m": 1772865900},
            "decision_ts": 1772865900,
            "valid_until_utc": 1772869500,
            "brief": {"tradeable_gate": {"tradeable": True, "reasons": []}},
            "alerts_digest": {"count_1h": 0, "count_4h": 0, "top_events": [], "dominant_types": [], "alerts_burst": False},
            "youtube_radar": {"available": False, "stale": True},
            "funding_deltas": {"funding_rate": 0.0001},
            "data_quality": {"overall": "GOOD", "notes": []},
            "input_budget_meta": {"dropped_context_blocks": ["account_snapshot"]},
        },
        include_external_views=True,
    )
    assert "account_snapshot" in meta["dropped_context_blocks"]


def test_market_prompt_legacy_path_still_works_without_context():
    prompt = build_analysis_prompt(symbol="BTCUSDT", snapshots=_snapshots(), context=None)
    assert "BTCUSDT" in prompt
    assert "JSON" in prompt
