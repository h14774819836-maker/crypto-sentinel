from __future__ import annotations

from datetime import datetime, timezone

from app.ai.prompts import SYSTEM_PROMPT, build_analysis_prompt


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
    assert "绝对禁止生成、计算、或篡改输入 JSON 中不存在的数值" in SYSTEM_PROMPT
    assert "\"anchors\"" in prompt


def test_market_prompt_legacy_path_still_works_without_context():
    prompt = build_analysis_prompt(symbol="BTCUSDT", snapshots=_snapshots(), context=None)
    assert "BTCUSDT" in prompt
    assert "JSON" in prompt
