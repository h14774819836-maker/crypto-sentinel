from __future__ import annotations

from app.ai.vta_scorer import compute_scores, normalize_vta, validate_vta


def test_normalize_vta_tolerates_null_collections():
    raw = {
        "market_view": {
            "bias_1_7d": "BULLISH",
            "bias_1_4w": None,
            "conviction": "MEDIUM",
        },
        "levels": {
            "resistance": None,
            "support": None,
            "other": None,
        },
        "trade_plans": None,
        "key_points": None,
        "risks": None,
        "provenance": {
            "schema_errors": None,
        },
    }
    vta = normalize_vta(raw)
    assert isinstance(vta["levels"]["resistance"], list)
    assert isinstance(vta["levels"]["support"], list)
    assert isinstance(vta["levels"]["other"], list)
    assert isinstance(vta["trade_plans"], list)
    assert isinstance(vta["key_points"], list)
    assert isinstance(vta["risks"], list)
    assert isinstance(vta["provenance"]["schema_errors"], list)


def test_compute_scores_tolerates_nulls_without_none_iterable():
    vta = normalize_vta(
        {
            "market_view": {
                "bias_1_7d": "BEARISH",
                "bias_1_4w": "BULLISH",
                "conviction": "LOW",
            },
            "trade_plans": [
                {
                    "direction": "SHORT",
                    "style": None,
                    "entry": None,
                    "stop": None,
                    "targets": None,
                    "rules": None,
                }
            ],
            "levels": {"resistance": None, "support": None, "other": None},
            "key_points": None,
            "risks": None,
        }
    )
    ok, errors = validate_vta(vta)
    assert ok is True
    assert errors == []
    scores = compute_scores(vta)
    assert isinstance(scores, dict)
    assert set(scores.keys()) == {"dc", "pq", "vsi", "pq_breakdown"}
