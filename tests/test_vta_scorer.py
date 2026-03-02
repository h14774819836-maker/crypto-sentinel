import pytest
from app.ai.vta_scorer import normalize_vta, validate_vta, compute_scores, _normalize_enum, _safe_float

def test_normalize_enum():
    assert _normalize_enum("强烈看空") == "STRONG_BEAR"
    assert _normalize_enum("Strong-Bear") == "STRONG_BEAR"
    assert _normalize_enum("极强") == "VERY_HIGH"
    assert _normalize_enum("Some random text") == "SOME_RANDOM_TEXT"
    assert _normalize_enum(None) is None

def test_safe_float():
    assert _safe_float("68,500") == 68500.0
    assert _safe_float("约68500附近") == 68500.0
    assert _safe_float("66000-65800") == 65900.0
    assert _safe_float(68500) == 68500.0
    assert _safe_float("无法确定") is None

def test_normalize_vta_defaults():
    raw = {}
    vta = normalize_vta(raw)
    assert vta['vta_version'] == '1.0'
    assert 'meta' in vta
    assert isinstance(vta['levels']['resistance'], list)
    assert 'provenance' in vta
    assert vta['provenance']['normalized'] is True

def test_validate_vta():
    vta = normalize_vta({})
    ok, errors = validate_vta(vta)
    assert ok is True
    assert len(errors) == 0

    vta['key_points'] = [1, 2, 3, 4, 5, 6]
    ok, errors = validate_vta(vta)
    assert ok is False
    assert "key_points extends maximum length of 5" in errors[0]

def test_compute_scores():
    vta = normalize_vta({
        "market_view": {
            "bias_1_7d": "STRONG_BEAR",
            "bias_1_4w": "BEAR",
            "conviction": "VERY_HIGH"
        },
        "key_points": [
            {"type": "PATTERN"}, {"type": "INDICATOR"}, {"type": "LEVEL"}
        ],
        "levels": {
            "resistance": [{"level": "68500"}],
            "support": [{"level": "59800"}]
        },
        "trade_plans": [
            {
                "direction": "SHORT",
                "style": "TREND",
                "entry": {"type": "LIMIT", "price": "67900"},
                "stop": {"price": "69000"},
                "targets": [{"price": "66000"}],
                "rules": ["NO_CHASE", "WAIT_CLOSE_CONFIRM"]
            }
        ],
        "risks": [
            {"text": "High volatility on weekends", "severity": "HIGH"}
        ]
    })
    
    scores = compute_scores(vta)
    
    # Check DC (50 * (-2*0.65 + -1*0.35) * 1) => 50 * (-1.3 - 0.35) => 50 * -1.65 => -82.5
    assert scores['dc'] == -82.5
    
    # Completeness: Entry (6), Stop (6), targets (8) = 20. Conviction is HIGH(1.0) and alt is missing so -5 => 15
    assert scores['pq_breakdown']['completeness'] == 15.0
    
    # Specificity: targets spec 10 + levels spec 10 => 20
    assert scores['pq_breakdown']['specificity'] == 20.0
    
    # Evidence: 3 points, >=2 types => 20
    assert scores['pq_breakdown']['evidence'] == 20.0
    
    # Consistency: direction SHORT matches dc_bear, TREND matches => 15
    assert scores['pq_breakdown']['consistency'] == 15.0
    
    # Risk: has NO_CHASE => 20
    assert scores['pq_breakdown']['risk'] == 20.0
    
    # Total PQ = 15 + 20 + 20 + 15 + 20 = 90
    assert scores['pq'] == 90.0
    
    # VSI = -82.5 * 0.9 = -74.25
    assert scores['vsi'] == -74.25

def test_compute_scores_counter_trend():
    vta = normalize_vta({
        "market_view": {
            "bias_1_7d": "STRONG_BEAR",
            "conviction": "MEDIUM"
        },
        "trade_plans": [
            {
                "direction": "LONG", # Counter trend!
                "style": "COUNTER_TREND",
            }
        ]
    })
    
    scores = compute_scores(vta)
    assert scores['dc'] < 0
    # Consistency should be 15 - 5 = 10 because it's COUNTER_TREND
    assert scores['pq_breakdown']['consistency'] == 10.0
