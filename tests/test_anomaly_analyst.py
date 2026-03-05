from datetime import datetime, timezone

from app.ai.anomaly_analyst import (
    _build_anomaly_diagnostic_prompt,
    _build_batched_prompt,
    _merge_batch_alerts,
    _normalize_alert_for_batch,
    _pick_intel_items,
)


def test_pick_intel_items_filters_severity():
    digest = {
        "items": [
            {"title": "low", "severity": 20},
            {"title": "mid", "severity": 55},
            {"headline": "high", "score": 90},
        ]
    }
    picked = _pick_intel_items(digest, min_severity=50, limit=5)
    assert len(picked) == 2
    assert picked[0]["title"] == "mid"
    assert picked[1]["title"] == "high"


def test_build_anomaly_diagnostic_prompt_is_trimmed():
    payload = {
        "symbol": "SOLUSDT",
        "reason": "1m spike",
        "ts": datetime(2026, 3, 4, 12, 0, 0, tzinfo=timezone.utc),
        "metrics_json": {
            "score": 84,
            "direction": "UP",
            "regime": "VOLATILE",
            "observations": {"ret_1m": 0.0046, "volume_zscore": 1.9},
            "thresholds": {"price_threshold_ret": 0.0013},
            "confirm": {"status": "pending_mtf"},
        },
    }
    prompt = _build_anomaly_diagnostic_prompt(
        symbol="SOLUSDT",
        alert_payload=payload,
        intel_digest_json={"items": [{"title": "A", "severity": 80}]},
        youtube_consensus_json={"summary": "short", "confidence": 0.6},
        youtube_insights=[],
    )
    assert "IntelKeyItems(Severity>=50)" in prompt
    assert "YoutubeConsensus" in prompt
    assert "1m spike" in prompt


def test_merge_batch_alerts_dedup_and_limit():
    batch = [
        {
            "event_uid": "uid1",
            "alert_ref": "A1",
            "alert_payload": {"event_uid": "uid1", "ts": "1", "reason": "r1", "metrics_json": {"score": 80}},
        },
        {
            "event_uid": "uid1",
            "alert_ref": "A1b",
            "alert_payload": {"event_uid": "uid1", "ts": "2", "reason": "r1-new", "metrics_json": {"score": 85}},
        },
        {
            "event_uid": "uid2",
            "alert_ref": "A2",
            "alert_payload": {"event_uid": "uid2", "ts": "3", "reason": "r2", "metrics_json": {"score": 88}},
        },
    ]
    merged = _merge_batch_alerts(batch, max_alerts=1)
    assert len(merged) == 1
    assert merged[0]["event_uid"] == "uid2"


def test_build_batched_prompt_mentions_batch_size():
    alerts = [
        {"alert_ref": "A1", "ts": "2026-03-04T12:00:00Z", "score": 80, "direction": "UP", "regime": "VOLATILE", "confirm": "pending_mtf", "reason": "x"},
        {"alert_ref": "A2", "ts": "2026-03-04T12:01:00Z", "score": 86, "direction": "UP", "regime": "VOLATILE", "confirm": "confirmed_5m", "reason": "y"},
    ]
    prompt = _build_batched_prompt(
        symbol="SOLUSDT",
        alerts=alerts,
        intel_digest_json={"items": []},
        youtube_consensus_json={},
        youtube_insights=[],
    )
    assert "batch_size=2" in prompt
    assert "最近短时告警序列" in prompt


def test_normalize_alert_for_batch_keeps_source():
    row = _normalize_alert_for_batch(
        {
            "event_uid": "tick:BTCUSDT:1:up",
            "alert_ref": "TICK-BTCUSDT-UP",
            "alert_payload": {
                "reason": "tick",
                "metrics_json": {"confirm": {"status": "tick_prealert"}, "delivery": {"source": "tick_prealert"}},
            },
        }
    )
    assert row["source"] == "tick_prealert"
