from datetime import datetime, timezone

from app.alerts.message_builder import (
    TelegramMessage,
    build_ai_diagnostic_alert,
    build_alert_ref,
    build_ai_signal_message,
    build_anomaly_message,
    build_flash_alert,
    escape_html,
    fmt_dt_bjt,
    fmt_price,
    truncate_safe_html,
)


def test_escape_html():
    assert escape_html("A < B & C > D") == "A &lt; B &amp; C &gt; D"
    assert escape_html(None) == ""


def test_fmt_price():
    assert fmt_price(1.23456) == "1.23"
    assert fmt_price(0.123456) == "0.1235"
    assert fmt_price(None) == "N/A"


def test_fmt_dt_bjt():
    dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert fmt_dt_bjt(dt) == "2025-01-01 20:00 北京时间"


def test_truncate():
    long_text = "A" * 5000
    truncated = truncate_safe_html(long_text, limit=3800)
    assert len(truncated) <= 3900
    assert "截断" in truncated


def test_build_anomaly_message_score_template():
    payload = {
        "event_uid": "event-123",
        "symbol": "BTCUSDT",
        "alert_type": "MOMENTUM_ANOMALY_UP",
        "severity": "WARNING",
        "reason": "1分钟快速上冲，异常强度 Score 82/100（严重），5m/15m待确认。",
        "ts": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "metrics_json": {
            "score": 82,
            "direction": "UP",
            "regime": "RANGING",
            "confirm": {"status": "pending_mtf"},
            "thresholds": {
                "price_threshold_ret": 0.00057,
                "price_threshold_mode": "atr_dynamic",
            },
            "observations": {
                "ret_1m": 0.0005966,
                "volume_zscore": 3.5,
                "threshold_multiple": 1.05,
            },
            "delivery": {"cooldown_seconds_applied": 3600},
            "debug": {"event_kind": "ENTER"},
        },
    }

    msg = build_anomaly_message(payload)
    assert isinstance(msg, TelegramMessage)
    assert msg.kind == "anomaly"
    assert msg.source_id == "event-123"
    assert "BTCUSDT 快速上冲" in msg.text
    assert "Score 82/100" in msg.text
    assert "5m/15m 待确认" in msg.text
    assert "2025-01-01 20:00 北京时间" in msg.text


def test_build_anomaly_message_legacy_fallback():
    payload = {
        "event_uid": "legacy-1",
        "symbol": "BTCUSDT",
        "alert_type": "VOLATILITY_SURGE",
        "severity": "INFO",
        "reason": "rolling_vol_20 > threshold",
        "ts": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "metrics_json": {"rolling_vol_20": 0.01, "threshold": 0.005},
    }
    msg = build_anomaly_message(payload)
    assert "BTCUSDT 异常" in msg.text
    assert "VOLATILITY_SURGE" in msg.text


def test_build_alert_ref():
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ref = build_alert_ref("SOLUSDT", "1d152da2abcdef", ts)
    assert ref.startswith("A250101-2000-SOL-1D15")


def test_build_flash_alert():
    payload = {
        "event_uid": "event-123",
        "symbol": "SOLUSDT",
        "alert_type": "MOMENTUM_ANOMALY_UP",
        "reason": "1m 异动",
        "ts": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "metrics_json": {
            "score": 84,
            "direction": "UP",
            "regime": "VOLATILE",
            "observations": {"ret_1m": 0.0046, "volume_zscore": 1.94},
        },
    }
    msg = build_flash_alert(payload, latest_price=101.234, alert_ref="A250101-2000-SOL-1D15")
    assert msg.kind == "anomaly_flash"
    assert "#A250101-2000-SOL-1D15" in msg.text
    assert "现价: 101.23" in msg.text
    assert "正在调用 AI" in msg.text


def test_build_ai_diagnostic_alert():
    msg = build_ai_diagnostic_alert(
        symbol="SOLUSDT",
        alert_ref="A250101-2000-SOL-1D15",
        diagnosis_text="更偏向顺势突破，关注回踩是否守住。",
        summary_reason="1m 快速上冲",
    )
    assert msg.kind == "anomaly_ai_diagnostic"
    assert "AI诊断 #A250101-2000-SOL-1D15" in msg.text
    assert "核心推演" in msg.text


class DummyAiSignal:
    pass


def test_build_ai_signal_message():
    sig = DummyAiSignal()
    sig.symbol = "ETHUSDT"
    sig.direction = "LONG"
    sig.confidence = 85
    sig.entry_price = 2000.5
    sig.take_profit = 2200.0
    sig.stop_loss = 1900.0
    sig.reasoning = "1. RSI oversold\n2. MACD cross <zero>"
    sig.model_name = "claude-3-5-sonnet-20241022"

    msg = build_ai_signal_message(sig, source_id=456)
    assert isinstance(msg, TelegramMessage)
    assert msg.kind == "ai_signal"
    assert msg.source_id == 456
    assert "📈 <b>ETHUSDT</b>" in msg.text
    assert "约 1:2.0" in msg.text
    assert "&lt;zero&gt;" in msg.text
    assert "claude-3-5-sonnet-20241022" in msg.text

