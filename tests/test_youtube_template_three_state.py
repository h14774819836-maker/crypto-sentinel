from __future__ import annotations

from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "app" / "web" / "templates" / "youtube.html"


def test_youtube_template_renders_three_state_summary():
    text = TEMPLATE.read_text(encoding="utf-8")
    assert "yt-queue-summary" in text
    assert "yt.queue.metric.waiting" in text or "Waiting" in text
    assert "yt.queue.metric.running" in text or "Running" in text
    assert "yt.queue.metric.blocked" in text or "Blocked" in text


def test_blocked_reason_badge_and_action_hint_present():
    text = TEMPLATE.read_text(encoding="utf-8")
    assert "analysis-action-" in text
    assert "yt-blocked-chip" in text
    assert "suggestedActionLabel" in text
