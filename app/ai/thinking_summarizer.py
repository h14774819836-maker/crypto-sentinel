"""Thinking summarizer: buffers reasoning content and triggers periodic one-sentence summaries via a fast model."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class ThoughtBuffer:
    """Maintains a buffer of reasoning text."""

    _buffer: str = field(default_factory=str)

    def append(self, text: str, *, max_chars: int | None = None) -> None:
        self._buffer += text
        if max_chars and max_chars > 0 and len(self._buffer) > max_chars:
            self._buffer = self._buffer[-max_chars:]

    def get_snapshot(self) -> str:
        return self._buffer

    def clear(self) -> None:
        self._buffer = ""


@dataclass
class ThinkingSummarizer:
    """
    Buffers reasoning chunks and triggers summarization when throttler conditions are met.
    Provides deduplication of similar summaries.
    """

    min_chars: int = 100
    min_chars_first: int = 30
    min_interval_sec: float = 3.0
    buffer_max_chars: int = 400
    buffer_keep_chars: int = 4000

    _buffer: ThoughtBuffer = field(default_factory=ThoughtBuffer)
    _last_summarized_len: int = 0
    _last_summary_time: float = 0.0
    _last_summary: str = ""

    def __post_init__(self) -> None:
        self.min_chars = max(1, int(self.min_chars or 1))
        self.min_chars_first = max(1, int(self.min_chars_first or 30))
        self.min_interval_sec = max(0.1, float(self.min_interval_sec or 0.1))
        self.buffer_max_chars = max(50, int(self.buffer_max_chars or 50))
        self.buffer_keep_chars = max(self.buffer_max_chars, int(self.buffer_keep_chars or self.buffer_max_chars))

    def add_reasoning(self, text: str) -> bool:
        """
        Append reasoning text to buffer. Returns True if summarization should be triggered.
        First trigger: when buf_len >= min_chars_first (10-50 chars).
        Subsequent: when min_chars added since last summary AND min_interval_sec elapsed.
        """
        self._buffer.append(text, max_chars=self.buffer_keep_chars)
        now = time.monotonic()
        buf_len = len(self._buffer.get_snapshot())
        chars_ok = (buf_len - self._last_summarized_len) >= self.min_chars
        time_ok = (now - self._last_summary_time) >= self.min_interval_sec
        first_trigger = buf_len >= self.min_chars_first and self._last_summarized_len == 0
        if first_trigger or (chars_ok and time_ok):
            self._last_summarized_len = buf_len
            self._last_summary_time = now
            return True
        return False

    def get_buffer_for_summary(self) -> str:
        """Return buffer content for summarization, truncated to buffer_max_chars (tail)."""
        snap = self._buffer.get_snapshot()
        if len(snap) <= self.buffer_max_chars:
            return snap
        return snap[-self.buffer_max_chars :]

    def is_duplicate(self, summary: str) -> bool:
        """Check if summary is too similar to last one (simple substring / prefix match)."""
        if not summary or not self._last_summary:
            return False
        s = summary.strip()
        last = self._last_summary.strip()
        if s == last:
            return True
        if len(s) < 4 or len(last) < 4:
            return False
        if s in last or last in s:
            return True
        if s[:6] == last[:6]:
            return True
        return False

    def set_last_summary(self, summary: str) -> None:
        self._last_summary = summary
