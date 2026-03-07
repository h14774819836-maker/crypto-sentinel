from __future__ import annotations

import io
import logging

from app.logging import UnicodeSafeStreamHandler


class _GbkLikeStream(io.StringIO):
    encoding = "gbk"

    def write(self, s: str) -> int:
        s.encode(self.encoding, errors="strict")
        return super().write(s)


def test_unicode_safe_stream_handler_escapes_unencodable_console_text():
    stream = _GbkLikeStream()
    handler = UnicodeSafeStreamHandler(stream=stream)
    handler.setFormatter(logging.Formatter("%(message)s"))

    record = logging.LogRecord(
        name="crypto_sentinel",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="stream check ✓",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    assert "\\u2713" in stream.getvalue()
