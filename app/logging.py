import logging
import os
import sys
import time
from logging.config import dictConfig
from logging.handlers import TimedRotatingFileHandler

from app.config import get_settings


class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Windows-friendly timed handler that tolerates locked files.

    On Windows, rename during rollover may fail with WinError 32 when another
    process still has the file open. In that case we skip this rollover cycle
    and move to the next rollover boundary instead of spamming stack traces.
    """

    def doRollover(self) -> None:  # noqa: N802 (stdlib signature)
        try:
            super().doRollover()
        except PermissionError as exc:
            # Re-open stream and postpone rollover to next interval.
            if self.stream:
                try:
                    self.stream.close()
                except Exception:
                    pass
                self.stream = None
            if not self.delay:
                self.stream = self._open()

            current_time = int(time.time())
            next_rollover = self.computeRollover(current_time)
            while next_rollover <= current_time:
                next_rollover += self.interval
            self.rolloverAt = next_rollover
            logging.getLogger("crypto_sentinel").warning(
                "market_ai log rollover skipped due locked file: %s", exc
            )


class UnicodeSafeStreamHandler(logging.StreamHandler):
    """Console handler that escapes non-encodable characters on legacy consoles."""

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        stream = self.stream
        if stream is None:
            return
        try:
            stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            try:
                encoding = getattr(stream, "encoding", None) or "utf-8"
                safe_msg = msg.encode(encoding, errors="backslashreplace").decode(encoding, errors="ignore")
                stream.write(safe_msg + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def _reconfigure_standard_streams() -> None:
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="backslashreplace")
            except Exception:
                pass


def setup_logging() -> None:
    _reconfigure_standard_streams()
    settings = get_settings()
    market_ai_log_file = settings.market_ai_log_file or "data/logs/market_ai.log"
    market_ai_log_dir = os.path.dirname(market_ai_log_file)
    if market_ai_log_dir:
        os.makedirs(market_ai_log_dir, exist_ok=True)
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                },
                "market_ai": {
                    "format": "%(message)s",
                },
            },
            "handlers": {
                "default": {
                    "class": "app.logging.UnicodeSafeStreamHandler",
                    "formatter": "default",
                },
                "market_ai_file": {
                    "class": "app.logging.SafeTimedRotatingFileHandler",
                    "formatter": "market_ai",
                    "filename": market_ai_log_file,
                    "when": "midnight",
                    "interval": 1,
                    "backupCount": max(1, int(settings.market_ai_log_backup_count or 7)),
                    "encoding": "utf-8",
                    "utc": True,
                },
            },
            "root": {
                "level": settings.log_level,
                "handlers": ["default"],
            },
            "loggers": {
                "apscheduler": {
                    "level": "WARNING",
                    "handlers": ["default"],
                    "propagate": False,
                },
                "httpx": {
                    "level": "WARNING",
                    "handlers": ["default"],
                    "propagate": False,
                },
                "crypto_sentinel.ai.market": {
                    "level": settings.market_ai_log_level,
                    "handlers": ["default", "market_ai_file"],
                    "propagate": False,
                },
            },
        }
    )


logger = logging.getLogger("crypto_sentinel")
market_ai_logger = logging.getLogger("crypto_sentinel.ai.market")
