import logging
import os
from logging.config import dictConfig

from app.config import get_settings


def setup_logging() -> None:
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
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
                "market_ai_file": {
                    "class": "logging.handlers.TimedRotatingFileHandler",
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
