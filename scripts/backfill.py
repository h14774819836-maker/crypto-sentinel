from __future__ import annotations

import argparse
import asyncio

from app.backfill_service import run_backfill
from app.config import get_settings
from app.db.session import Base, engine
from app.logging import setup_logging


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Backfill Binance 1m candles and rebuild local 10m bars")
    parser.add_argument("--days", type=int, default=settings.backfill_days_default, help="Number of days to backfill")
    parser.add_argument("--symbols", type=str, default=",".join(settings.watchlist_symbols), help="CSV symbols")
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    Base.metadata.create_all(bind=engine)
    args = parse_args()
    symbol_list = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    settings = get_settings()
    asyncio.run(run_backfill(days=args.days, symbols=symbol_list, settings=settings))
