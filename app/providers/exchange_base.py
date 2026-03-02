from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable


@dataclass(slots=True)
class Candle:
    symbol: str
    timeframe: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str = "binance"


@dataclass(slots=True)
class FundingRateData:
    symbol: str
    mark_price: float | None
    index_price: float | None
    last_funding_rate: float | None
    next_funding_time: datetime | None
    interest_rate: float | None


PriceCallback = Callable[[str, float, datetime], Awaitable[None]]
CandleCallback = Callable[[Candle], Awaitable[None]]


class ExchangeProvider(ABC):
    @abstractmethod
    async def consume_kline_stream(self, symbols: list[str], on_candle: CandleCallback, on_price: PriceCallback | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fetch_klines(self, symbol: str, interval: str, start_ts: datetime, end_ts: datetime) -> list[Candle]:
        raise NotImplementedError

    async def fetch_1m_klines(self, symbol: str, start_ts: datetime, end_ts: datetime) -> list[Candle]:
        return await self.fetch_klines(symbol, "1m", start_ts, end_ts)
