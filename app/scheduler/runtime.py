from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx
from sqlalchemy.orm import sessionmaker

from app.alerts.telegram import TelegramClient
from app.config import Settings
from app.providers.binance_provider import BinanceProvider

if TYPE_CHECKING:
    from app.ai.analyst import MarketAnalyst
    from app.ai.provider import LLMProvider


@dataclass(slots=True)
class WorkerRuntime:
    settings: Settings
    session_factory: sessionmaker
    provider: BinanceProvider
    telegram: TelegramClient
    started_at: datetime
    version: str
    latest_prices: dict[str, float] = field(default_factory=dict)
    market_analyst: MarketAnalyst | None = None
    youtube_llm_provider: LLMProvider | None = None
    llm_reload_revision_applied: str = ""
    llm_reload_last_check_ts: datetime | None = None
    http_client: httpx.AsyncClient | None = None
    sem_llm: asyncio.Semaphore | None = None
    sem_youtube: asyncio.Semaphore | None = None
    sem_binance: asyncio.Semaphore | None = None
    job_metrics: list[dict[str, Any]] = field(default_factory=list)

