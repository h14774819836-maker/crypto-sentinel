from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SignalCard(BaseModel):
    timestamp: datetime
    symbol: str
    direction: str
    p_up: float | None = None
    payout_ratio: float | None = None
    ev: float | None = None
    top_features: list[str] = []
    regime: str | None = None
    rationale: str = ""


class AlertCard(BaseModel):
    timestamp: datetime
    symbol: str
    alert_type: str
    severity: str
    reason: str
    metrics: dict[str, Any] = {}


class MarketSnapshot(BaseModel):
    symbol: str
    price: float
    ret_1m: float | None = None
    ret_10m: float | None = None
    rolling_vol_20: float | None = None
    volume_zscore: float | None = None
    updated_at: datetime
