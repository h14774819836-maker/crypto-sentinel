from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(str, Enum):
    HARD = "hard"
    WARN = "warn"


@dataclass(slots=True)
class Finding:
    code: str
    severity: Severity
    message: str
    path: str | None = None
    metric: str | None = None
    timeframe: str | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    expected: float | str | None = None
    observed: float | str | None = None


@dataclass(slots=True)
class GroundingResult:
    hard_errors: list[Finding] = field(default_factory=list)
    warnings: list[Finding] = field(default_factory=list)
    score: float = 100.0
    score_breakdown: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FactsIndex:
    by_metric_key: dict[str, list[float]]
    by_timeframe: dict[str, dict[str, list[float]]]
    text_blobs: list[str]
    available_timeframes: set[str]
    reference_price: float | None
    atr_by_timeframe: dict[str, float]
