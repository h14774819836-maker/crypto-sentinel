from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.ai.grounding.models import FactsIndex, Finding


@dataclass(slots=True)
class GroundingContext:
    data: dict[str, Any]
    facts: dict[str, Any]
    facts_index: FactsIndex
    mode: str
    severe_multiplier: float = 3.0


class GroundingValidator:
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        raise NotImplementedError
