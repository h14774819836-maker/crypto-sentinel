from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import normalize_timeframe
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class TimeframeCoherenceValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        evidence = ctx.data.get("evidence") or []
        if not isinstance(evidence, list):
            return findings
        for ev_idx, ev in enumerate(evidence):
            if not isinstance(ev, dict):
                continue
            timeframe = normalize_timeframe(ev.get("timeframe"))
            if not timeframe:
                continue
            if timeframe not in ctx.facts_index.available_timeframes:
                findings.append(
                    Finding(
                        code="TIMEFRAME_MISSING",
                        severity=Severity.WARN,
                        message=f"evidence[{ev_idx}] timeframe 不存在于快照: {timeframe}",
                        timeframe=timeframe,
                    )
                )
        return findings
