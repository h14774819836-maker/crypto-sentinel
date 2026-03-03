from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import (
    classify_diff,
    nearest_value,
    normalize_metric_key,
    normalize_timeframe,
    parse_number,
    relative_diff,
    tolerance_for_metric,
)
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class EvidenceMetricNearestMatchValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        evidence = ctx.data.get("evidence") or []
        if not isinstance(evidence, list):
            return findings

        for ev_idx, ev in enumerate(evidence):
            if not isinstance(ev, dict):
                continue
            timeframe = normalize_timeframe(ev.get("timeframe"))
            metrics = ev.get("metrics")
            if not isinstance(metrics, dict):
                continue
            tf_index = ctx.facts_index.by_timeframe.get(timeframe, {})
            for raw_metric, raw_value in metrics.items():
                observed = parse_number(raw_value)
                if observed is None:
                    continue
                metric = normalize_metric_key(raw_metric)
                candidates = list(tf_index.get(metric, [])) or list(ctx.facts_index.by_metric_key.get(metric, []))
                nearest = nearest_value(candidates, observed)
                if nearest is None:
                    findings.append(
                        Finding(
                            code="EVIDENCE_METRIC_NOT_FOUND",
                            severity=Severity.WARN,
                            message=f"evidence[{ev_idx}].metrics.{raw_metric} 在事实索引中无候选值",
                            metric=metric,
                            timeframe=timeframe,
                            observed=observed,
                        )
                    )
                    continue

                atr = ctx.facts_index.atr_by_timeframe.get(timeframe)
                tol = tolerance_for_metric(metric, reference_price=ctx.facts_index.reference_price, atr=atr)
                decision = classify_diff(actual=nearest, observed=observed, tol=tol)
                if decision == "pass":
                    continue

                abs_diff = abs(observed - nearest)
                rel = relative_diff(nearest, observed)
                findings.append(
                    Finding(
                        code="EVIDENCE_METRIC_OUT_OF_TOL",
                        severity=Severity.WARN if decision == "warn" else Severity.HARD,
                        message=(
                            f"evidence[{ev_idx}].metrics.{raw_metric} 偏差过大: nearest={nearest:.8g}, "
                            f"observed={observed:.8g}, abs_diff={abs_diff:.6g}, rel_diff={rel:.4%}"
                        ),
                        metric=metric,
                        timeframe=timeframe,
                        abs_diff=abs_diff,
                        rel_diff=rel,
                        expected=nearest,
                        observed=observed,
                    )
                )
        return findings
