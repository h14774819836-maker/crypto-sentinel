from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import (
    classify_diff,
    is_scalar,
    parse_number,
    relative_diff,
    resolve_dot_path,
    scalar_ground_str,
    tolerance_for_metric,
)
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class AnchorValueToleranceValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        anchors = ctx.data.get("anchors") or []
        if not isinstance(anchors, list):
            return findings

        for idx, anchor in enumerate(anchors):
            if not isinstance(anchor, dict):
                continue
            path = str(anchor.get("path") or "")
            observed_raw = anchor.get("value")
            ok, actual, resolved = resolve_dot_path(ctx.facts, path)
            if not ok or not is_scalar(actual):
                continue

            actual_num = parse_number(actual)
            observed_num = parse_number(observed_raw)
            metric = _metric_from_path(resolved)
            timeframe = _timeframe_from_path(resolved)
            atr = ctx.facts_index.atr_by_timeframe.get(timeframe) if timeframe else None
            tol = tolerance_for_metric(metric, reference_price=ctx.facts_index.reference_price, atr=atr)

            if actual_num is None:
                expected = scalar_ground_str(actual)
                observed = scalar_ground_str(observed_raw)
                if expected != observed:
                    findings.append(
                        Finding(
                            code="ANCHOR_VALUE_MISMATCH",
                            severity=Severity.HARD,
                            message=(
                                f"anchors[{idx}] value 不匹配: path={resolved}, expected={expected}, got={observed}"
                            ),
                            path=resolved,
                            metric=metric,
                            timeframe=timeframe,
                            expected=expected,
                            observed=observed,
                        )
                    )
                continue

            if observed_num is None:
                findings.append(
                    Finding(
                        code="ANCHOR_VALUE_NOT_NUMERIC",
                        severity=Severity.HARD,
                        message=f"anchors[{idx}] value 不是可解析数字: path={resolved}, got={observed_raw}",
                        path=resolved,
                        metric=metric,
                        timeframe=timeframe,
                        expected=actual_num,
                        observed=str(observed_raw),
                    )
                )
                continue

            decision = classify_diff(actual=actual_num, observed=observed_num, tol=tol)
            if decision == "pass":
                continue
            abs_diff = abs(observed_num - actual_num)
            rel = relative_diff(actual_num, observed_num)
            findings.append(
                Finding(
                    code="ANCHOR_VALUE_OUT_OF_TOL",
                    severity=Severity.WARN if decision == "warn" else Severity.HARD,
                    message=(
                        f"anchors[{idx}] value 偏差过大: path={resolved}, expected={actual_num:.8g}, "
                        f"got={observed_num:.8g}, abs_diff={abs_diff:.6g}, rel_diff={rel:.4%}"
                    ),
                    path=resolved,
                    metric=metric,
                    timeframe=timeframe,
                    abs_diff=abs_diff,
                    rel_diff=rel,
                    expected=actual_num,
                    observed=observed_num,
                )
            )
        return findings


def _metric_from_path(path: str) -> str:
    token = (path or "").split(".")[-1]
    return token or "unknown_metric"


def _timeframe_from_path(path: str) -> str | None:
    parts = (path or "").split(".")
    for idx, part in enumerate(parts):
        if part == "multi_tf_snapshots" and idx + 1 < len(parts):
            return parts[idx + 1]
    return None
