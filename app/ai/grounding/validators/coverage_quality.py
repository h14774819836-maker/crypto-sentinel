from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import normalize_metric_key, parse_number, resolve_dot_path
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class CoverageQualityValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        anchors = ctx.data.get("anchors")
        evidence = ctx.data.get("evidence")

        anchors_list = anchors if isinstance(anchors, list) else []
        evidence_list = evidence if isinstance(evidence, list) else []

        if len(anchors_list) < 2:
            findings.append(
                Finding(
                    code="COVERAGE_ANCHORS_TOO_FEW",
                    severity=Severity.WARN,
                    message=f"anchors 数量偏少（{len(anchors_list)}）",
                )
            )
        if len(evidence_list) < 2:
            findings.append(
                Finding(
                    code="COVERAGE_EVIDENCE_TOO_FEW",
                    severity=Severity.WARN,
                    message=f"evidence 数量偏少（{len(evidence_list)}）",
                )
            )

        anchored_metrics = _collect_anchored_metrics(ctx, anchors_list)
        evidence_numeric_metrics: set[str] = set()
        sparse_evidence_count = 0

        for idx, ev in enumerate(evidence_list):
            if not isinstance(ev, dict):
                continue
            metrics = ev.get("metrics")
            if not isinstance(metrics, dict):
                sparse_evidence_count += 1
                continue
            numeric_count = 0
            for raw_key, raw_value in metrics.items():
                if parse_number(raw_value) is None:
                    continue
                numeric_count += 1
                evidence_numeric_metrics.add(normalize_metric_key(raw_key))
            if numeric_count == 0:
                sparse_evidence_count += 1
                findings.append(
                    Finding(
                        code="COVERAGE_EVIDENCE_NO_NUMERIC",
                        severity=Severity.WARN,
                        message=f"evidence[{idx}] 缺少可解析数值指标",
                    )
                )

        if sparse_evidence_count == len(evidence_list) and evidence_list:
            findings.append(
                Finding(
                    code="COVERAGE_NUMERIC_EMPTY",
                    severity=Severity.WARN,
                    message="全部 evidence 都没有可解析数值，核查覆盖度不足",
                )
            )

        key_metrics = {"close", "rsi_14", "atr_14", "funding_rate", "ret_1m"}
        covered = (anchored_metrics | evidence_numeric_metrics) & key_metrics
        if not covered:
            findings.append(
                Finding(
                    code="COVERAGE_KEY_METRICS_MISSING",
                    severity=Severity.WARN,
                    message="关键指标覆盖不足（close/rsi_14/atr_14/funding_rate/ret_1m 均未被引用）",
                )
            )

        return findings


def _collect_anchored_metrics(ctx: GroundingContext, anchors: list[object]) -> set[str]:
    metrics: set[str] = set()
    for item in anchors:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "")
        ok, _actual, resolved = resolve_dot_path(ctx.facts, path)
        if not ok:
            continue
        token = (resolved or "").split(".")[-1]
        if token:
            metrics.add(normalize_metric_key(token))
    return metrics
