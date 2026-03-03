from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.ai.grounding.models import FactsIndex, Finding, GroundingResult, Severity
from app.ai.grounding.utils import normalize_metric_key, normalize_timeframe, parse_number
from app.ai.grounding.validators.anchor_path import AnchorPathValidator
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator
from app.ai.grounding.validators.anchor_value_tolerance import AnchorValueToleranceValidator
from app.ai.grounding.validators.coverage_quality import CoverageQualityValidator
from app.ai.grounding.validators.cross_field_consistency import CrossFieldConsistencyValidator
from app.ai.grounding.validators.evidence_metric_nearest import EvidenceMetricNearestMatchValidator
from app.ai.grounding.validators.range_plausibility import RangePlausibilityValidator
from app.ai.grounding.validators.timeframe_coherence import TimeframeCoherenceValidator


DEFAULT_GROUNDING_MODE = "balanced"
SUPPORTED_GROUNDING_MODES = {"strict", "balanced", "lenient"}


class GroundingEngine:
    def __init__(self, validators: list[GroundingValidator] | None = None) -> None:
        self.validators = validators or [
            AnchorPathValidator(),
            AnchorValueToleranceValidator(),
            EvidenceMetricNearestMatchValidator(),
            TimeframeCoherenceValidator(),
            RangePlausibilityValidator(),
            CrossFieldConsistencyValidator(),
            CoverageQualityValidator(),
        ]

    def validate(
        self,
        *,
        data: dict[str, Any],
        facts: dict[str, Any],
        facts_index: FactsIndex,
        mode: str = DEFAULT_GROUNDING_MODE,
        severe_multiplier: float = 3.0,
    ) -> GroundingResult:
        mode_norm = (mode or DEFAULT_GROUNDING_MODE).strip().lower()
        if mode_norm not in SUPPORTED_GROUNDING_MODES:
            mode_norm = DEFAULT_GROUNDING_MODE

        ctx = GroundingContext(
            data=data,
            facts=facts,
            facts_index=facts_index,
            mode=mode_norm,
            severe_multiplier=max(1.0, float(severe_multiplier or 3.0)),
        )

        all_findings: list[Finding] = []
        for validator in self.validators:
            all_findings.extend(validator.validate(ctx))

        hard_errors: list[Finding] = []
        warnings: list[Finding] = []
        for finding in all_findings:
            severity = self._apply_mode_to_severity(finding, ctx)
            if severity == Severity.HARD:
                hard_errors.append(finding)
            else:
                warnings.append(finding)

        score, score_breakdown = self._calculate_score_breakdown(hard_errors=hard_errors, warnings=warnings)
        stats = {
            "validators": len(self.validators),
            "hard_error_count": len(hard_errors),
            "warning_count": len(warnings),
            "anchors_checked": len(data.get("anchors") or []) if isinstance(data.get("anchors"), list) else 0,
            "evidence_checked": len(data.get("evidence") or []) if isinstance(data.get("evidence"), list) else 0,
            "validator_names": [validator.__class__.__name__ for validator in self.validators],
        }
        return GroundingResult(
            hard_errors=hard_errors,
            warnings=warnings,
            score=score,
            score_breakdown=score_breakdown,
            stats=stats,
        )

    def _apply_mode_to_severity(self, finding: Finding, ctx: GroundingContext) -> Severity:
        if ctx.mode == "strict":
            return Severity.HARD
        if ctx.mode != "lenient":
            return finding.severity
        if finding.severity != Severity.HARD:
            return finding.severity

        if finding.code not in {"ANCHOR_VALUE_OUT_OF_TOL", "EVIDENCE_METRIC_OUT_OF_TOL"}:
            return finding.severity

        if finding.abs_diff is None and finding.rel_diff is None:
            return finding.severity

        hard_abs = 0.0
        hard_rel = 0.0
        if isinstance(finding.expected, (int, float)) and isinstance(finding.observed, (int, float)):
            expected_num = abs(float(finding.expected))
            hard_abs = max(1e-8, expected_num * 0.001)
            hard_rel = 0.0045

        too_far_abs = (finding.abs_diff or 0.0) > hard_abs * ctx.severe_multiplier if hard_abs > 0 else False
        too_far_rel = (finding.rel_diff or 0.0) > hard_rel * ctx.severe_multiplier if hard_rel > 0 else False
        if too_far_abs or too_far_rel:
            return Severity.HARD
        return Severity.WARN

    def _calculate_score_breakdown(
        self,
        *,
        hard_errors: list[Finding],
        warnings: list[Finding],
    ) -> tuple[float, dict[str, Any]]:
        components: dict[str, dict[str, Any]] = {
            "structure": {"weight": 0.2, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "numeric_alignment": {"weight": 0.25, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "plausibility": {"weight": 0.15, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "timeframe": {"weight": 0.1, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "cross_field": {"weight": 0.2, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "coverage": {"weight": 0.1, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
            "other": {"weight": 0.0, "hard": 0, "warn": 0, "score": 100.0, "codes": []},
        }

        for finding in hard_errors:
            bucket = _component_for_code(finding.code)
            components[bucket]["hard"] += 1
            components[bucket]["codes"].append(finding.code)
        for finding in warnings:
            bucket = _component_for_code(finding.code)
            components[bucket]["warn"] += 1
            components[bucket]["codes"].append(finding.code)

        weighted_sum = 0.0
        total_weight = 0.0
        for bucket_name, bucket in components.items():
            hard_n = int(bucket["hard"])
            warn_n = int(bucket["warn"])
            raw_score = max(0.0, 100.0 - hard_n * 35.0 - warn_n * 10.0)
            bucket["score"] = round(raw_score, 2)
            bucket["codes"] = sorted(set(bucket["codes"]))[:8]
            weight = float(bucket["weight"])
            if bucket_name == "other":
                continue
            weighted_sum += raw_score * weight
            total_weight += weight

        final_score = (weighted_sum / total_weight) if total_weight > 0 else 100.0
        final_score = max(0.0, min(100.0, final_score))
        return round(final_score, 2), components


def build_facts_index(facts: dict[str, Any]) -> FactsIndex:
    by_metric_key: dict[str, list[float]] = {}
    by_timeframe: dict[str, dict[str, list[float]]] = {}
    text_blobs: list[str] = []
    available_timeframes: set[str] = set()
    atr_by_timeframe: dict[str, float] = {}
    reference_price: float | None = None

    root = facts.get("facts") if isinstance(facts, dict) else {}
    snapshots = (root or {}).get("multi_tf_snapshots") if isinstance(root, dict) else {}
    if isinstance(snapshots, dict):
        for raw_tf, snap in snapshots.items():
            tf = normalize_timeframe(raw_tf)
            if not tf:
                continue
            available_timeframes.add(tf)
            tf_bucket = by_timeframe.setdefault(tf, {})
            latest = (snap or {}).get("latest") if isinstance(snap, dict) else {}
            if not isinstance(latest, dict):
                continue
            for raw_key, raw_value in latest.items():
                key = normalize_metric_key(raw_key)
                num = parse_number(raw_value)
                if num is None:
                    if isinstance(raw_value, str) and raw_value.strip():
                        text_blobs.append(raw_value.strip())
                    continue
                by_metric_key.setdefault(key, []).append(num)
                tf_bucket.setdefault(key, []).append(num)

            atr = tf_bucket.get("atr_14", [])
            if atr:
                atr_by_timeframe[tf] = float(atr[-1])
            if reference_price is None:
                close_vals = tf_bucket.get("close", [])
                if close_vals:
                    reference_price = float(close_vals[-1])

    for pref_tf in ("1m", "5m", "15m", "1h", "4h"):
        if reference_price is not None:
            break
        close_vals = ((by_timeframe.get(pref_tf) or {}).get("close") or [])
        if close_vals:
            reference_price = float(close_vals[-1])

    return FactsIndex(
        by_metric_key={k: sorted(v) for k, v in by_metric_key.items()},
        by_timeframe={tf: {k: sorted(vs) for k, vs in metrics.items()} for tf, metrics in by_timeframe.items()},
        text_blobs=text_blobs,
        available_timeframes=available_timeframes,
        reference_price=reference_price,
        atr_by_timeframe=atr_by_timeframe,
    )


def finding_to_dict(finding: Finding) -> dict[str, Any]:
    payload = asdict(finding)
    payload["severity"] = finding.severity.value
    return payload


def _component_for_code(code: str) -> str:
    if code.startswith("ANCHOR_PATH_"):
        return "structure"
    if code.startswith("ANCHOR_VALUE_") or code.startswith("EVIDENCE_METRIC_"):
        return "numeric_alignment"
    if code.startswith("TIMEFRAME_"):
        return "timeframe"
    if (
        code.startswith("METRIC_")
        or code.startswith("PRICE_")
        or code.startswith("VOLATILITY_")
        or code.startswith("ZSCORE_")
        or code.startswith("FUNDING_")
    ):
        return "plausibility"
    if code.startswith("CROSS_FIELD_"):
        return "cross_field"
    if code.startswith("COVERAGE_"):
        return "coverage"
    return "other"
