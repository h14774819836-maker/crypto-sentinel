from __future__ import annotations

from app.ai.grounding.models import Finding, Severity
from app.ai.grounding.utils import is_scalar, resolve_dot_path
from app.ai.grounding.validators.base import GroundingContext, GroundingValidator


class AnchorPathValidator(GroundingValidator):
    def validate(self, ctx: GroundingContext) -> list[Finding]:
        findings: list[Finding] = []
        anchors = ctx.data.get("anchors") or []
        if not isinstance(anchors, list):
            return findings
        for idx, anchor in enumerate(anchors):
            if not isinstance(anchor, dict):
                continue
            path = str(anchor.get("path") or "")
            ok, actual, resolved = resolve_dot_path(ctx.facts, path)
            if not ok:
                findings.append(
                    Finding(
                        code="ANCHOR_PATH_MISSING",
                        severity=Severity.HARD,
                        message=f"anchors[{idx}] path 不存在: {path}",
                        path=path,
                    )
                )
                continue
            if not is_scalar(actual):
                findings.append(
                    Finding(
                        code="ANCHOR_PATH_NOT_SCALAR",
                        severity=Severity.HARD,
                        message=f"anchors[{idx}] path 必须指向标量: {resolved}",
                        path=resolved,
                    )
                )
        return findings
