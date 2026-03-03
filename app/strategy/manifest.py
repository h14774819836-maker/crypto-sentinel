from __future__ import annotations

import hashlib
import json
from typing import Any


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_manifest_payload(
    *,
    prompt_template_hash: str,
    schema_version: str,
    model_provider: str,
    model_name: str,
    temperature: float,
    top_p: float | None,
    max_tokens: int | None,
    reasoning_effort: str | None,
    feature_keys: list[str],
    timeframes: list[str],
    decision_rules_version: str,
    facts_builder_version: str,
    data_pipeline_version: str,
    exchange: str,
    market_type: str,
    regime_calc_mode: str,
    eval_replay_tf: str = "1m",
    ambiguous_policy: str = "BOTH_HIT_AS_AMBIGUOUS",
    git_commit: str = "unknown",
) -> dict[str, Any]:
    return {
        "prompt_template_hash": prompt_template_hash,
        "schema_version": schema_version,
        "model_provider": model_provider,
        "model_name": model_name,
        "temperature": float(temperature),
        "top_p": top_p,
        "max_tokens": max_tokens,
        "reasoning_effort": reasoning_effort,
        "feature_keys": sorted([str(k) for k in feature_keys]),
        "timeframes": sorted([str(tf) for tf in timeframes]),
        "decision_rules_version": decision_rules_version,
        "facts_builder_version": facts_builder_version,
        "data_pipeline_version": data_pipeline_version,
        "exchange": exchange,
        "market_type": market_type,
        "regime_calc_mode": regime_calc_mode,
        "eval_config": {
            "replay_tf": eval_replay_tf,
            "ambiguous_policy": ambiguous_policy,
        },
        "git_commit": git_commit or "unknown",
    }


def build_manifest_id(manifest_payload: dict[str, Any]) -> str:
    canonical = _canonical_json(manifest_payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
