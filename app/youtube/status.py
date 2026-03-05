"""Explicit FSM status for YouTube video processing pipeline."""

from __future__ import annotations

# Pipeline stages (explicit, queryable)
PENDING_SUBTITLE = "pending_subtitle"
QUEUED_ASR = "queued_asr"
ASR_FAILED = "asr_failed"
PENDING_ANALYSIS = "pending_analysis"
ANALYZING = "analyzing"
COMPLETED = "completed"
FAILED = "failed"

ALL_STATUSES = (
    PENDING_SUBTITLE,
    QUEUED_ASR,
    ASR_FAILED,
    PENDING_ANALYSIS,
    ANALYZING,
    COMPLETED,
    FAILED,
)

DEFAULT_STATUS = PENDING_SUBTITLE
