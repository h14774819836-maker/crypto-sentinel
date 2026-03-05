"""LLM API routes. Handlers imported from views for now."""
from __future__ import annotations

from fastapi import APIRouter

from app.web import views

router = APIRouter()

router.add_api_route(
    "/api/llm/config",
    views.llm_get_config,
    methods=["GET"],
)
router.add_api_route(
    "/api/llm/config",
    views.llm_update_config,
    methods=["POST"],
)
router.add_api_route(
    "/api/llm/status",
    views.llm_status_api,
    methods=["GET"],
)
router.add_api_route(
    "/api/llm/calls",
    views.llm_calls_api,
    methods=["GET"],
)
router.add_api_route(
    "/api/llm/failures",
    views.llm_failures_api,
    methods=["GET"],
)
router.add_api_route(
    "/api/llm/selfcheck",
    views.llm_selfcheck_api,
    methods=["POST"],
)
