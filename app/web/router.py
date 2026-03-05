from __future__ import annotations

from fastapi import APIRouter

from app.config import get_settings
from app.web.routes.api_ops import router as ops_router
from app.web.routers.api_ai import router as api_ai_router
from app.web.routers.api_llm import router as api_llm_router
from app.web.routers.api_market import router as api_market_router
from app.web.routers.api_youtube import router as api_youtube_router
from app.web.routers.pages import router as pages_router

# Keep a module-level settings reference so hot-reload can refresh it too.
settings = get_settings()

router = APIRouter()
router.include_router(pages_router)
router.include_router(api_market_router)
router.include_router(api_youtube_router)
router.include_router(api_llm_router)
router.include_router(api_ai_router)
router.include_router(ops_router)

