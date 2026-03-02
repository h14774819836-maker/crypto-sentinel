from __future__ import annotations

from fastapi import APIRouter

from app.config import get_settings
from app.web.routes.api_ops import router as ops_router
from app.web.views import router as legacy_router

# Keep a module-level settings reference so hot-reload can refresh it too.
settings = get_settings()

router = APIRouter()
router.include_router(legacy_router)
router.include_router(ops_router)

