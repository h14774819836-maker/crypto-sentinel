from __future__ import annotations
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.asr_runtime import log_asr_runtime_status
from app.config import get_settings
from app.db.guards import ensure_db_backend_allowed
from app.logging import setup_logging
from app.runtime_control import clear_runtime_state, is_docker_compose_runtime
from app.web.api_telegram import router as telegram_router
from app.web.router import router as web_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings = get_settings()
    ensure_db_backend_allowed(settings)
    if is_docker_compose_runtime():
        clear_runtime_state()
    app.state.asr_status = log_asr_runtime_status(settings, component="api")
    yield


app = FastAPI(title="Crypto Sentinel", version="0.1.0", lifespan=lifespan)
static_dir = Path(__file__).resolve().parent / "web" / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.include_router(telegram_router)
app.include_router(web_router)
