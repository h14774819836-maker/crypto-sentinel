from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.db.guards import ensure_db_backend_allowed
from app.logging import setup_logging
from app.web.api_telegram import router as telegram_router
from app.web.router import router as web_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    setup_logging()
    ensure_db_backend_allowed(get_settings())
    yield


app = FastAPI(title="Crypto Sentinel", version="0.1.0", lifespan=lifespan)
static_dir = Path(__file__).resolve().parent / "web" / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.include_router(telegram_router)
app.include_router(web_router)
