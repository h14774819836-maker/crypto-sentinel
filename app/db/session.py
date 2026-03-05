from __future__ import annotations

import logging
import os
from collections.abc import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


def _ensure_sqlite_dir(url: str) -> None:
    if not url.startswith("sqlite:///"):
        return
    path = url.replace("sqlite:///", "", 1)
    if path.startswith("./"):
        path = path[2:]
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


settings = get_settings()
_ensure_sqlite_dir(settings.database_url)

engine_kwargs: dict = {"future": True, "pool_pre_ping": True}
if settings.database_url.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"timeout": 15}
engine = create_engine(settings.database_url, **engine_kwargs)

if settings.database_url.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA busy_timeout = 15000")  # 15s wait when DB locked (scheduler vs manual refresh)
        cursor.close()


def _ensure_sqlite_youtube_analysis_runtime_columns() -> None:
    """Best-effort SQLite compatibility patch for older DBs missing new runtime columns."""
    if not settings.database_url.startswith("sqlite"):
        return

    runtime_columns: dict[str, str] = {
        "analysis_runtime_status": "TEXT",
        "analysis_stage": "TEXT",
        "analysis_started_at": "DATETIME",
        "analysis_updated_at": "DATETIME",
        "analysis_finished_at": "DATETIME",
        "analysis_retry_count": "INTEGER DEFAULT 0",
        "analysis_next_retry_at": "DATETIME",
        "analysis_last_error_type": "TEXT",
        "analysis_last_error_code": "TEXT",
        "analysis_last_error_message": "TEXT",
    }

    try:
        with engine.begin() as conn:
            has_table = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='youtube_videos'")
            ).first()
            if not has_table:
                return

            rows = conn.execute(text("PRAGMA table_info(youtube_videos)")).fetchall()
            existing_cols = {str(row[1]) for row in rows if len(row) >= 2}
            added: list[str] = []
            for col, sql_type in runtime_columns.items():
                if col in existing_cols:
                    continue
                conn.execute(text(f"ALTER TABLE youtube_videos ADD COLUMN {col} {sql_type}"))
                added.append(col)

        if added:
            logger.warning(
                "Applied SQLite compatibility schema patch for youtube_videos, added columns: %s. "
                "Consider running `python scripts/migrate_youtube_analysis_runtime.py` manually on all deployments.",
                ", ".join(added),
            )
    except Exception as exc:
        # Do not fail import/startup here; downstream code may still run if table is absent.
        logger.warning("SQLite compatibility patch for youtube_videos failed: %s", exc)


_ensure_sqlite_youtube_analysis_runtime_columns()

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
