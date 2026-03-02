from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.db.repository import get_worker_last_seen
from app.db.session import SessionLocal


def quick_db_health_and_worker(database_url: str, worker_id: str) -> tuple[bool, datetime | None]:
    if _is_sqlite_url(database_url):
        return _quick_sqlite_health_and_worker(database_url, worker_id)
    return _quick_generic_health_and_worker(worker_id)


def _quick_generic_health_and_worker(worker_id: str) -> tuple[bool, datetime | None]:
    db_ok = False
    worker_last_seen = None
    with SessionLocal() as db:
        try:
            db.execute(text("SELECT 1"))
            db_ok = True
        except Exception:
            db_ok = False

        if db_ok:
            try:
                worker_last_seen = get_worker_last_seen(db, worker_id=worker_id)
            except Exception:
                worker_last_seen = None
    return db_ok, worker_last_seen


def _is_sqlite_url(database_url: str) -> bool:
    try:
        parsed = make_url(database_url)
        return parsed.get_backend_name() == "sqlite"
    except Exception:
        return database_url.startswith("sqlite")


def _resolve_sqlite_path(database_url: str) -> str:
    parsed = make_url(database_url)
    database = parsed.database or ""
    if database == ":memory:":
        return database
    if database.startswith("./"):
        database = database[2:]
    return str((Path.cwd() / database).resolve()) if database and not Path(database).is_absolute() else database


def _quick_sqlite_health_and_worker(database_url: str, worker_id: str) -> tuple[bool, datetime | None]:
    db_path = _resolve_sqlite_path(database_url)
    if not db_path or db_path == ":memory:":
        return False, None

    db_ok = False
    worker_last_seen = None
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=0.1)
        conn.execute("PRAGMA busy_timeout = 100")
        conn.execute("SELECT 1").fetchone()
        db_ok = True
        try:
            row = conn.execute(
                "SELECT last_seen FROM worker_status WHERE worker_id = ? ORDER BY last_seen DESC LIMIT 1",
                (worker_id,),
            ).fetchone()
            if row and row[0]:
                worker_last_seen = _parse_datetime(row[0])
        except sqlite3.OperationalError:
            worker_last_seen = None
    except sqlite3.OperationalError:
        db_ok = False
    except Exception:
        db_ok = False
    finally:
        if conn is not None:
            conn.close()
    return db_ok, worker_last_seen


def _parse_datetime(raw_value: str | datetime) -> datetime | None:
    if isinstance(raw_value, datetime):
        return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

