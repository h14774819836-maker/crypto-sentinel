from __future__ import annotations

import sqlite3
from pathlib import Path

from sqlalchemy.engine import make_url

from app.config import get_settings


RUNTIME_COLUMNS: dict[str, str] = {
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


def _sqlite_path_from_url(database_url: str) -> str:
    url = make_url(database_url)
    if url.drivername != "sqlite":
        raise RuntimeError(f"Only sqlite is supported by this migration helper (got {url.drivername})")
    if url.database in (None, "", ":memory:"):
        raise RuntimeError("SQLite in-memory database is not supported for this migration helper")
    db_path = Path(url.database)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return str(db_path)


def main() -> None:
    settings = get_settings()
    db_path = _sqlite_path_from_url(settings.database_url)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='youtube_videos'")
        if cur.fetchone() is None:
            print("youtube_videos table not found, nothing to migrate")
            return

        cols = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(youtube_videos)").fetchall()
            if len(row) >= 2
        }

        added: list[str] = []
        for col, sql_type in RUNTIME_COLUMNS.items():
            if col in cols:
                continue
            conn.execute(f"ALTER TABLE youtube_videos ADD COLUMN {col} {sql_type}")
            added.append(col)

        if added:
            conn.commit()
            print(f"Added columns to youtube_videos: {', '.join(added)}")
        else:
            print("youtube_videos runtime analysis columns already present")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
