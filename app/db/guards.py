from __future__ import annotations

from typing import Any

from sqlalchemy.engine import make_url


def _csv_to_set(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def _is_sqlite_url(database_url: str) -> bool:
    try:
        parsed = make_url(database_url)
        return parsed.get_backend_name() == "sqlite"
    except Exception:
        return str(database_url).startswith("sqlite")


def ensure_db_backend_allowed(settings: Any) -> None:
    app_env = str(getattr(settings, "app_env", "dev") or "dev").strip().lower()
    enforce_envs = _csv_to_set(getattr(settings, "db_enforce_postgres_envs", ""))
    database_url = str(getattr(settings, "database_url", "") or "")
    if app_env in enforce_envs and _is_sqlite_url(database_url):
        raise RuntimeError(
            f"DATABASE_URL backend sqlite is forbidden in APP_ENV={app_env!r}. "
            "Please use PostgreSQL in strict environments."
        )


def enforce_migration_strict_mode(settings: Any) -> bool:
    app_env = str(getattr(settings, "app_env", "dev") or "dev").strip().lower()
    strict_envs = _csv_to_set(getattr(settings, "db_disable_fallback_envs", ""))
    return app_env in strict_envs

