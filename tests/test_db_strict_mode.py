from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app import cli
from app.db.guards import ensure_db_backend_allowed


def test_db_backend_guard_blocks_sqlite_in_prod():
    settings = SimpleNamespace(
        app_env="prod",
        db_enforce_postgres_envs="stage,prod",
        database_url="sqlite:///./data/test.db",
    )
    with pytest.raises(RuntimeError):
        ensure_db_backend_allowed(settings)


def test_db_backend_guard_allows_sqlite_in_dev():
    settings = SimpleNamespace(
        app_env="dev",
        db_enforce_postgres_envs="stage,prod",
        database_url="sqlite:///./data/test.db",
    )
    ensure_db_backend_allowed(settings)


def test_initialize_database_strict_mode_disables_fallback_and_stamp(tmp_path: Path):
    alembic_ini = tmp_path / "alembic.ini"
    migrations = tmp_path / "app" / "db" / "migrations"
    alembic_ini.write_text("[alembic]\n", encoding="utf-8")
    migrations.mkdir(parents=True)

    settings = SimpleNamespace(
        database_url="sqlite:///./data/test.db",
        app_env="prod",
        db_disable_fallback_envs="stage,prod",
    )
    runtime_env = {"DATABASE_URL": settings.database_url}
    fallback_called = {"called": False}
    commands: list[list[str]] = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1, stdout="", stderr="migration failed")

    def fake_fallback(_: str):
        fallback_called["called"] = True

    old_ini = cli.ALEMBIC_INI_PATH
    old_migrations = cli.MIGRATIONS_PATH
    try:
        cli.ALEMBIC_INI_PATH = alembic_ini
        cli.MIGRATIONS_PATH = migrations
        with pytest.raises(RuntimeError):
            cli.initialize_database(settings, runtime_env, runner=fake_run, fallback_fn=fake_fallback)
    finally:
        cli.ALEMBIC_INI_PATH = old_ini
        cli.MIGRATIONS_PATH = old_migrations

    assert commands
    assert commands[0][-2:] == ["upgrade", "head"]
    assert fallback_called["called"] is False

