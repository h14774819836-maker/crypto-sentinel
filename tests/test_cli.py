from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import cli


def test_up_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["up"])
    assert args.host == "127.0.0.1"
    assert args.port == 8000
    assert args.db_init is True
    assert args.backfill_days == 0
    assert args.multi_worker is True


def test_up_single_worker_flag_overrides_default():
    parser = cli.build_parser()
    args = parser.parse_args(["up", "--single-worker"])
    assert args.multi_worker is False


def test_down_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["down"])
    assert args.reason == "manual_stop"
    assert args.requested_by == "cli"
    assert args.delay_seconds == 0.0
    assert args.wait_seconds == 15.0
    assert args.force is True


def test_commands_use_sys_executable():
    api_cmd = cli.build_api_command("127.0.0.1", 8000)
    worker_cmd = cli.build_worker_command()
    alembic_cmd = cli.build_alembic_upgrade_command()
    assert api_cmd[0] == sys.executable
    assert worker_cmd[0] == sys.executable
    assert alembic_cmd[0] == sys.executable


def test_initialize_database_prefers_alembic(tmp_path: Path):
    alembic_ini = tmp_path / "alembic.ini"
    migrations = tmp_path / "app" / "db" / "migrations"
    alembic_ini.write_text("[alembic]\n")
    migrations.mkdir(parents=True)

    settings = SimpleNamespace(database_url="sqlite:///./data/test.db")
    runtime_env = {"DATABASE_URL": settings.database_url}
    captured: dict = {}
    fallback_called = {"called": False}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def fake_fallback(_: str):
        fallback_called["called"] = True

    old_ini = cli.ALEMBIC_INI_PATH
    old_migrations = cli.MIGRATIONS_PATH
    cli.ALEMBIC_INI_PATH = alembic_ini
    cli.MIGRATIONS_PATH = migrations
    try:
        mode = cli.initialize_database(settings, runtime_env, runner=fake_run, fallback_fn=fake_fallback)
    finally:
        cli.ALEMBIC_INI_PATH = old_ini
        cli.MIGRATIONS_PATH = old_migrations

    assert mode == "alembic"
    assert captured["command"] == [sys.executable, "-m", "alembic", "upgrade", "head"]
    assert captured["kwargs"]["env"]["DATABASE_URL"] == settings.database_url
    assert fallback_called["called"] is False


def test_initialize_database_fallback_logs_warning(caplog):
    settings = SimpleNamespace(database_url="sqlite:///./data/test.db")
    runtime_env = {"DATABASE_URL": settings.database_url}
    fallback_called = {"called": False}

    def fake_fallback(_: str):
        fallback_called["called"] = True

    old_ini = cli.ALEMBIC_INI_PATH
    old_migrations = cli.MIGRATIONS_PATH
    cli.ALEMBIC_INI_PATH = Path("missing-alembic.ini")
    cli.MIGRATIONS_PATH = Path("missing-migrations")
    try:
        mode = cli.initialize_database(settings, runtime_env, fallback_fn=fake_fallback)
    finally:
        cli.ALEMBIC_INI_PATH = old_ini
        cli.MIGRATIONS_PATH = old_migrations

    assert mode == "fallback"
    assert fallback_called["called"] is True
    assert "fallback mode may miss constraints" in caplog.text


def test_initialize_database_stamps_legacy_sqlite_schema(tmp_path: Path):
    alembic_ini = tmp_path / "alembic.ini"
    migrations = tmp_path / "app" / "db" / "migrations"
    alembic_ini.write_text("[alembic]\n", encoding="utf-8")
    migrations.mkdir(parents=True)

    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_file))
    try:
        conn.execute("CREATE TABLE ohlcv (id INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()

    settings = SimpleNamespace(database_url=f"sqlite:///{db_file}")
    runtime_env = {"DATABASE_URL": settings.database_url}
    fallback_called = {"called": False}
    commands: list[list[str]] = []

    def fake_run(command, **kwargs):
        commands.append(command)
        if command[-2:] == ["upgrade", "head"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="sqlite3.OperationalError: table ohlcv already exists")
        if command[-2:] == ["stamp", "head"]:
            return SimpleNamespace(returncode=0, stdout="stamped", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="unexpected")

    def fake_fallback(_: str):
        fallback_called["called"] = True

    old_ini = cli.ALEMBIC_INI_PATH
    old_migrations = cli.MIGRATIONS_PATH
    try:
        cli.ALEMBIC_INI_PATH = alembic_ini
        cli.MIGRATIONS_PATH = migrations
        mode = cli.initialize_database(settings, runtime_env, runner=fake_run, fallback_fn=fake_fallback)
    finally:
        cli.ALEMBIC_INI_PATH = old_ini
        cli.MIGRATIONS_PATH = old_migrations

    assert mode == "stamped"
    assert commands[0][-2:] == ["upgrade", "head"]
    assert commands[1][-2:] == ["stamp", "head"]
    assert fallback_called["called"] is False


def test_wait_for_service_ready_retries_until_success(monkeypatch):
    attempts = {"count": 0}

    def fake_probe(*_args, **_kwargs):
        attempts["count"] += 1
        return attempts["count"] >= 3

    monkeypatch.setattr(cli, "probe_http_ready", fake_probe)
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)

    ready = cli.wait_for_service_ready("127.0.0.1", 8000, timeout=5, interval=0.1)
    assert ready is True
    assert attempts["count"] >= 3


def test_maybe_open_browser_after_retry_success(monkeypatch):
    opened: list[str] = []

    monkeypatch.setattr(cli, "wait_for_service_ready", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(cli.webbrowser, "open", lambda url: opened.append(url))
    if hasattr(cli.os, "startfile"):
        monkeypatch.setattr(cli.os, "startfile", lambda url: opened.append(url))

    cli.maybe_open_browser("127.0.0.1", 8000)
    assert opened == ["http://127.0.0.1:8000/"]


def test_up_handles_interrupt_and_stops_both_processes(monkeypatch):
    settings = SimpleNamespace(database_url="sqlite:///./data/test.db", watchlist_symbols=["BTCUSDT"])

    class FakeProc:
        def __init__(self):
            self.pid = 1234

        def poll(self):
            return None

    started_commands: list[list[str]] = []
    runtime_env_values: list[dict[str, str]] = []
    stopped_names: list[str] = []
    handlers = {}
    processes = [FakeProc(), FakeProc()]

    def fake_start_process(command, runtime_env, name):
        started_commands.append(command)
        runtime_env_values.append(runtime_env)
        return processes.pop(0)

    def fake_stop_process(process, name, timeout=10):
        stopped_names.append(name)

    def fake_signal(sig, handler):
        handlers[sig] = handler
        return None

    def fake_sleep(_):
        if cli.signal.SIGINT in handlers:
            handlers[cli.signal.SIGINT](int(cli.signal.SIGINT), None)

    monkeypatch.setattr(cli, "load_settings_from_env", lambda: settings)
    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "initialize_database", lambda *a, **k: "alembic")
    monkeypatch.setattr(cli, "start_process", fake_start_process)
    monkeypatch.setattr(cli, "stop_process", fake_stop_process)
    monkeypatch.setattr(cli, "log_latest_ingest_status", lambda _: None)
    monkeypatch.setattr(cli, "clear_runtime_stop_request", lambda: None)
    monkeypatch.setattr(cli, "write_runtime_state", lambda payload: None)
    monkeypatch.setattr(cli, "clear_runtime_state", lambda: None)
    monkeypatch.setattr(cli.signal, "signal", fake_signal)
    monkeypatch.setattr(cli.signal, "getsignal", lambda _: None)
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)

    args = argparse.Namespace(
        command="up",
        open_browser=False,
        host="127.0.0.1",
        port=8000,
        backfill_days=0,
        db_init=True,
        multi_worker=False,
    )

    exit_code = cli.command_up(args)
    assert exit_code == 0
    assert started_commands[0][0] == sys.executable
    assert started_commands[1][0] == sys.executable
    assert runtime_env_values[0]["DATABASE_URL"] == settings.database_url
    assert runtime_env_values[1]["DATABASE_URL"] == settings.database_url
    assert runtime_env_values[0]["BACKFILL_DAYS_DEFAULT"] == "0"
    assert set(stopped_names) == {"API", "Worker"}


def test_build_api_env_for_multi_worker_sets_core_identity_and_metrics_map():
    settings = SimpleNamespace(
        database_url="sqlite:///./data/test.db",
        ai_manual_preflight_mode="stale_guarded",
    )

    env = cli.build_api_env_for_multi_worker(
        settings,
        backfill_days=1,
        worker_id="worker-core-1",
        redis_url="redis://localhost:6379/0",
    )

    assert env["DATABASE_URL"] == settings.database_url
    assert env["BACKFILL_DAYS_DEFAULT"] == "1"
    assert env["WORKER_ROLE"] == "core"
    assert env["WORKER_ID"] == "worker-core-1"
    assert env["REDIS_URL"] == "redis://localhost:6379/0"
    assert env["LLM_HOT_RELOAD_USE_REDIS"] == "true"
    assert env["AI_MANUAL_PREFLIGHT_MODE"] == "stale_guarded"
    assert json.loads(env["OPS_JOB_METRICS_FILES_JSON"]) == {
        "core": "data/job_metrics_core.json",
        "ai": "data/job_metrics_ai.json",
    }


def test_down_requests_graceful_shutdown(monkeypatch):
    runtime_state = {
        "supervisor_pid": 999,
        "children": {
            "api": {"pid": 1001, "label": "API"},
            "worker": {"pid": 1002, "label": "Worker"},
        },
    }
    requests: list[dict[str, object]] = []
    cleared: list[bool] = []

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "read_runtime_state", lambda: runtime_state)
    monkeypatch.setattr(cli, "extract_runtime_pids", lambda payload: [999, 1001, 1002])
    monkeypatch.setattr(
        cli,
        "request_runtime_stop",
        lambda **kwargs: requests.append(kwargs) or {
            "requested_by": kwargs["requested_by"],
            "reason": kwargs["reason"],
            "delay_seconds": kwargs["delay_seconds"],
        },
    )
    monkeypatch.setattr(cli, "_wait_for_runtime_shutdown", lambda pids, timeout: True)
    monkeypatch.setattr(cli, "clear_runtime_state", lambda: cleared.append(True))

    args = argparse.Namespace(
        reason="script_stop",
        requested_by="test",
        delay_seconds=0.75,
        wait_seconds=5.0,
        force=True,
    )
    exit_code = cli.command_down(args)

    assert exit_code == 0
    assert requests == [{
        "reason": "script_stop",
        "requested_by": "test",
        "delay_seconds": 0.75,
    }]
    assert cleared == [True]


def test_down_forces_remaining_processes_after_timeout(monkeypatch):
    runtime_state = {
        "supervisor_pid": 999,
        "children": {
            "api": {"pid": 1001, "label": "API"},
            "worker": {"pid": 1002, "label": "Worker"},
        },
    }
    waits = iter([False, True])
    forced: list[dict[str, object]] = []
    cleared: list[bool] = []

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "read_runtime_state", lambda: runtime_state)
    monkeypatch.setattr(cli, "extract_runtime_pids", lambda payload: [999, 1001, 1002])
    monkeypatch.setattr(
        cli,
        "request_runtime_stop",
        lambda **kwargs: {
            "requested_by": kwargs["requested_by"],
            "reason": kwargs["reason"],
            "delay_seconds": kwargs["delay_seconds"],
        },
    )
    monkeypatch.setattr(cli, "_wait_for_runtime_shutdown", lambda pids, timeout: next(waits))
    monkeypatch.setattr(cli, "_force_stop_local_runtime", lambda payload: forced.append(payload))
    monkeypatch.setattr(cli, "clear_runtime_state", lambda: cleared.append(True))

    args = argparse.Namespace(
        reason="manual_stop",
        requested_by="test",
        delay_seconds=0.0,
        wait_seconds=0.1,
        force=True,
    )
    exit_code = cli.command_down(args)

    assert exit_code == 0
    assert forced == [runtime_state]
    assert cleared == [True]
