from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import signal
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from types import FrameType
from typing import Callable

import httpx
import websockets
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.db.guards import enforce_migration_strict_mode, ensure_db_backend_allowed
from app.logging import logger, setup_logging

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"
ALEMBIC_INI_PATH = REPO_ROOT / "alembic.ini"
MIGRATIONS_PATH = REPO_ROOT / "app" / "db" / "migrations"
BUSINESS_TABLES = ("ohlcv", "market_metrics", "alert_events", "worker_status")


def load_settings_from_env():
    load_dotenv(ENV_PATH, override=False)
    from app.config import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    ensure_db_backend_allowed(settings)
    os.environ["DATABASE_URL"] = settings.database_url
    return settings


def mask_database_url(url: str) -> str:
    try:
        parsed = make_url(url)
    except Exception:
        return url
    if parsed.password:
        parsed = parsed.set(password="***")
    return str(parsed)


def parse_symbols(symbols_value: str | None, default_symbols: list[str]) -> list[str]:
    if not symbols_value:
        return default_symbols
    return [item.strip().upper() for item in symbols_value.split(",") if item.strip()]


def build_api_command(host: str, port: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        host,
        "--port",
        str(port),
    ]


def build_worker_command() -> list[str]:
    return [sys.executable, "-m", "app.worker.main"]


def build_alembic_upgrade_command() -> list[str]:
    return [sys.executable, "-m", "alembic", "upgrade", "head"]


def build_alembic_stamp_command() -> list[str]:
    return [sys.executable, "-m", "alembic", "stamp", "head"]


def build_runtime_env(settings, backfill_days: int) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = settings.database_url
    env["BACKFILL_DAYS_DEFAULT"] = str(backfill_days)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def fallback_create_all(database_url: str) -> None:
    os.environ["DATABASE_URL"] = database_url
    from app.db.session import Base, engine

    Base.metadata.create_all(bind=engine)


def _is_sqlite_url(database_url: str) -> bool:
    try:
        parsed = make_url(database_url)
        return parsed.get_backend_name() == "sqlite"
    except Exception:
        return database_url.startswith("sqlite")


def _resolve_sqlite_path(database_url: str) -> str:
    parsed = make_url(database_url)
    database = parsed.database or ""
    if database in ("", ":memory:"):
        return database
    path = Path(database)
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return str(path)


def _inspect_sqlite_schema(database_url: str) -> tuple[bool, bool]:
    path = _resolve_sqlite_path(database_url)
    if path in ("", ":memory:"):
        return False, False

    business_exists = False
    alembic_version_exists = False
    conn = sqlite3.connect(path, timeout=1.0)
    try:
        conn.execute("PRAGMA busy_timeout = 1000")
        placeholders = ",".join("?" for _ in BUSINESS_TABLES)
        rows = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
            BUSINESS_TABLES,
        ).fetchall()
        business_exists = len(rows) > 0

        alembic_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='alembic_version'"
        ).fetchall()
        alembic_version_exists = len(alembic_rows) > 0
    finally:
        conn.close()
    return business_exists, alembic_version_exists


def _looks_like_existing_table_error(error_text: str) -> bool:
    lowered = (error_text or "").lower()
    return "already exists" in lowered and "table" in lowered


def initialize_database(
    settings,
    runtime_env: dict[str, str],
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    fallback_fn: Callable[[str], None] = fallback_create_all,
) -> str:
    strict_mode = enforce_migration_strict_mode(settings)
    allow_fallback_raw = getattr(settings, "db_allow_fallback_create_all", None)
    allow_fallback = True if allow_fallback_raw is None else bool(allow_fallback_raw)
    if ALEMBIC_INI_PATH.exists() and MIGRATIONS_PATH.exists():
        cmd = build_alembic_upgrade_command()
        result = runner(
            cmd,
            cwd=str(REPO_ROOT),
            env=runtime_env,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            logger.info("DB init via alembic upgrade head")
            if result.stdout.strip():
                logger.debug("alembic stdout: %s", result.stdout.strip())
            return "alembic"

        stderr_text = result.stderr.strip()
        stdout_text = result.stdout.strip()
        error_text = "\n".join(item for item in [stderr_text, stdout_text] if item)
        logger.warning("Alembic upgrade failed (exit=%s). stderr=%s", result.returncode, stderr_text)

        if strict_mode or not allow_fallback:
            raise RuntimeError(
                "Alembic upgrade failed. Fallback/stamp/create_all are disabled. "
                "Run `alembic upgrade head` before starting services or set DB_ALLOW_FALLBACK_CREATE_ALL=true."
            )

        if _is_sqlite_url(settings.database_url):
            try:
                business_exists, alembic_version_exists = _inspect_sqlite_schema(settings.database_url)
            except Exception as exc:
                logger.warning("Could not inspect SQLite schema: %s", exc)
                business_exists, alembic_version_exists = False, False
            if alembic_version_exists:
                logger.warning(
                    "SQLite DB already has alembic_version. Assuming migrated state and continue startup."
                )
                return "already_migrated"

            if business_exists and _looks_like_existing_table_error(error_text):
                stamp_cmd = build_alembic_stamp_command()
                stamp_result = runner(
                    stamp_cmd,
                    cwd=str(REPO_ROOT),
                    env=runtime_env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if stamp_result.returncode == 0:
                    logger.warning(
                        "Detected legacy SQLite schema without alembic_version. "
                        "Applied `alembic stamp head` compatibility mode. "
                        "For full constraint consistency, consider deleting the DB and rebuilding."
                    )
                    return "stamped"
                logger.warning(
                    "Alembic stamp head failed (exit=%s). stderr=%s",
                    stamp_result.returncode,
                    stamp_result.stderr.strip(),
                )
    else:
        if strict_mode or not allow_fallback:
            raise RuntimeError(
                "Alembic metadata is unavailable. Refusing to run fallback/create_all. "
                "Ensure alembic.ini and migrations are present, or set DB_ALLOW_FALLBACK_CREATE_ALL=true."
            )
        logger.warning("Alembic unavailable (missing alembic.ini or migrations directory)")

    fallback_fn(settings.database_url)
    logger.warning("DB init fallback mode may miss constraints")
    return "fallback"


def start_process(command: list[str], runtime_env: dict[str, str], name: str) -> subprocess.Popen[str]:
    kwargs: dict = {
        "cwd": str(REPO_ROOT),
        "env": runtime_env,
        "text": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        kwargs["start_new_session"] = True
    process = subprocess.Popen(command, **kwargs)
    logger.info("%s started pid=%s", name, process.pid)
    return process


def stop_process(process: subprocess.Popen[str], name: str, timeout: int = 10) -> None:
    if process.poll() is not None:
        return

    try:
        if os.name == "nt":
            if hasattr(signal, "CTRL_BREAK_EVENT"):
                try:
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                    time.sleep(0.4)
                except Exception:
                    pass
            if process.poll() is None:
                process.terminate()
        else:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except Exception:
                process.terminate()
    except Exception:
        pass

    try:
        process.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        logger.warning("%s did not stop gracefully, killing", name)

    try:
        if os.name == "nt":
            process.kill()
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except Exception:
                process.kill()
    except Exception:
        pass
    try:
        process.wait(timeout=3)
    except Exception:
        pass


def probe_http_ready(host: str, port: int, timeout_per_request: float = 1.0) -> bool:
    import urllib.request
    import urllib.error
    
    urls = [f"http://{host}:{port}/api/health", f"http://{host}:{port}/"]
    for url in urls:
        try:
            # Bypass system proxies to ensure we hit localhost
            proxy_handler = urllib.request.ProxyHandler({})
            opener = urllib.request.build_opener(proxy_handler)
            req = urllib.request.Request(url)
            with opener.open(req, timeout=timeout_per_request) as response:
                if response.getcode() < 500:
                    return True
        except urllib.error.HTTPError as e:
            if e.code < 500:
                return True
        except Exception:
            continue
    return False


def wait_for_service_ready(host: str, port: int, timeout: int = 60, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if probe_http_ready(host, port, timeout_per_request=1.0):
            return True
        time.sleep(interval)
    return False


def maybe_open_browser(host: str, port: int) -> None:
    if wait_for_service_ready(host, port):
        url = f"http://{host}:{port}/"
        logger.info("Service is ready. Opening browser at %s", url)
        try:
            if os.name == 'nt':
                os.startfile(url)
            else:
                webbrowser.open(url)
        except Exception as e:
            logger.error("Failed to open browser automatically: %s", e)
            webbrowser.open(url)
    else:
        logger.warning("Service readiness timed out; open manually at http://%s:%d", host, port)


def log_latest_ingest_status(settings) -> None:
    try:
        from app.db.repository import get_latest_ohlcv_ts
        from app.db.session import SessionLocal
    except Exception as exc:
        logger.warning("Could not read ingest timestamps: %s", exc)
        return

    with SessionLocal() as session:
        latest_1m = None
        latest_10m = None
        for symbol in settings.watchlist_symbols:
            ts_1m = get_latest_ohlcv_ts(session, symbol, "1m")
            ts_10m = get_latest_ohlcv_ts(session, symbol, "10m")
            if ts_1m and (latest_1m is None or ts_1m > latest_1m):
                latest_1m = ts_1m
            if ts_10m and (latest_10m is None or ts_10m > latest_10m):
                latest_10m = ts_10m
    logger.info("Latest ingest ts: 1m=%s 10m=%s", latest_1m, latest_10m)


def command_up(args: argparse.Namespace) -> int:
    settings = load_settings_from_env()
    setup_logging()

    runtime_env = build_runtime_env(settings, backfill_days=args.backfill_days)
    logger.info("Using DATABASE_URL=%s", mask_database_url(settings.database_url))

    if args.db_init:
        try:
            initialize_database(settings, runtime_env)
        except RuntimeError as exc:
            logger.error("Database initialization failed: %s", exc)
            return 1

    api_command = build_api_command(host=args.host, port=args.port)
    worker_command = build_worker_command()

    api_process = start_process(api_command, runtime_env, "API")
    worker_process = start_process(worker_command, runtime_env, "Worker")

    logger.info("Dashboard: http://%s:%d", args.host, args.port)
    logger.info("Health: http://%s:%d/api/health", args.host, args.port)
    log_latest_ingest_status(settings)

    if args.open_browser:
        threading.Thread(target=maybe_open_browser, args=(args.host, args.port), daemon=True).start()

    stop_event = threading.Event()
    previous_handlers: dict[signal.Signals, Callable | int | None] = {}

    def _handle_signal(_: int, __: FrameType | None) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_signal)
        except Exception:
            continue

    exit_code = 0
    try:
        while not stop_event.is_set():
            api_exit = api_process.poll()
            worker_exit = worker_process.poll()
            if api_exit is not None:
                logger.error("API process exited unexpectedly: %s", api_exit)
                exit_code = api_exit or 1
                stop_event.set()
                break
            if worker_exit is not None:
                logger.error("Worker process exited unexpectedly: %s", worker_exit)
                exit_code = worker_exit or 1
                stop_event.set()
                break
            time.sleep(0.5)
    finally:
        stop_process(api_process, "API")
        stop_process(worker_process, "Worker")
        for sig, handler in previous_handlers.items():
            try:
                signal.signal(sig, handler)
            except Exception:
                pass
    return 0 if exit_code == 0 else 1


def command_backfill(args: argparse.Namespace) -> int:
    settings = load_settings_from_env()
    setup_logging()

    if args.days < 0:
        logger.error("--days must be >= 0")
        return 2

    symbols = parse_symbols(args.symbols, settings.watchlist_symbols)
    from app.backfill_service import run_backfill

    asyncio.run(run_backfill(days=args.days, symbols=symbols, settings=settings))
    return 0


def _port_in_use(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


async def _ws_probe(settings, timeout: float) -> tuple[bool, str]:
    if not settings.watchlist_symbols:
        return False, "watchlist is empty"
    stream = f"{settings.watchlist_symbols[0].lower()}@kline_1m"
    ws_url = f"{settings.binance_ws_url}?streams={stream}"
    try:
        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
            _ = await asyncio.wait_for(ws.recv(), timeout=timeout)
            return True, "received ws payload"
    except Exception as exc:
        return False, str(exc)


def command_doctor(args: argparse.Namespace) -> int:
    settings = load_settings_from_env()
    setup_logging()
    results: list[tuple[str, str, str]] = []

    def add_result(name: str, status: str, detail: str) -> None:
        results.append((name, status, detail))
        logger.info("[%s] %s: %s", status, name, detail)

    try:
        db_engine = create_engine(settings.database_url, future=True, pool_pre_ping=True)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        add_result("database", "PASS", f"connected to {mask_database_url(settings.database_url)}")
    except Exception as exc:
        add_result("database", "FAIL", str(exc))

    if _port_in_use(args.host, args.port):
        add_result("port", "WARN", f"{args.host}:{args.port} is occupied")
    else:
        add_result("port", "PASS", f"{args.host}:{args.port} is available")

    try:
        with httpx.Client(timeout=args.timeout) as client:
            resp = client.get(f"http://{args.host}:{args.port}/api/health")
        if resp.status_code == 200:
            add_result("api-health", "PASS", "reachable")
        else:
            add_result("api-health", "WARN", f"HTTP {resp.status_code}")
    except Exception as exc:
        add_result("api-health", "WARN", str(exc))

    ws_ok, ws_message = asyncio.run(_ws_probe(settings, timeout=args.timeout))
    add_result("ws-probe", "PASS" if ws_ok else "WARN", ws_message)

    try:
        from app.db.repository import get_worker_last_seen
        from app.db.session import SessionLocal

        with SessionLocal() as session:
            last_seen = get_worker_last_seen(session, worker_id=settings.worker_id)
        if last_seen is None:
            add_result("worker-heartbeat", "WARN", "no heartbeat record")
        else:
            age_seconds = (time.time() - last_seen.timestamp())
            limit = settings.worker_heartbeat_seconds * 3
            if age_seconds <= limit:
                add_result("worker-heartbeat", "PASS", f"last seen {age_seconds:.1f}s ago")
            else:
                add_result("worker-heartbeat", "WARN", f"stale heartbeat ({age_seconds:.1f}s)")
    except Exception as exc:
        add_result("worker-heartbeat", "WARN", str(exc))

    return 1 if any(status == "FAIL" for _, status, _ in results) else 0


def command_test_telegram(args: argparse.Namespace) -> int:
    settings = load_settings_from_env()
    setup_logging()
    
    from app.alerts.telegram import TelegramClient
    telegram = TelegramClient(settings)
    
    if not telegram.enabled or not telegram.bot_token or not telegram.chat_id:
        logger.error("Telegram is not properly configured. Please check TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        return 1

    logger.info("Sending test message to Telegram...")
    success = asyncio.run(telegram.send_text("✅ Crypto Sentinel Telegram Bot 接入测试成功！系统运行正常。"))
    
    if success:
        logger.info("Test message sent successfully! Please check your Telegram client.")
        return 0
    else:
        logger.error("Failed to send test message. Please verify your bot token, chat ID and network connection.")
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crypto Sentinel CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    up_parser = subparsers.add_parser("up", help="Start API + Worker")
    up_parser.add_argument("--open-browser", action="store_true", help="Open dashboard after API becomes healthy")
    up_parser.add_argument("--host", type=str, default="127.0.0.1", help="API host (default: 127.0.0.1)")
    up_parser.add_argument("--port", type=int, default=8000, help="API port (default: 8000)")
    up_parser.add_argument("--backfill-days", type=int, default=0, help="Worker startup backfill days (default: 0)")
    up_parser.set_defaults(db_init=True)
    up_parser.add_argument("--db-init", dest="db_init", action="store_true", help="Run database initialization before start")
    up_parser.add_argument("--no-db-init", dest="db_init", action="store_false", help="Skip database initialization")

    backfill_parser = subparsers.add_parser("backfill", help="Backfill 1m data and rebuild 10m")
    backfill_parser.add_argument("--days", type=int, required=True, help="Number of days to backfill")
    backfill_parser.add_argument("--symbols", type=str, default="", help="CSV symbols (default from WATCHLIST)")

    doctor_parser = subparsers.add_parser("doctor", help="Run local health diagnostics")
    doctor_parser.add_argument("--host", type=str, default="127.0.0.1", help="API host for checks")
    doctor_parser.add_argument("--port", type=int, default=8000, help="API port for checks")
    doctor_parser.add_argument("--timeout", type=float, default=5.0, help="Timeout seconds for network checks")
    
    test_telegram_parser = subparsers.add_parser("test-telegram", help="Test Telegram bot integration by sending a message")
    
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "up":
        raise SystemExit(command_up(args))
    if args.command == "backfill":
        raise SystemExit(command_backfill(args))
    if args.command == "doctor":
        raise SystemExit(command_doctor(args))
    if args.command == "test-telegram":
        raise SystemExit(command_test_telegram(args))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
