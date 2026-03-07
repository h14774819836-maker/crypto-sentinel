@echo off
setlocal EnableExtensions
goto :main

:use_docker_stack
if /I "%~1"=="docker" exit /b 0
if /I "%~1"=="--docker" exit /b 0
if /I "%~1"=="compose" exit /b 0
exit /b 1

:use_stop_action
if /I "%~1"=="stop" exit /b 0
if /I "%~1"=="down" exit /b 0
if /I "%~1"=="exit" exit /b 0
exit /b 1

:use_single_worker_action
if /I "%~1"=="single" exit /b 0
if /I "%~1"=="--single" exit /b 0
if /I "%~1"=="single-worker" exit /b 0
if /I "%~1"=="--single-worker" exit /b 0
exit /b 1

:detect_compose_command
set "COMPOSE_CMD="
docker compose version >nul 2>nul
if not errorlevel 1 (
    set "COMPOSE_CMD=docker compose"
    exit /b 0
)
docker-compose version >nul 2>nul
if not errorlevel 1 (
    set "COMPOSE_CMD=docker-compose"
    exit /b 0
)
exit /b 1

:redis_ping
call python -c "import os, socket, urllib.parse; u=urllib.parse.urlparse(os.environ.get('REDIS_URL', 'redis://localhost:6379/0')); s=socket.create_connection((u.hostname or 'localhost', u.port or 6379), 1); s.close()" >nul 2>nul
exit /b %errorlevel%

:wait_for_redis
set "REDIS_WAIT_ATTEMPTS=%~1"
if "%REDIS_WAIT_ATTEMPTS%"=="" set "REDIS_WAIT_ATTEMPTS=20"
for /L %%I in (1,1,%REDIS_WAIT_ATTEMPTS%) do (
    call :redis_ping
    if not errorlevel 1 exit /b 0
    timeout /t 1 /nobreak >nul
)
exit /b 1

:ensure_redis
call :redis_ping
if not errorlevel 1 (
    echo [INFO] Redis is already available.
    exit /b 0
)

echo [CHECK] Redis is not reachable at %REDIS_URL%

set "REDIS_COMPOSE_OK=0"
if defined COMPOSE_CMD (
    echo [SETUP] Trying to start Redis via %COMPOSE_CMD% up -d redis...
    call %COMPOSE_CMD% up -d redis
    if not errorlevel 1 set "REDIS_COMPOSE_OK=1"
)
if "%REDIS_COMPOSE_OK%"=="1" (
    call :wait_for_redis 20
    if not errorlevel 1 (
        echo [INFO] Redis started via docker compose.
        exit /b 0
    )
)

set "REDIS_LOCAL_OK=0"
where /q redis-server
if not errorlevel 1 (
    echo [SETUP] Trying to start local redis-server...
    start "Crypto Sentinel Redis" /min redis-server --appendonly yes
    set "REDIS_LOCAL_OK=1"
)
if "%REDIS_LOCAL_OK%"=="1" (
    call :wait_for_redis 20
    if not errorlevel 1 (
        echo [INFO] Redis started via local redis-server.
        exit /b 0
    )
)

echo [WARN] Could not start Redis automatically.
if defined COMPOSE_CMD echo         Docker mode is available via: run.bat docker
echo         Or install/start a local redis-server for local multi-worker mode.
exit /b 1

:docker_prepare_multi_worker
if not defined COMPOSE_CMD exit /b 0
echo [SETUP] Preparing Docker multi-worker dependencies...
call %COMPOSE_CMD% up -d redis db
if errorlevel 1 exit /b 1
echo [SETUP] Clearing stale worker identity leases from Redis...
call %COMPOSE_CMD% exec -T redis redis-cli DEL worker:heartbeat:worker-core-1 worker:heartbeat:worker-ai-1 >nul 2>nul
exit /b 0

:wait_for_http
set "WAIT_HTTP_URL=%~1"
set "WAIT_HTTP_ATTEMPTS=%~2"
if "%WAIT_HTTP_URL%"=="" set "WAIT_HTTP_URL=http://127.0.0.1:8000/"
if "%WAIT_HTTP_ATTEMPTS%"=="" set "WAIT_HTTP_ATTEMPTS=60"
for /L %%I in (1,1,%WAIT_HTTP_ATTEMPTS%) do (
    powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; try { $resp = Invoke-WebRequest -UseBasicParsing -Uri '%WAIT_HTTP_URL%' -TimeoutSec 2; if ($resp.StatusCode -lt 500) { exit 0 } else { exit 1 } } catch { exit 1 }" >nul 2>nul
    if not errorlevel 1 exit /b 0
    timeout /t 1 /nobreak >nul
)
exit /b 1

:open_browser
set "BROWSER_URL=%~1"
if "%BROWSER_URL%"=="" set "BROWSER_URL=http://127.0.0.1:8000/"
start "" "%BROWSER_URL%"
exit /b 0

:prompt_attach_logs
if not defined COMPOSE_CMD exit /b 0
echo.
choice /C YN /N /M "Attach Docker logs now? [Y/N]: "
if errorlevel 2 exit /b 0
echo.
echo [INFO] Attaching logs. Press Ctrl+C to stop viewing logs.
call %COMPOSE_CMD% logs -f api worker worker_ai
exit /b %errorlevel%

:main
REM Use standard code page
chcp 65001
title Crypto Sentinel

cd /d "%~dp0"

echo.
echo [Crypto Sentinel V0.2] AI Crypto Analysis System
echo.

REM --- Check if venv exists ---
if exist ".venv\Scripts\python.exe" (
    set "PYTHON=.venv\Scripts\python.exe"
    goto :venv_ready
)

REM --- Resolve Python (Only if venv missing) ---
set "PYTHON="
where /q python && set "PYTHON=python"
if "%PYTHON%"=="" (
    where /q py && set "PYTHON=py -3.11"
)
if "%PYTHON%"=="" (
    echo [ERROR] Python 3.11+ is required.
    echo         Download from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM --- Check Python Version ---
call %PYTHON% -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)"
if errorlevel 1 (
    echo [ERROR] Python 3.11+ is required.
    echo         Download from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM --- Setup venv (first run only) ---
echo [SETUP] Creating virtual environment...
call %PYTHON% -m venv .venv
if errorlevel 1 (
    echo [ERROR] Failed to create venv
    pause
    exit /b 1
)
set "PYTHON=.venv\Scripts\python.exe"
set "NEED_INSTALL=1"

:venv_ready
call ".venv\Scripts\activate.bat"

call :use_stop_action "%~1"
if not errorlevel 1 (
    echo [STOP] Requesting local runtime shutdown...
    call python -m app.cli down --reason script_stop --requested-by run.bat
    set "STOP_EXIT=%errorlevel%"
    call :detect_compose_command
    if defined COMPOSE_CMD (
        echo [STOP] Bringing down Docker stack if running...
        call %COMPOSE_CMD% down >nul 2>nul
    )
    exit /b %STOP_EXIT%
)

REM --- Check for update flag ---
if "%1"=="update" set "NEED_INSTALL=1"
if "%1"=="--update" set "NEED_INSTALL=1"
if "%1"=="install" set "NEED_INSTALL=1"

REM --- Install/Update Dependencies (Only if needed) ---
if defined NEED_INSTALL (
    echo [CHECK] Installing/Updating dependencies...
    call python -m pip install --quiet -e .[dev]
    if errorlevel 1 (
        echo [ERROR] pip install failed
        pause
        exit /b 1
    )
) else (
    echo [INFO] Skipping dependency check. Run 'run.bat update' to update.
)

REM --- Create .env if missing ---
if not exist ".env" (
    copy /Y ".env.example" ".env" > NUL
    echo [SETUP] Created .env from .env.example
    echo         Please edit .env to add your DEEPSEEK_API_KEY
    echo.
)

set "REDIS_URL=redis://localhost:6379/0"
for /f "usebackq tokens=1,* delims==" %%A in (`findstr /R /B /I "REDIS_URL=" ".env" 2^>nul`) do (
    set "REDIS_URL=%%B"
)

call :detect_compose_command
call :use_single_worker_action "%~1"
set "USE_SINGLE_MODE=0"
if not errorlevel 1 set "USE_SINGLE_MODE=1"
set "EXPLICIT_DOCKER_MODE=0"
call :use_docker_stack "%~1"
if not errorlevel 1 set "EXPLICIT_DOCKER_MODE=1"
set "USE_DOCKER_MODE=%EXPLICIT_DOCKER_MODE%"
if "%~1"=="" (
    if defined COMPOSE_CMD set "USE_DOCKER_MODE=1"
)
if "%USE_DOCKER_MODE%"=="1" (
    if defined COMPOSE_CMD (
        if "%EXPLICIT_DOCKER_MODE%"=="1" (
            echo [INFO] Docker mode selected. Launching Docker multi-worker stack...
        ) else (
            echo [INFO] No mode specified. Defaulting to Docker multi-worker stack...
        )
        call :docker_prepare_multi_worker
        if errorlevel 1 (
            echo [ERROR] Failed to prepare Docker dependencies.
            pause
            exit /b 1
        )
        echo [START] Launching Crypto Sentinel via %COMPOSE_CMD% up --build -d
        call %COMPOSE_CMD% up --build -d
        if errorlevel 1 (
            echo [ERROR] Docker stack failed to start.
            pause
            exit /b 1
        )
        echo [WAIT] Waiting for http://127.0.0.1:8000/ ...
        call :wait_for_http "http://127.0.0.1:8000/" 90
        if errorlevel 1 (
            echo [ERROR] API did not become ready in time.
            echo         Check logs with: %COMPOSE_CMD% logs --tail=200 api worker worker_ai
            pause
            exit /b 1
        )
        echo [INFO] API is ready. Opening browser...
        call :open_browser "http://127.0.0.1:8000/"
        call :prompt_attach_logs
        exit /b 0
    )
    if "%EXPLICIT_DOCKER_MODE%"=="1" (
        echo [ERROR] Docker mode requested, but docker compose is unavailable.
        pause
        exit /b 1
    )
    echo [WARN] docker compose is unavailable. Falling back to local runtime mode.
)

if "%USE_SINGLE_MODE%"=="1" (
    set "START_MODE=single"
    set "START_MULTI_ARG=--single-worker"
) else (
    call :ensure_redis
    if errorlevel 1 (
        echo [ERROR] Default startup now requires Redis for multi-worker mode.
        echo         Use run.bat single for explicit single-worker mode.
        pause
        exit /b 1
    )
    set "START_MODE=multi"
    set "START_MULTI_ARG=--multi-worker"
)

echo.
if /I "%START_MODE%"=="multi" (
    echo [START] Launching Crypto Sentinel ^(API + Core Worker + AI Worker^)...
) else (
    echo [WARN] Explicit single-worker mode selected.
    echo [START] Launching Crypto Sentinel ^(API + Worker^)...
)
echo         Dashboard: http://127.0.0.1:8000
echo         Database: local .env DATABASE_URL (preserves existing SQLite data)
if /I "%START_MODE%"=="multi" (
    echo         Redis: %REDIS_URL%
) else (
    echo         Redis: disabled for this run
)
if defined COMPOSE_CMD echo         Docker stack: run.bat docker
echo         Run 'run.bat update' to update dependencies
echo         Press Ctrl+C to stop
echo.

call python -m alembic upgrade head
if errorlevel 1 (
    echo [ERROR] Alembic upgrade failed.
    pause
    exit /b 1
)

call python -m app.cli up --open-browser --no-db-init --backfill-days 1 %START_MULTI_ARG%
if errorlevel 1 (
    echo [ERROR] Crypto Sentinel failed to start.
    pause
    exit /b 1
)
exit /b %errorlevel%
