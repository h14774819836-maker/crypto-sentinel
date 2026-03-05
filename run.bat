@echo off
chcp 65001 >nul 2>&1
title Crypto Sentinel

cd /d "%~dp0"

echo.
echo [Crypto Sentinel V0.2] AI 加密货币分析系统
echo.

REM --- Resolve Python ---
set "PYTHON="
where python >nul 2>&1 && set "PYTHON=python"
if "%PYTHON%"=="" (
    where py >nul 2>&1 && set "PYTHON=py -3.11"
)
if "%PYTHON%"=="" (
    echo [ERROR] Python 3.11+ is required.
    echo         Download from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM --- Check Python Version ---
call %PYTHON% -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" 2>nul
if errorlevel 1 (
    echo [ERROR] Python 3.11+ is required.
    echo         Download from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM --- Setup venv (first run only) ---
if not exist ".venv\Scripts\python.exe" (
    echo [SETUP] Creating virtual environment...
    call %PYTHON% -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create venv
        pause
        exit /b 1
    )
)

call ".venv\Scripts\activate.bat"

REM --- Verify environment: install/update dependencies every run ---
echo [CHECK] Verifying dependencies...
call %PYTHON% -m pip install --quiet -e .[dev]
if errorlevel 1 (
    echo [ERROR] pip install failed
    pause
    exit /b 1
)

REM --- Create .env if missing ---
if not exist ".env" (
    copy /Y ".env.example" ".env" >nul
    echo [SETUP] Created .env from .env.example
    echo         Please edit .env to add your DEEPSEEK_API_KEY
    echo.
)

REM --- Launch ---
echo.
echo [START] Launching Crypto Sentinel...
echo         Dashboard: http://127.0.0.1:8000
echo         Press Ctrl+C to stop
echo.

call %PYTHON% -m alembic upgrade head
if errorlevel 1 exit /b 1
call %PYTHON% -m app.cli up --open-browser --no-db-init --backfill-days 1
exit /b %errorlevel%
