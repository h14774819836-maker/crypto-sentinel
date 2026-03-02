@echo off
setlocal ENABLEDELAYEDEXPANSION

set "PROJECT_ROOT=%~dp0.."
cd /d "%PROJECT_ROOT%"

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)"
if errorlevel 1 (
  echo Python 3.11+ is required.
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Creating virtual environment...
  python -m venv .venv
  if errorlevel 1 exit /b 1
)

call ".venv\Scripts\activate.bat"
python -m pip install -e .[dev]
if errorlevel 1 exit /b 1

if not exist ".env" (
  copy /Y ".env.example" ".env" >nul
  echo Created .env from .env.example (Telegram is optional).
)

python -m app.cli up --open-browser --db-init --backfill-days 1
exit /b %errorlevel%
