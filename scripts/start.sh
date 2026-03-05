#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python not found. Please install Python 3.11+." >&2
  exit 1
fi

"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit(f"Python 3.11+ required, current: {sys.version.split()[0]}")
PY

if [ ! -x ".venv/bin/python" ]; then
  echo "Creating virtual environment..."
  "$PYTHON_BIN" -m venv .venv
fi

source .venv/bin/activate
python -m pip install -e '.[dev]'

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from .env.example (Telegram is optional)."
fi

python -m alembic upgrade head
python -m app.cli up --open-browser --no-db-init --backfill-days 1
