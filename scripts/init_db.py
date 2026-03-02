from __future__ import annotations

import subprocess
import sys


if __name__ == "__main__":
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    print("Database migrated")
