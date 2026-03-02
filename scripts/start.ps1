param()

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Assert-PythonVersion {
    $v = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if (-not $v) {
        throw "Python not found. Please install Python 3.11+."
    }
    $parts = $v.Split(".")
    $major = [int]$parts[0]
    $minor = [int]$parts[1]
    if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 11)) {
        throw "Python 3.11+ is required. Current: $v"
    }
}

Assert-PythonVersion

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Creating virtual environment..."
    python -m venv .venv
}

& ".\.venv\Scripts\Activate.ps1"
python -m pip install -e .[dev]

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example (Telegram is optional)."
}

python -m app.cli up --open-browser --db-init --backfill-days 1
