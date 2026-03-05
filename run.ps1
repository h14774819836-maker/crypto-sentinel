param()

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Host.UI.RawUI.WindowTitle = "Crypto Sentinel"

Write-Host ""
Write-Host "  ╔══════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║    Crypto Sentinel V0.2         ║" -ForegroundColor Cyan
Write-Host "  ║    AI 加密货币分析系统           ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# --- Check Python ---
$pyVersion = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
if (-not $pyVersion) {
    Write-Host "[ERROR] Python not found. Install Python 3.11+ from https://www.python.org/downloads/" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}
$parts = $pyVersion.Split(".")
if ([int]$parts[0] -lt 3 -or ([int]$parts[0] -eq 3 -and [int]$parts[1] -lt 11)) {
    Write-Host "[ERROR] Python 3.11+ required. Current: $pyVersion" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# --- Setup venv (first run only) ---
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "[SETUP] Creating virtual environment..." -ForegroundColor Yellow
    python -m venv .venv
}
& ".\.venv\Scripts\Activate.ps1"

# --- Install/update dependencies (skip if already done) ---
$marker = ".venv\.deps_installed"
$needsInstall = -not (Test-Path $marker)

if (-not $needsInstall) {
    $pyprojectTime = (Get-Item "pyproject.toml" -ErrorAction SilentlyContinue).LastWriteTime
    $markerTime = (Get-Item $marker -ErrorAction SilentlyContinue).LastWriteTime
    if ($pyprojectTime -and $markerTime -and $pyprojectTime -gt $markerTime) {
        $needsInstall = $true
    }
}

if ($needsInstall) {
    Write-Host "[SETUP] Installing dependencies..." -ForegroundColor Yellow
    python -m pip install --quiet -e .[dev]
    "" | Out-File $marker
    Write-Host "[SETUP] Dependencies installed." -ForegroundColor Green
} else {
    Write-Host "[OK] Dependencies already installed." -ForegroundColor Green
}

# --- Create .env if missing ---
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[SETUP] Created .env from .env.example" -ForegroundColor Yellow
    Write-Host "        Please edit .env to add your DEEPSEEK_API_KEY" -ForegroundColor Yellow
    Write-Host ""
}

# --- Launch ---
Write-Host ""
Write-Host "[START] Launching Crypto Sentinel..." -ForegroundColor Green
Write-Host "        Dashboard: http://127.0.0.1:8000" -ForegroundColor Cyan
Write-Host "        Press Ctrl+C to stop" -ForegroundColor DarkGray
Write-Host ""

python -m alembic upgrade head
python -m app.cli up --open-browser --no-db-init --backfill-days 1
