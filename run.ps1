param(
    [string]$Action = ""
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Host.UI.RawUI.WindowTitle = "Crypto Sentinel"

function Get-ComposeCommand {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        try {
            docker compose version | Out-Null
            return @("docker", "compose")
        } catch {
        }
    }
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        try {
            docker-compose version | Out-Null
            return @("docker-compose")
        } catch {
        }
    }
    return $null
}

function Invoke-Compose {
    param(
        [string[]]$ComposeCommand,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )
    & $ComposeCommand[0] $ComposeCommand[1..($ComposeCommand.Length - 1)] @Args
    return $LASTEXITCODE
}

function Get-RedisUrlFromEnvFile {
    $defaultUrl = "redis://localhost:6379/0"
    if (-not (Test-Path ".env")) {
        return $defaultUrl
    }
    $line = Select-String -Path ".env" -Pattern "^REDIS_URL=(.+)$" | Select-Object -First 1
    if (-not $line) {
        return $defaultUrl
    }
    $value = $line.Matches[0].Groups[1].Value.Trim()
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $defaultUrl
    }
    return $value
}

function Test-RedisTcp {
    param([string]$RedisUrl)
    try {
        $uri = [Uri]$RedisUrl
        $client = New-Object System.Net.Sockets.TcpClient
        $iar = $client.BeginConnect($uri.Host, $uri.Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(1000, $false)) {
            $client.Close()
            return $false
        }
        $client.EndConnect($iar)
        $client.Close()
        return $true
    } catch {
        return $false
    }
}

function Wait-HttpReady {
    param(
        [string]$Url = "http://127.0.0.1:8000/",
        [int]$Attempts = 90
    )
    for ($i = 0; $i -lt $Attempts; $i++) {
        try {
            $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 2
            if ($resp.StatusCode -lt 500) {
                return $true
            }
        } catch {
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Ensure-Redis {
    param(
        [string]$RedisUrl,
        [string[]]$ComposeCommand
    )
    if (Test-RedisTcp -RedisUrl $RedisUrl) {
        Write-Host "[INFO] Redis is already available." -ForegroundColor Green
        return $true
    }

    Write-Host "[CHECK] Redis is not reachable at $RedisUrl" -ForegroundColor Yellow

    if ($ComposeCommand) {
        Write-Host "[SETUP] Trying to start Redis via $($ComposeCommand -join ' ') up -d redis..." -ForegroundColor Yellow
        if ((Invoke-Compose -ComposeCommand $ComposeCommand -Args @("up", "-d", "redis")) -eq 0) {
            for ($i = 0; $i -lt 20; $i++) {
                if (Test-RedisTcp -RedisUrl $RedisUrl) {
                    Write-Host "[INFO] Redis started via compose." -ForegroundColor Green
                    return $true
                }
                Start-Sleep -Seconds 1
            }
        }
    }

    if (Get-Command redis-server -ErrorAction SilentlyContinue) {
        Write-Host "[SETUP] Trying to start local redis-server..." -ForegroundColor Yellow
        Start-Process -WindowStyle Minimized -FilePath "redis-server" -ArgumentList "--appendonly", "yes" | Out-Null
        for ($i = 0; $i -lt 20; $i++) {
            if (Test-RedisTcp -RedisUrl $RedisUrl) {
                Write-Host "[INFO] Redis started via local redis-server." -ForegroundColor Green
                return $true
            }
            Start-Sleep -Seconds 1
        }
    }

    Write-Host "[ERROR] Could not start Redis automatically." -ForegroundColor Red
    if ($ComposeCommand) {
        Write-Host "        Try: $($ComposeCommand -join ' ') up -d redis" -ForegroundColor Yellow
    } else {
        Write-Host "        Neither 'docker compose' nor 'docker-compose' was found in PATH." -ForegroundColor Yellow
    }
    Write-Host "        Or install/start a local redis-server and retry." -ForegroundColor Yellow
    return $false
}

function Prepare-DockerMultiWorker {
    param([string[]]$ComposeCommand)
    if (-not $ComposeCommand) {
        return $false
    }
    Write-Host "[SETUP] Preparing Docker multi-worker dependencies..." -ForegroundColor Yellow
    if ((Invoke-Compose -ComposeCommand $ComposeCommand -Args @("up", "-d", "redis", "db")) -ne 0) {
        return $false
    }
    Write-Host "[SETUP] Clearing stale worker identity leases from Redis..." -ForegroundColor Yellow
    Invoke-Compose -ComposeCommand $ComposeCommand -Args @("exec", "-T", "redis", "redis-cli", "DEL", "worker:heartbeat:worker-core-1", "worker:heartbeat:worker-ai-1") | Out-Null
    return $true
}

function Prompt-AttachLogs {
    param([string[]]$ComposeCommand)
    if (-not $ComposeCommand) {
        return
    }
    $answer = Read-Host "Attach Docker logs now? [Y/N]"
    if ($answer -match "^(?i)y") {
        Write-Host "[INFO] Attaching logs. Press Ctrl+C to stop viewing logs." -ForegroundColor Cyan
        Invoke-Compose -ComposeCommand $ComposeCommand -Args @("logs", "-f", "api", "worker", "worker_ai") | Out-Null
    }
}

Write-Host ""
Write-Host "[Crypto Sentinel V0.2] AI Crypto Analysis System" -ForegroundColor Cyan
Write-Host ""

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

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "[SETUP] Creating virtual environment..." -ForegroundColor Yellow
    python -m venv .venv
}
& ".\.venv\Scripts\Activate.ps1"

$marker = ".venv\.deps_installed"
$needsInstall = -not (Test-Path $marker)
if (-not $needsInstall) {
    $pyprojectTime = (Get-Item "pyproject.toml" -ErrorAction SilentlyContinue).LastWriteTime
    $markerTime = (Get-Item $marker -ErrorAction SilentlyContinue).LastWriteTime
    if ($pyprojectTime -and $markerTime -and $pyprojectTime -gt $markerTime) {
        $needsInstall = $true
    }
}

if ($Action -in @("update", "--update", "install")) {
    $needsInstall = $true
}

if ($needsInstall) {
    Write-Host "[CHECK] Installing/Updating dependencies..." -ForegroundColor Yellow
    python -m pip install --quiet -e .[dev]
    "" | Out-File $marker
} else {
    Write-Host "[INFO] Skipping dependency check. Run 'run.ps1 update' to update." -ForegroundColor Green
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[SETUP] Created .env from .env.example" -ForegroundColor Yellow
    Write-Host "        Please edit .env to add your DEEPSEEK_API_KEY" -ForegroundColor Yellow
    Write-Host ""
}

$redisUrl = Get-RedisUrlFromEnvFile
$composeCommand = Get-ComposeCommand

if (-not (Test-RedisTcp -RedisUrl $redisUrl)) {
    if ($composeCommand) {
        Write-Host "[INFO] Local Redis is unavailable. Switching to Docker multi-worker stack..." -ForegroundColor Yellow
        if (-not (Prepare-DockerMultiWorker -ComposeCommand $composeCommand)) {
            Write-Host "[ERROR] Failed to prepare Docker dependencies." -ForegroundColor Red
            Read-Host "Press Enter to exit"
            exit 1
        }
        Write-Host "[START] Launching Crypto Sentinel via $($composeCommand -join ' ') up --build -d" -ForegroundColor Green
        if ((Invoke-Compose -ComposeCommand $composeCommand -Args @("up", "--build", "-d")) -ne 0) {
            Write-Host "[ERROR] Docker stack failed to start." -ForegroundColor Red
            Read-Host "Press Enter to exit"
            exit 1
        }
        Write-Host "[WAIT] Waiting for http://127.0.0.1:8000/ ..." -ForegroundColor Yellow
        if (-not (Wait-HttpReady -Url "http://127.0.0.1:8000/" -Attempts 90)) {
            Write-Host "[ERROR] API did not become ready in time." -ForegroundColor Red
            Write-Host "        Check logs with: $($composeCommand -join ' ') logs --tail=200 api worker worker_ai" -ForegroundColor Yellow
            Read-Host "Press Enter to exit"
            exit 1
        }
        Write-Host "[INFO] API is ready. Opening browser..." -ForegroundColor Green
        Start-Process "http://127.0.0.1:8000/"
        Prompt-AttachLogs -ComposeCommand $composeCommand
        exit 0
    }
}

if (-not (Ensure-Redis -RedisUrl $redisUrl -ComposeCommand $composeCommand)) {
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "[START] Launching Crypto Sentinel (API + Core Worker + AI Worker)..." -ForegroundColor Green
Write-Host "        Dashboard: http://127.0.0.1:8000" -ForegroundColor Cyan
Write-Host "        Redis: $redisUrl" -ForegroundColor Cyan
Write-Host "        Press Ctrl+C to stop" -ForegroundColor DarkGray
Write-Host ""

python -m alembic upgrade head
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Alembic upgrade failed." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

python -m app.cli up --open-browser --no-db-init --backfill-days 1 --multi-worker
exit $LASTEXITCODE
