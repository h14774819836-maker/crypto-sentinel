from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_root_compose_exists():
    compose_file = PROJECT_ROOT / "docker-compose.yml"
    assert compose_file.exists()
    content = compose_file.read_text(encoding="utf-8")
    assert "services:" in content
    assert "api:" in content
    assert "worker:" in content
    assert "worker_ai:" in content
    assert "db:" in content
    assert "redis:" in content
    assert "migrate:" in content
    assert "service_completed_successfully" in content
    assert "python -m alembic upgrade head" in content
    assert "LLM_HOT_RELOAD_USE_REDIS" in content
    assert '"6379:6379"' in content
    assert "HTTP_PROXY: ${HTTP_PROXY:-}" in content
    assert "HTTPS_PROXY: ${HTTPS_PROXY:-}" in content
    assert "scripts/init_db.py" not in content


def test_docker_subdir_compose_is_redirect_notice():
    legacy_compose = PROJECT_ROOT / "docker" / "docker-compose.yml"
    assert legacy_compose.exists()
    content = legacy_compose.read_text(encoding="utf-8")
    assert "Deprecated location" in content
    assert "docker compose -f ../docker-compose.yml up --build" in content
    assert "services: {}" in content


def test_readme_mentions_new_quick_start_paths():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    assert "docker compose up --build" in readme
    assert "run.bat" in readme
    assert "run.ps1" in readme
    assert "127.0.0.1" in readme


def test_templates_default_to_chinese():
    overview = (PROJECT_ROOT / "app" / "web" / "templates" / "overview.html").read_text(encoding="utf-8")
    alerts = (PROJECT_ROOT / "app" / "web" / "templates" / "alerts.html").read_text(encoding="utf-8")
    assert "市场总览" in overview
    assert ("告警中心" in alerts) or ("Alerts Center" in alerts)


def test_start_scripts_force_backfill_one_day():
    ps1_content = (PROJECT_ROOT / "scripts" / "start.ps1").read_text(encoding="utf-8")
    sh_content = (PROJECT_ROOT / "scripts" / "start.sh").read_text(encoding="utf-8")
    bat_content = (PROJECT_ROOT / "scripts" / "start.bat").read_text(encoding="utf-8")
    assert "--backfill-days 1" in ps1_content
    assert "--backfill-days 1" in sh_content
    assert "--backfill-days 1" in bat_content


def test_root_run_bat_supports_local_and_docker_modes():
    content = (PROJECT_ROOT / "run.bat").read_text(encoding="utf-8")
    assert "setlocal EnableExtensions" in content
    assert ":detect_compose_command" in content
    assert ":use_stop_action" in content
    assert ":use_single_worker_action" in content
    assert ":docker_prepare_multi_worker" in content
    assert "docker compose version" in content
    assert "docker-compose version" in content
    assert "python -m app.cli down --reason script_stop --requested-by run.bat" in content
    assert "up -d redis" in content
    assert "up -d redis db" in content
    assert "redis-cli DEL worker:heartbeat:worker-core-1 worker:heartbeat:worker-ai-1" in content
    assert "up --build" in content
    assert "redis-server --appendonly yes" in content
    assert "--multi-worker" in content
    assert "--single-worker" in content
    assert ":ensure_redis" in content
    assert "Docker mode is available via: run.bat docker" in content
    assert "Use run.bat single for explicit single-worker mode." in content


def test_root_run_ps1_supports_local_and_docker_modes():
    content = (PROJECT_ROOT / "run.ps1").read_text(encoding="utf-8")
    assert "function Get-ComposeCommand" in content
    assert "function Use-StopAction" in content
    assert "function Use-SingleWorkerAction" in content
    assert "function Prepare-DockerMultiWorker" in content
    assert "python -m app.cli down --reason script_stop --requested-by run.ps1" in content
    assert 'Invoke-Compose -ComposeCommand $ComposeCommand -Args @("up", "-d", "redis")' in content or 'Invoke-Compose -ComposeCommand $composeCommand -Args @("up", "-d", "redis")' in content
    assert 'Invoke-Compose -ComposeCommand $composeCommand -Args @("up", "--build", "-d")' in content
    assert 'Wait-HttpReady -Url "http://127.0.0.1:8000/" -Attempts 90' in content
    assert 'Start-Process "http://127.0.0.1:8000/"' in content
    assert "Prompt-AttachLogs" in content
    assert '--multi-worker' in content
    assert '--single-worker' in content
    assert 'Docker mode is available via: run.ps1 docker' in content
    assert 'Use run.ps1 single for explicit single-worker mode.' in content


def test_postgres_migration_uses_boolean_false_default():
    content = (
        PROJECT_ROOT
        / "app"
        / "db"
        / "migrations"
        / "versions"
        / "09280b48e726_add_sent_to_telegram_to_aisignal.py"
    ).read_text(encoding="utf-8")
    assert "server_default=sa.false()" in content
    assert "server_default=sa.text('0')" not in content


def test_youtube_status_migration_backfills_runtime_columns():
    content = (
        PROJECT_ROOT
        / "app"
        / "db"
        / "migrations"
        / "versions"
        / "20260304_0013_add_youtube_explicit_status.py"
    ).read_text(encoding="utf-8")
    assert 'sa.inspect(conn).get_columns("youtube_videos")' in content
    assert '"analysis_runtime_status"' in content
    assert '"analysis_updated_at"' in content
    assert '"analysis_retry_count"' in content


def test_env_example_mentions_optional_proxy_settings():
    content = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    assert "HTTP_PROXY=" in content
    assert "HTTPS_PROXY=" in content
    assert "ALL_PROXY=" in content
    assert "NO_PROXY=localhost,127.0.0.1,db,redis" in content
