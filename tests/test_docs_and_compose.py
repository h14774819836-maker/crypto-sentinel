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
    assert "db:" in content
    assert "migrate:" in content
    assert "service_completed_successfully" in content
    assert "python -m alembic upgrade head" in content
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
