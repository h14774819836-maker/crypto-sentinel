from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import app


def test_ops_jobs_api_reads_metrics_file(monkeypatch, tmp_path):
    metrics_file = tmp_path / "job_metrics.json"
    metrics_file.write_text(
        json.dumps(
            [
                {"job_name": "feature_job", "status": "ok", "duration_ms": 12},
                {"job_name": "anomaly_job", "status": "error", "duration_ms": 33, "error_type": "ValueError"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("OPS_JOB_METRICS_FILE", str(metrics_file))
    get_settings.cache_clear()
    try:
        with TestClient(app) as client:
            resp = client.get("/api/ops/jobs?limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert data["worker"] == "core"
            assert data["count"] == 2
            assert data["items"][-1]["job_name"] == "anomaly_job"
            assert isinstance(data.get("warnings"), list)
    finally:
        get_settings.cache_clear()


def test_ops_jobs_api_supports_worker_all_merge(monkeypatch, tmp_path):
    core_file = tmp_path / "job_metrics_core.json"
    ai_file = tmp_path / "job_metrics_ai.json"
    core_file.write_text(json.dumps([{"job_name": "feature_job", "status": "ok"}], ensure_ascii=False), encoding="utf-8")
    ai_file.write_text(json.dumps([{"job_name": "ai_analysis_job", "status": "ok"}], ensure_ascii=False), encoding="utf-8")

    monkeypatch.setenv(
        "OPS_JOB_METRICS_FILES_JSON",
        json.dumps({"core": str(core_file), "ai": str(ai_file)}, ensure_ascii=False),
    )
    get_settings.cache_clear()
    try:
        with TestClient(app) as client:
            resp = client.get("/api/ops/jobs?worker=all&limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert data["worker"] == "all"
            assert data["count"] == 2
            workers = {item["worker"] for item in data["items"]}
            assert workers == {"core", "ai"}
    finally:
        get_settings.cache_clear()


def test_ops_jobs_api_invalid_mapping_returns_warning(monkeypatch):
    monkeypatch.setenv("OPS_JOB_METRICS_FILES_JSON", "{bad-json")
    monkeypatch.setenv("OPS_JOB_METRICS_FILE", "data/non-existent-metrics.json")
    get_settings.cache_clear()
    try:
        with TestClient(app) as client:
            resp = client.get("/api/ops/jobs?worker=core&limit=10")
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert data["count"] == 0
            assert any("OPS_JOB_METRICS_FILES_JSON" in msg for msg in data.get("warnings", []))
    finally:
        get_settings.cache_clear()
