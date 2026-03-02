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
            assert data["count"] == 2
            assert data["items"][-1]["job_name"] == "anomaly_job"
    finally:
        get_settings.cache_clear()

