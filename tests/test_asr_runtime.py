from __future__ import annotations

from types import SimpleNamespace

from app.asr_runtime import inspect_asr_runtime


def _settings(**overrides):
    base = {
        "asr_enabled": True,
        "asr_backend": "local_faster_whisper",
        "asr_model": "small",
        "asr_device": "cpu",
        "asr_compute_type": "int8",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_inspect_asr_runtime_ready_on_cpu(monkeypatch):
    monkeypatch.setattr("app.asr_runtime.runtime_mode_from_env", lambda: "docker_compose")
    monkeypatch.setattr("app.asr_runtime.shutil.which", lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None)
    monkeypatch.setattr("app.asr_runtime.importlib.util.find_spec", lambda name: object() if name == "faster_whisper" else None)

    status = inspect_asr_runtime(_settings())

    assert status["status"] == "ready"
    assert status["runtime_mode"] == "docker_compose"
    assert status["ffmpeg_available"] is True
    assert status["issues"] == []


def test_inspect_asr_runtime_degraded_without_ffmpeg(monkeypatch):
    monkeypatch.setattr("app.asr_runtime.runtime_mode_from_env", lambda: "docker_compose")
    monkeypatch.setattr("app.asr_runtime.shutil.which", lambda _name: None)
    monkeypatch.setattr("app.asr_runtime.importlib.util.find_spec", lambda name: object() if name == "faster_whisper" else None)

    status = inspect_asr_runtime(_settings())

    assert status["status"] == "degraded"
    assert "ffmpeg binary missing" in status["issues"]


def test_inspect_asr_runtime_degraded_when_cuda_requested_without_gpu(monkeypatch):
    monkeypatch.setattr("app.asr_runtime.runtime_mode_from_env", lambda: "docker_compose")
    monkeypatch.setattr("app.asr_runtime.shutil.which", lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None)
    monkeypatch.setattr("app.asr_runtime.importlib.util.find_spec", lambda name: object() if name == "faster_whisper" else None)
    monkeypatch.setattr("app.asr_runtime._cuda_device_count", lambda: 0)

    status = inspect_asr_runtime(_settings(asr_device="cuda", asr_compute_type="float16"))

    assert status["status"] == "degraded"
    assert "cuda requested but no CUDA device detected" in status["issues"]
