from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


_BLOB_DIR = Path("data/blobs")


def _serialize_payload(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")


def _compress(payload_bytes: bytes) -> bytes:
    try:
        import zstandard as zstd  # type: ignore

        compressor = zstd.ZstdCompressor(level=3)
        return compressor.compress(payload_bytes)
    except Exception:
        # Fallback keeps storage API stable even when zstd is unavailable.
        return payload_bytes


def save_blob_with_meta(kind: str, payload: Any) -> tuple[str, str, int]:
    raw = _serialize_payload(payload)
    sha = hashlib.sha256(raw).hexdigest()
    compressed = _compress(raw)
    _BLOB_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{kind}_{sha}.json.zst"
    path = _BLOB_DIR / filename
    if not path.exists():
        path.write_bytes(compressed)
    return str(path.as_posix()), sha, len(compressed)


def save_blob(kind: str, payload: Any) -> str:
    blob_ref, _, _ = save_blob_with_meta(kind, payload)
    return blob_ref
