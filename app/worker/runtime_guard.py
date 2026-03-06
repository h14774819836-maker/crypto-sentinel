from __future__ import annotations

import time


_SPLIT_ROLES = {"core", "ai"}
_FORBIDDEN_SPLIT_WORKER_IDS = {"", "worker-1", "core-worker-1", "ai-worker-1"}


def resolve_worker_role(settings) -> str:
    role = str(getattr(settings, "worker_role_normalized", "") or "").strip().lower()
    if role in {"all", "core", "ai"}:
        return role
    role_raw = str(getattr(settings, "worker_role", "all") or "").strip().lower()
    return role_raw if role_raw in {"all", "core", "ai"} else "all"


def is_split_worker_role(role: str) -> bool:
    return role in _SPLIT_ROLES


def worker_identity_lease_seconds(settings) -> int:
    heartbeat = max(1, int(getattr(settings, "worker_heartbeat_seconds", 15) or 15))
    return max(300, heartbeat * 4)


def worker_identity_stale_seconds(settings) -> int:
    heartbeat = max(1, int(getattr(settings, "worker_heartbeat_seconds", 15) or 15))
    return max(30, heartbeat * 3)


def worker_identity_redis_key(worker_id: str) -> str:
    return f"worker:heartbeat:{worker_id}"


async def ensure_split_worker_runtime_constraints(settings) -> None:
    role = resolve_worker_role(settings)
    if not is_split_worker_role(role):
        return

    worker_id = str(getattr(settings, "worker_id", "") or "").strip()
    if worker_id in _FORBIDDEN_SPLIT_WORKER_IDS:
        raise RuntimeError(
            f"WORKER_ROLE={role} requires non-default WORKER_ID. "
            f"Current WORKER_ID={worker_id!r} is not allowed."
        )

    if not bool(getattr(settings, "llm_hot_reload_use_redis", True)):
        raise RuntimeError(
            f"WORKER_ROLE={role} requires LLM_HOT_RELOAD_USE_REDIS=true. "
            "File hot-reload fallback is disabled in split-worker mode."
        )

    redis_url = str(getattr(settings, "redis_url", "") or "").strip()
    if not redis_url:
        raise RuntimeError(f"WORKER_ROLE={role} requires REDIS_URL.")

    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        await client.ping()
    except Exception as exc:  # pragma: no cover - network dependency
        raise RuntimeError(f"WORKER_ROLE={role} Redis ping failed: {exc}") from exc
    finally:
        await client.aclose()


async def reserve_worker_identity_lease(settings) -> None:
    role = resolve_worker_role(settings)
    if not is_split_worker_role(role):
        return
    if not bool(getattr(settings, "worker_id_strict_unique", True)):
        return

    worker_id = str(getattr(settings, "worker_id", "") or "").strip()
    if not worker_id:
        raise RuntimeError("WORKER_ID must not be empty in split-worker mode.")

    redis_url = str(getattr(settings, "redis_url", "") or "").strip()
    if not redis_url:
        raise RuntimeError("REDIS_URL is required for worker identity lease.")

    lease_seconds = worker_identity_lease_seconds(settings)
    key = worker_identity_redis_key(worker_id)

    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        now_ts = f"{time.time():.6f}"
        claimed = await client.set(key, now_ts, nx=True, ex=lease_seconds)
        if not claimed:
            existing_raw = await client.get(key)
            try:
                existing_ts = float(existing_raw) if existing_raw is not None else 0.0
            except (TypeError, ValueError):
                existing_ts = 0.0
            age_seconds = time.time() - existing_ts if existing_ts > 0 else float("inf")
            if age_seconds >= worker_identity_stale_seconds(settings):
                await client.set(key, now_ts, ex=lease_seconds)
                return
            raise RuntimeError(
                f"WORKER_ID conflict: {worker_id!r} already has an active lease "
                f"(key={key}, ttl~{lease_seconds}s)."
            )
    finally:
        await client.aclose()


async def touch_worker_identity_lease(settings) -> None:
    role = resolve_worker_role(settings)
    if not is_split_worker_role(role):
        return
    if not bool(getattr(settings, "worker_id_strict_unique", True)):
        return

    worker_id = str(getattr(settings, "worker_id", "") or "").strip()
    if not worker_id:
        return

    redis_url = str(getattr(settings, "redis_url", "") or "").strip()
    if not redis_url:
        return

    lease_seconds = worker_identity_lease_seconds(settings)
    key = worker_identity_redis_key(worker_id)

    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        await client.set(key, f"{time.time():.6f}", ex=lease_seconds)
    finally:
        await client.aclose()


async def release_worker_identity_lease(settings) -> None:
    role = resolve_worker_role(settings)
    if not is_split_worker_role(role):
        return
    if not bool(getattr(settings, "worker_id_strict_unique", True)):
        return

    worker_id = str(getattr(settings, "worker_id", "") or "").strip()
    if not worker_id:
        return

    redis_url = str(getattr(settings, "redis_url", "") or "").strip()
    if not redis_url:
        return

    key = worker_identity_redis_key(worker_id)

    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        await client.delete(key)
    finally:
        await client.aclose()
