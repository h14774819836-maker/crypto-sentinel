"""Admin authentication dependency for FastAPI management endpoints.

Usage:
    from app.web.auth import require_admin

    @router.post("/api/some-admin-endpoint")
    def admin_only(admin: str = Depends(require_admin)):
        ...
"""
from __future__ import annotations

import logging
from fastapi import Depends, HTTPException, Request
from app.config import get_settings

logger = logging.getLogger(__name__)


def require_admin(request: Request) -> str:
    """FastAPI dependency that enforces Bearer token authentication.

    Accepts the token from:
    1. ``Authorization: Bearer <token>`` header (preferred)
    2. ``?token=<token>`` query parameter (fallback for EventSource/SSE
       which cannot send custom headers)

    Returns the validated token string on success.
    Raises 403 if the token is missing, empty, or does not match.
    """
    settings = get_settings()
    expected = (settings.admin_token or "").strip()
    if not expected:
        logger.warning(
            "ADMIN_TOKEN is not configured – all admin endpoints are locked. "
            "Set ADMIN_TOKEN in .env to unlock."
        )
        raise HTTPException(status_code=403, detail="ADMIN_TOKEN not configured")

    # Try Authorization header first
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer "):].strip()
        if token == expected:
            return token

    # Fallback: ?token= query parameter (for EventSource/SSE)
    query_token = request.query_params.get("token", "").strip()
    if query_token and query_token == expected:
        return query_token

    raise HTTPException(status_code=403, detail="Invalid or missing admin token")
