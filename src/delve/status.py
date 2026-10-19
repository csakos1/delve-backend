"""What a client needs to know right after connecting (hello.ok, GET /api/v1/status)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve import API_REVISION, REVISION, __version__
from delve.db.models import SystemState
from delve.runs.manager import RunManager


async def thinking_route(sm: async_sessionmaker[AsyncSession]) -> dict[str, Any] | None:
    """Decision 138 as last seen: {ok, run_id, at}; None before the first thinking run."""
    async with sm() as s:
        row = await s.get(SystemState, "thinking_route")
    if row is None or not isinstance(row.value, dict):
        return None
    v = row.value
    return {"ok": bool(v.get("ok")), "run_id": v.get("run_id"), "at": v.get("at")}


async def snapshot(sm: async_sessionmaker[AsyncSession], mgr: RunManager) -> dict[str, Any]:
    ok = mgr.hermes_ok
    return {
        "server_version": __version__,
        "api_revision": API_REVISION,
        "revision": REVISION,
        "hermes": None if ok is None else ("ok" if ok else "down"),
        "hermes_version": mgr.hermes_version,
        "thinking_route": await thinking_route(sm),
    }
