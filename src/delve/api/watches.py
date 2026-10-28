"""REST for watchers (W2): list, one watcher, its items, pause / resume / run now.

Watchers are made, changed and deleted in the chat (the agent's `watch_manage` tool);
here they are read, and their cron job is paused, resumed or run through the helper.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request

from delve.api.rest import Device
from delve.system import HelperFailed, HelperUnavailable
from delve.watches import WatchJobMissing, WatchNotFound, WatchService

router = APIRouter(prefix="/api/v1")

VERDICTS = frozenset({"pending", "match", "reject", "mechanical", "error"})
# shorthand for the app: what was (or would be) pushed
VERDICT_GROUPS = {"hits": ("match", "mechanical")}


def _svc(request: Request) -> WatchService:
    svc: WatchService = request.app.state.watches
    return svc


def _missing() -> HTTPException:
    return HTTPException(404, {"code": "watch_not_found", "message": "no such watcher"})


@router.get("/watches")
async def list_watches(request: Request, _device: Device) -> dict[str, Any]:
    return {"data": await _svc(request).list_watches()}


@router.get("/watches/{slug}")
async def get_watch(request: Request, _device: Device, slug: str) -> dict[str, Any]:
    try:
        return await _svc(request).get(slug)
    except WatchNotFound as exc:
        raise _missing() from exc


@router.get("/watches/{slug}/items")
async def watch_items(
    request: Request,
    _device: Device,
    slug: str,
    verdict: str | None = None,
    before: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    verdicts: list[str] | None = None
    if verdict:
        verdicts = []
        for part in verdict.split(","):
            part = part.strip()
            if part in VERDICT_GROUPS:
                verdicts.extend(VERDICT_GROUPS[part])
            elif part in VERDICTS:
                verdicts.append(part)
            else:
                raise HTTPException(
                    422, {"code": "invalid_verdict", "message": f"unknown verdict {part!r}"}
                )
    try:
        data, next_before = await _svc(request).items(
            slug, verdicts=verdicts, before=before, limit=limit
        )
    except WatchNotFound as exc:
        raise _missing() from exc
    return {"data": data, "next_before": next_before}


@router.post("/watches/{slug}/{verb}")
async def watch_action(
    request: Request, _device: Device, slug: str, verb: Literal["pause", "resume", "run"]
) -> dict[str, Any]:
    try:
        return await _svc(request).action(slug, verb)
    except WatchNotFound as exc:
        raise _missing() from exc
    except WatchJobMissing as exc:
        raise HTTPException(
            409, {"code": "watch_job_missing", "message": "the watcher has no cron job"}
        ) from exc
    except HelperUnavailable as exc:
        raise HTTPException(
            503, {"code": "helper_unavailable", "message": "the privileged helper is not reachable"}
        ) from exc
    except HelperFailed as exc:
        raise HTTPException(
            502, {"code": "helper_failed", "message": exc.message or exc.code}
        ) from exc
