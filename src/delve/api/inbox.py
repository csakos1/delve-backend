"""REST for the Inbox (I0, API 23): the list, clearing an item, taking that back."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictStr

from delve.api.rest import Device
from delve.inbox import InboxError, InboxService

router = APIRouter(prefix="/api/v1")


def _svc(request: Request) -> InboxService:
    svc: InboxService = request.app.state.inbox
    return svc


class ItemRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: StrictStr = Field(min_length=3, max_length=300)


def _http(exc: InboxError) -> HTTPException:
    return HTTPException(exc.status, {"code": exc.code, "message": exc.message})


@router.get("/inbox")
async def inbox(request: Request, _device: Device) -> dict[str, Any]:
    return await _svc(request).view()


@router.post("/inbox/done")
async def inbox_done(request: Request, _device: Device, body: ItemRef) -> dict[str, Any]:
    try:
        return await _svc(request).done(body.id)
    except InboxError as exc:
        raise _http(exc) from exc


@router.post("/inbox/undo")
async def inbox_undo(request: Request, _device: Device, body: ItemRef) -> dict[str, Any]:
    try:
        return await _svc(request).undo(body.id)
    except InboxError as exc:
        raise _http(exc) from exc
