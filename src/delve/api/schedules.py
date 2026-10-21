"""REST for schedules (B4): list, create, change, delete, run now."""

from __future__ import annotations

import uuid
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr

from delve.api.rest import Device
from delve.schedules import InvalidSchedule, ScheduleConflict, ScheduleNotFound, ScheduleService

router = APIRouter(prefix="/api/v1")


def _svc(request: Request) -> ScheduleService:
    svc: ScheduleService = request.app.state.schedules
    return svc


class ScheduleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: StrictStr = Field(min_length=1, max_length=200)
    cron: StrictStr = Field(min_length=1, max_length=200)
    prompt: StrictStr = Field(min_length=1)
    conversation_mode: Literal["new", "fixed"] = "new"
    conversation_id: StrictStr | None = None
    thinking: StrictBool = False
    enabled: StrictBool = True


class SchedulePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: StrictStr | None = Field(default=None, min_length=1, max_length=200)
    cron: StrictStr | None = Field(default=None, min_length=1, max_length=200)
    prompt: StrictStr | None = Field(default=None, min_length=1)
    conversation_mode: Literal["new", "fixed"] | None = None
    conversation_id: StrictStr | None = None
    thinking: StrictBool | None = None
    enabled: StrictBool | None = None


def _bad(exc: InvalidSchedule) -> HTTPException:
    return HTTPException(422, {"code": exc.code, "message": exc.message})


def _missing() -> HTTPException:
    return HTTPException(404, {"code": "schedule_not_found", "message": "no such schedule"})


@router.get("/schedules")
async def list_schedules(request: Request, _device: Device) -> dict[str, Any]:
    return {"data": await _svc(request).list()}


@router.post("/schedules", status_code=201)
async def create_schedule(request: Request, _device: Device, body: ScheduleIn) -> dict[str, Any]:
    try:
        return await _svc(request).create(body.model_dump())
    except InvalidSchedule as exc:
        raise _bad(exc) from exc
    except ScheduleConflict as exc:
        raise HTTPException(409, {"code": exc.code, "message": exc.message}) from exc


@router.patch("/schedules/{schedule_id}")
async def patch_schedule(
    request: Request, _device: Device, schedule_id: uuid.UUID, body: SchedulePatch
) -> dict[str, Any]:
    fields = body.model_dump(exclude_unset=True)
    for key in ("name", "cron", "prompt", "conversation_mode", "thinking", "enabled"):
        if key in fields and fields[key] is None:
            raise HTTPException(422, {"code": "invalid_field", "message": f"{key} cannot be null"})
    if not fields:
        raise HTTPException(422, {"code": "nothing_to_change", "message": "no field given"})
    try:
        return await _svc(request).update(schedule_id, fields)
    except ScheduleNotFound as exc:
        raise _missing() from exc
    except InvalidSchedule as exc:
        raise _bad(exc) from exc


@router.delete("/schedules/{schedule_id}", status_code=204)
async def delete_schedule(request: Request, _device: Device, schedule_id: uuid.UUID) -> Response:
    try:
        await _svc(request).delete(schedule_id)
    except ScheduleNotFound as exc:
        raise _missing() from exc
    except ScheduleConflict as exc:
        raise HTTPException(409, {"code": exc.code, "message": exc.message}) from exc
    return Response(status_code=204)


@router.post("/schedules/{schedule_id}/run")
async def run_schedule(request: Request, _device: Device, schedule_id: uuid.UUID) -> dict[str, Any]:
    try:
        return await _svc(request).run_now(schedule_id)
    except ScheduleNotFound as exc:
        raise _missing() from exc
