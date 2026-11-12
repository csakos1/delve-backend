"""REST for the system view (B3): `GET /system`, `POST /system/llama` (start, stop and,
with M, switching the model preset and dismissing a failed switch)."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator

from delve.api.rest import Device
from delve.system import HelperFailed, HelperUnavailable, SystemConflict, SystemService

router = APIRouter(prefix="/api/v1")


def _system(request: Request) -> SystemService:
    svc: SystemService = request.app.state.system
    return svc


class LlamaAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["start", "stop", "switch", "dismiss"]
    force: StrictBool = False
    # for "switch": a preset name as the helper lists it (system.llama.models[].name)
    model: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9._-]{0,39}$")

    @model_validator(mode="after")
    def _model_only_for_switch(self) -> LlamaAction:
        if (self.action == "switch") != (self.model is not None):
            raise ValueError("model is required for switch, and only for it")
        return self


@router.get("/system")
async def get_system(request: Request, _device: Device) -> dict[str, Any]:
    return await _system(request).snapshot()


@router.post("/system/llama")
async def control_llama(request: Request, device: Device, body: LlamaAction) -> dict[str, Any]:
    svc = _system(request)
    try:
        if body.action == "stop":
            return await svc.stop_llama(force=body.force, by=device.name)
        if body.action == "switch":
            return await svc.switch_llama(body.model or "", force=body.force, by=device.name)
        if body.action == "dismiss":
            return await svc.dismiss_switch()
        return await svc.start_llama(by=device.name)
    except SystemConflict as exc:
        status = 404 if exc.code == "unknown_model" else 409
        raise HTTPException(status, {"code": exc.code, "message": exc.message}) from exc
    except HelperUnavailable as exc:
        raise HTTPException(
            503, {"code": "helper_unavailable", "message": "the privileged helper is not reachable"}
        ) from exc
    except HelperFailed as exc:
        raise HTTPException(
            502, {"code": "helper_failed", "message": exc.message or exc.code}
        ) from exc
