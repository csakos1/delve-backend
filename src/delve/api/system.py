"""REST for the system view (B3): `GET /system`, `POST /system/llama`."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, StrictBool

from delve.api.rest import Device
from delve.system import HelperFailed, HelperUnavailable, SystemConflict, SystemService

router = APIRouter(prefix="/api/v1")


def _system(request: Request) -> SystemService:
    svc: SystemService = request.app.state.system
    return svc


class LlamaAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["start", "stop"]
    force: StrictBool = False


@router.get("/system")
async def get_system(request: Request, _device: Device) -> dict[str, Any]:
    return await _system(request).snapshot()


@router.post("/system/llama")
async def control_llama(request: Request, device: Device, body: LlamaAction) -> dict[str, Any]:
    svc = _system(request)
    try:
        if body.action == "stop":
            return await svc.stop_llama(force=body.force, by=device.name)
        return await svc.start_llama(by=device.name)
    except SystemConflict as exc:
        raise HTTPException(409, {"code": exc.code, "message": exc.message}) from exc
    except HelperUnavailable as exc:
        raise HTTPException(
            503, {"code": "helper_unavailable", "message": "the privileged helper is not reachable"}
        ) from exc
    except HelperFailed as exc:
        raise HTTPException(
            502, {"code": "helper_failed", "message": exc.message or exc.code}
        ) from exc
