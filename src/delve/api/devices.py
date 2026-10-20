"""Pairing, the calling device, and status (plan 6.8, 12/8 part, 12/12).

`POST /api/v1/devices/register` is the only route here without a device token: it takes
the one-time code from `delve-backend pair`. Failed attempts are rate limited globally.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.api.rest import Device
from delve.auth import FCM_TOKEN_MAX, redeem_pairing_code, revoke_device, set_fcm_token
from delve.db.models import Device as DeviceRow
from delve.serialize import iso
from delve.status import snapshot

router = APIRouter(prefix="/api/v1")


class FailureLimiter:
    """At most `limit` failed attempts per `window_s`, across all clients."""

    def __init__(self, limit: int = 10, window_s: float = 60.0) -> None:
        self.limit, self.window_s = limit, window_s
        self._fails: deque[float] = deque()

    def _trim(self, now: float) -> None:
        while self._fails and now - self._fails[0] > self.window_s:
            self._fails.popleft()

    def blocked(self) -> float | None:
        """Seconds until the next attempt is allowed, or None."""
        now = time.monotonic()
        self._trim(now)
        if len(self._fails) < self.limit:
            return None
        return max(0.0, self.window_s - (now - self._fails[0]))

    def fail(self) -> None:
        self._fails.append(time.monotonic())


def _sm(request: Request) -> async_sessionmaker[AsyncSession]:
    sm: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    return sm


class Register(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Annotated[str, Field(min_length=1, max_length=32)]
    name: Annotated[str | None, Field(max_length=64)] = None
    app_version: Annotated[str | None, Field(max_length=64)] = None


@router.post("/devices/register", status_code=201)
async def register(request: Request, body: Register) -> dict[str, Any]:
    limiter: FailureLimiter = request.app.state.pairing_limiter
    wait = limiter.blocked()
    if wait is not None:
        raise HTTPException(
            429,
            {"code": "too_many_attempts", "message": "too many failed pairing attempts"},
            headers={"Retry-After": str(int(wait) + 1)},
        )
    async with _sm(request)() as s:
        paired = await redeem_pairing_code(s, body.code, body.name)
        if paired is None:
            await s.rollback()
            limiter.fail()
            raise HTTPException(
                400, {"code": "invalid_code", "message": "unknown, used or expired code"}
            )
        device, token = paired
        await s.commit()
    return {"device_id": str(device.id), "name": device.name, "token": token}


@router.get("/devices/me")
async def me(request: Request, device: Device) -> dict[str, Any]:
    async with _sm(request)() as s:
        row = await s.get(DeviceRow, device.id)
    if row is None:  # revoked rows stay; a deleted row would have failed authentication
        raise HTTPException(404, {"code": "device_not_found", "message": "device not found"})
    return {
        "id": str(row.id),
        "name": row.name,
        "created_at": iso(row.created_at),
        "last_seen_at": iso(row.last_seen_at),
        "fcm": row.fcm_token is not None,
    }


class Fcm(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: Annotated[str | None, Field(min_length=1, max_length=FCM_TOKEN_MAX)]


@router.put("/devices/me/fcm", status_code=204)
async def put_fcm(request: Request, device: Device, body: Fcm) -> Response:
    """Store this device's FCM token; `null` clears it."""
    async with _sm(request)() as s:
        await set_fcm_token(s, device.id, body.token)
        await s.commit()
    return Response(status_code=204)


@router.delete("/devices/me", status_code=204)
async def unpair(request: Request, device: Device) -> Response:
    """The app's "Unpair this device": revokes the calling token (and its FCM token).
    An open WebSocket of the device is not closed; the app closes its own."""
    async with _sm(request)() as s:
        await revoke_device(s, device.id)
        await s.commit()
    return Response(status_code=204)


@router.get("/status")
async def status(request: Request, _device: Device) -> dict[str, Any]:
    """The same as hello.ok's status part, for a client without a socket yet."""
    return await snapshot(_sm(request), request.app.state.run_manager)
