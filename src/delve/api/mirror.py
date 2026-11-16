"""Notification mirroring (DA2, `delve.mirror`): the phone's events in, the desktop
agent's commands out.

`POST /api/v1/mirror/phone` and `GET /api/v1/mirror` take an app's device token like every
other route. `GET /api/v1/desk/next` and `GET /api/v1/desk/icon/{icon}` take the desktop
agent's token - a device with the role `desk` (`delve-backend token create --role desk`),
which opens nothing else here, as an app's token does not open these two.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StrictBool, model_validator

from delve.api.rest import Device, bearer_identity
from delve.auth import ROLE_DESK, DeviceIdentity
from delve.mirror import (
    APP_MAX,
    BODY_MAX,
    CATEGORY_MAX,
    EVENTS_MAX,
    ICON_B64_MAX,
    KEY_MAX,
    POLL_MAX_S,
    TITLE_MAX,
    MirrorService,
    decode_icon,
)

router = APIRouter(prefix="/api/v1")


async def require_desk(request: Request) -> DeviceIdentity:
    identity = await bearer_identity(request)
    if identity is None or identity.role != ROLE_DESK:
        raise HTTPException(
            status_code=401,
            detail={"code": "unauthorized", "message": "missing or invalid agent token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    return identity


Desk = Annotated[DeviceIdentity, Depends(require_desk)]


def _mirror(request: Request) -> MirrorService:
    service: MirrorService = request.app.state.mirror
    return service


class PhoneEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["post", "remove"]
    # the listener's key of the notification; the same key again replaces it
    key: Annotated[str, Field(min_length=1, max_length=KEY_MAX)]
    app: Annotated[str | None, Field(max_length=APP_MAX)] = None
    title: Annotated[str | None, Field(max_length=TITLE_MAX)] = None
    text: Annotated[str | None, Field(max_length=BODY_MAX)] = None
    # Android's category (msg, email, ...): lowercase letters and underscores
    category: Annotated[str | None, Field(max_length=CATEGORY_MAX, pattern=r"^[a-z_]*$")] = None
    hidden: StrictBool = False
    # the posting app's icon: a small PNG as base64 (API 12)
    icon: Annotated[str | None, Field(max_length=ICON_B64_MAX)] = None

    @model_validator(mode="after")
    def _post_names_its_app(self) -> PhoneEvent:
        if self.op == "post" and not (self.app or "").strip():
            raise ValueError("a post needs the app's name")
        if self.icon:
            decode_icon(self.icon)
        return self


class PhoneEvents(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: Annotated[list[PhoneEvent], Field(min_length=1, max_length=EVENTS_MAX)]


@router.post("/mirror/phone")
async def phone_events(request: Request, device: Device, body: PhoneEvents) -> dict[str, Any]:
    """What the phone's listener saw, in order. The answer says whether the desktop agent
    is there to show it; events for an absent agent wait a minute and are dropped."""
    service = _mirror(request)
    accepted = service.accept(device.id, [e.model_dump() for e in body.events])
    return {"accepted": accepted, **service.status()}


@router.get("/mirror")
async def mirror_status(request: Request, _device: Device) -> dict[str, Any]:
    return _mirror(request).status()


@router.get("/desk/next")
async def desk_next(
    request: Request,
    _desk: Desk,
    wait: Annotated[float, Query(ge=0, le=POLL_MAX_S)] = 25.0,
) -> dict[str, Any]:
    """Long poll: the queued commands at once, else those arriving within `wait` seconds,
    else an empty list. The agent asks again right away."""
    return {"commands": await _mirror(request).next(wait)}


@router.get("/desk/icon/{icon}")
async def desk_icon(
    request: Request,
    _desk: Desk,
    icon: Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")],
) -> Response:
    """An app's icon by the name a `notify.show` gave it. 404 once it is no longer kept
    (the backend restarted, or many other icons came since): the agent shows the
    notification without one, and the phone's next post of that app brings it again."""
    png = _mirror(request).icon(icon)
    if png is None:
        raise HTTPException(
            status_code=404, detail={"code": "icon_not_found", "message": "no such icon"}
        )
    return Response(content=png, media_type="image/png", headers={"Cache-Control": "no-store"})
