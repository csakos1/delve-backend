"""REST for the Observability screen (B5): `GET /stats?range=24h|7d|30d`."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query, Request

from delve.api.rest import Device
from delve.stats import StatsService

router = APIRouter(prefix="/api/v1")


@router.get("/stats")
async def get_stats(
    request: Request,
    _device: Device,
    range_: Annotated[Literal["24h", "7d", "30d"], Query(alias="range")] = "24h",
) -> dict[str, Any]:
    svc: StatsService = request.app.state.stats
    return await svc.stats(range_)
