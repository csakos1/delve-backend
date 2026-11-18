"""REST for News (N1, `delve.news`): the Dashboard block, the items by topic, one story
with its other sources and its article (N1c), read / saved / feedback, "Fetch text", and
the topics, feeds and settings.

The bodies are checked by the service (the same rules as `delve-backend news import`);
a refusal is `{code, message}` with 404, 409 or 422."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Path, Query, Request, Response

from delve.api.rest import Device
from delve.news import NewsError, NewsService

router = APIRouter(prefix="/api/v1")

Json = Annotated[dict[str, Any], Body()]
TopicId = Annotated[str, Path(pattern=r"^[a-z][a-z0-9_]{0,31}$")]
SourceId = Annotated[str, Path(pattern=r"^[a-z0-9][a-z0-9.-]{0,47}$")]


def _svc(request: Request) -> NewsService:
    svc: NewsService = request.app.state.news
    return svc


def _refused(exc: NewsError) -> HTTPException:
    return HTTPException(exc.status, {"code": exc.code, "message": exc.message})


@router.get("/news")
async def news_digest(request: Request, _device: Device) -> dict[str, Any]:
    """The Dashboard's News block: `top` (at most 4 unread stories of the last day, the
    best first - newest first until scoring exists), `new` (unread stories of the last day),
    `updated_at` (the collector's last batch), `state` (`empty` before the first batch),
    `sources` (enabled feeds), `failing` (feeds that failed 3 times in a row), `weekly`
    (`{count}` on Mondays from the digest time on, else null)."""
    return await _svc(request).digest()


@router.get("/news/items")
async def news_items(
    request: Request,
    _device: Device,
    topic: Annotated[str | None, Query(pattern=r"^[a-z][a-z0-9_]{0,31}$")] = None,
    saved: bool = False,
    weekly: bool = False,
    before: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    """Newest first; `saved=true` the saved ones; `weekly=true` the best unopened stories of
    the last 7 days (one page, best first)."""
    data, next_before = await _svc(request).items(
        topic=topic, saved=saved, weekly=weekly, before=before, limit=limit
    )
    return {"data": data, "next_before": next_before}


@router.get("/news/items/{item_id}")
async def news_item(
    request: Request, _device: Device, item_id: Annotated[int, Path(ge=1)]
) -> dict[str, Any]:
    """One story with its other sources (`others`); a duplicate's id opens its story."""
    try:
        return await _svc(request).item(item_id)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.post("/news/items/{item_id}/text")
async def fetch_news_text(
    request: Request, _device: Device, item_id: Annotated[int, Path(ge=1)]
) -> dict[str, Any]:
    """ "Fetch text" (N1c): the story's article is queued for the collector's next run (at
    most an hour; a failed one is tried again) -> the story (`text_state: queued`). A story
    that has its text is returned as it is; a day of releases has none (409)."""
    try:
        return await _svc(request).request_text(item_id)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.patch("/news/items/{item_id}")
async def mark_news_item(
    request: Request, _device: Device, item_id: Annotated[int, Path(ge=1)], body: Json
) -> dict[str, Any]:
    """`{read?: bool, saved?: bool, feedback?: "more" | "less" | null}` -> the story."""
    try:
        return await _svc(request).mark(item_id, body)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.get("/news/topics")
async def news_topics(request: Request, _device: Device) -> dict[str, Any]:
    return {"data": await _svc(request).topics()}


@router.post("/news/topics", status_code=201)
async def create_news_topic(request: Request, _device: Device, body: Json) -> dict[str, Any]:
    try:
        return await _svc(request).create_topic(body)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.patch("/news/topics/{topic_id}")
async def update_news_topic(
    request: Request, _device: Device, topic_id: TopicId, body: Json
) -> dict[str, Any]:
    try:
        return await _svc(request).update_topic(topic_id, body)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.delete("/news/topics/{topic_id}", status_code=204)
async def delete_news_topic(request: Request, _device: Device, topic_id: TopicId) -> Response:
    """Its feeds go with it; the items stay until the retention, without a topic."""
    try:
        await _svc(request).delete_topic(topic_id)
    except NewsError as exc:
        raise _refused(exc) from exc
    return Response(status_code=204)


@router.get("/news/sources")
async def news_sources(request: Request, _device: Device) -> dict[str, Any]:
    return {"data": await _svc(request).sources()}


@router.post("/news/sources", status_code=201)
async def create_news_source(request: Request, _device: Device, body: Json) -> dict[str, Any]:
    """`{url, topic, id?, kind?, window_h?, every_min?, enabled?}`; the collector reads it
    from its next run on."""
    try:
        return await _svc(request).create_source(body)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.patch("/news/sources/{source_id}")
async def update_news_source(
    request: Request, _device: Device, source_id: SourceId, body: Json
) -> dict[str, Any]:
    """`{enabled?, topic?, window_h?, every_min?}` ("Hide this source": enabled false)."""
    try:
        return await _svc(request).update_source(source_id, body)
    except NewsError as exc:
        raise _refused(exc) from exc


@router.delete("/news/sources/{source_id}", status_code=204)
async def delete_news_source(request: Request, _device: Device, source_id: SourceId) -> Response:
    try:
        await _svc(request).delete_source(source_id)
    except NewsError as exc:
        raise _refused(exc) from exc
    return Response(status_code=204)


@router.get("/news/settings")
async def news_settings(request: Request, _device: Device) -> dict[str, Any]:
    return await _svc(request).settings()


@router.patch("/news/settings")
async def update_news_settings(request: Request, _device: Device, body: Json) -> dict[str, Any]:
    """`{setup?, digest_at? ("HH:MM"), push?, push_max? (0..5)}`."""
    try:
        return await _svc(request).update_settings(body)
    except NewsError as exc:
        raise _refused(exc) from exc
