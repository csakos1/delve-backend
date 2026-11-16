"""The personal profile (P1, `delve.persona`): what the app shows and edits under
More → About me, and the part of it the app sends along with a General chat."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Path, Request
from pydantic import BaseModel, ConfigDict, Field, StrictBool

from delve.api.rest import Device
from delve.persona import (
    SECTION_TEXT_MAX,
    SECTION_TITLE_MAX,
    SECTIONS_MAX,
    TOPIC_SUMMARY_MAX,
    TOPIC_TEXT_MAX,
    TOPIC_TITLE_MAX,
    PersonaError,
    PersonaStore,
    Section,
    Topic,
)

router = APIRouter(prefix="/api/v1")

TopicName = Annotated[str, Path(pattern=r"^[a-z0-9][a-z0-9-]{0,39}$")]


def _store(request: Request) -> PersonaStore:
    store: PersonaStore = request.app.state.persona
    return store


def _refused(exc: PersonaError) -> HTTPException:
    status = 409 if exc.code == "too_many_topics" else 503
    return HTTPException(status, {"code": exc.code, "message": exc.message})


class SectionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=SECTION_TITLE_MAX)]
    text: Annotated[str, Field(max_length=SECTION_TEXT_MAX)]
    # may also go to the cloud models of the General chats
    general: StrictBool = False


class ProfileIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sections: Annotated[list[SectionIn], Field(max_length=SECTIONS_MAX)]


class TopicIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Annotated[str, Field(min_length=1, max_length=TOPIC_TITLE_MAX)]
    summary: Annotated[str, Field(max_length=TOPIC_SUMMARY_MAX)] = ""
    text: Annotated[str, Field(max_length=TOPIC_TEXT_MAX)]
    general: StrictBool = False


@router.get("/persona")
async def get_persona(request: Request, _device: Device) -> dict[str, Any]:
    """The profile as it is on disk now. `block` is what a new agent chat gets,
    `general_text` what the app may send with a General chat."""
    return await _store(request).out()


@router.put("/persona/profile")
async def put_profile(request: Request, _device: Device, body: ProfileIn) -> dict[str, Any]:
    """Replaces the core with these sections, in this order. A section without a text is
    dropped. Running chats keep the profile they started with."""
    store = _store(request)
    try:
        await store.save_profile([Section(s.title, s.text, s.general) for s in body.sections])
    except PersonaError as exc:
        raise _refused(exc) from exc
    return await store.out()


@router.put("/persona/topics/{name}")
async def put_topic(
    request: Request, _device: Device, name: TopicName, body: TopicIn
) -> dict[str, Any]:
    store = _store(request)
    try:
        await store.save_topic(Topic(name, body.title, body.summary, body.text, body.general))
    except PersonaError as exc:
        raise _refused(exc) from exc
    return await store.out()


@router.delete("/persona/topics/{name}")
async def delete_topic(request: Request, _device: Device, name: TopicName) -> dict[str, Any]:
    store = _store(request)
    try:
        found = await store.delete_topic(name)
    except PersonaError as exc:
        raise _refused(exc) from exc
    if not found:
        raise HTTPException(404, {"code": "topic_not_found", "message": "no such topic"})
    return await store.out()
