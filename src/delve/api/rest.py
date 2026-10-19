"""REST API, part of plan 6.8: conversations and their messages.

Every route needs `Authorization: Bearer <device token>`.
"""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.api.history import messages_with_steps
from delve.auth import DeviceIdentity, authenticate
from delve.db.models import Conversation, Message
from delve.runs.approvals import DEFAULT_LIST_STATUSES, ApprovalError, ApprovalService
from delve.runs.manager import NotFound, RunManager
from delve.serialize import conversation_out


def _sessionmaker(request: Request) -> async_sessionmaker[AsyncSession]:
    sm: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    return sm


def _manager(request: Request) -> RunManager:
    mgr: RunManager = request.app.state.run_manager
    return mgr


async def require_device(request: Request) -> DeviceIdentity:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    device = None
    if scheme.lower() == "bearer" and token:
        async with _sessionmaker(request)() as s:
            device = await authenticate(s, token.strip())
    if device is None:
        raise HTTPException(
            status_code=401,
            detail={"code": "unauthorized", "message": "missing or invalid device token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    return device


Device = Annotated[DeviceIdentity, Depends(require_device)]
router = APIRouter(prefix="/api/v1")


def _encode_cursor(updated_at: datetime, conv_id: str) -> str:
    raw = json.dumps([updated_at.isoformat(), conv_id]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        pad = "=" * (-len(cursor) % 4)
        ts, conv_id = json.loads(base64.urlsafe_b64decode(cursor + pad))
        return datetime.fromisoformat(ts), str(conv_id)
    except (ValueError, TypeError, binascii.Error) as exc:
        raise HTTPException(400, {"code": "invalid_cursor", "message": "bad cursor"}) from exc


@router.get("/conversations")
async def list_conversations(
    request: Request,
    _device: Device,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
    archived: bool = False,
) -> dict[str, Any]:
    q = select(Conversation).where(Conversation.archived == archived)
    if cursor:
        ts, cid = _decode_cursor(cursor)
        q = q.where(
            or_(
                Conversation.updated_at < ts,
                and_(Conversation.updated_at == ts, Conversation.id < cid),
            )
        )
    q = q.order_by(Conversation.updated_at.desc(), Conversation.id.desc()).limit(limit + 1)
    async with _sessionmaker(request)() as s:
        rows = list((await s.execute(q)).scalars())
    more = len(rows) > limit
    rows = rows[:limit]
    return {
        "data": [conversation_out(c) for c in rows],
        "next_cursor": _encode_cursor(rows[-1].updated_at, rows[-1].id) if more else None,
    }


@router.get("/conversations/{conversation_id}")
async def get_conversation(request: Request, _device: Device, conversation_id: str) -> Any:
    async with _sessionmaker(request)() as s:
        conv = await s.get(Conversation, conversation_id)
    if conv is None:
        raise _not_found("conversation")
    return conversation_out(conv)


@router.get("/conversations/{conversation_id}/messages")
async def list_messages(
    request: Request,
    _device: Device,
    conversation_id: str,
    before: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> dict[str, Any]:
    """The newest `limit` messages (before message id `before`), oldest first."""
    async with _sessionmaker(request)() as s:
        if await s.get(Conversation, conversation_id) is None:
            raise _not_found("conversation")
        q = select(Message).where(Message.conversation_id == conversation_id)
        if before is not None:
            q = q.where(Message.id < before)
        rows = list((await s.execute(q.order_by(Message.id.desc()).limit(limit + 1))).scalars())
        more = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
        data = await messages_with_steps(s, rows)
    return {"data": data, "has_more": more}


class ConversationPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thinking: bool | None = None
    archived: bool | None = None


@router.patch("/conversations/{conversation_id}")
async def patch_conversation(
    request: Request, _device: Device, conversation_id: str, body: ConversationPatch
) -> Any:
    try:
        return await _manager(request).set_conversation(
            conversation_id, thinking=body.thinking, archived=body.archived
        )
    except NotFound as exc:
        raise _not_found("conversation") from exc


def _approvals(request: Request) -> ApprovalService:
    svc: ApprovalService = request.app.state.approvals
    return svc


@router.get("/approvals")
async def list_approvals(
    request: Request,
    _device: Device,
    status: str = ",".join(DEFAULT_LIST_STATUSES),
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> dict[str, Any]:
    statuses = tuple(x for x in (p.strip() for p in status.split(",")) if x)
    return {"data": await _approvals(request).list(statuses, limit)}


class ApprovalAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    choice: str


@router.post("/approvals/{request_id}")
async def answer_approval(
    request: Request, device: Device, request_id: str, body: ApprovalAnswer
) -> Any:
    try:
        return await _approvals(request).respond(request_id, body.choice, device.name)
    except ApprovalError as exc:
        raise HTTPException(exc.http_status, {"code": exc.code, "message": exc.message}) from exc


def _not_found(what: str) -> HTTPException:
    return HTTPException(404, {"code": f"{what}_not_found", "message": f"{what} not found"})
