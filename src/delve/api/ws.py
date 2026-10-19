"""WebSocket gateway (plan 6.7), `/api/v1/ws`.

Frames are JSON objects `{"type", "id"?, "data"}`. The first client frame must be `hello`
with a device token. Broadcast events carry a global `seq`; replies to a client frame
(`ack`, `pong`, `error`) carry the frame's `id` instead.

Close codes: 4400 protocol error, 4401 unauthorized, 4408 hello timeout or slow
consumer (reconnect with `last_seq`).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve import __version__
from delve.api.history import messages_with_steps
from delve.auth import DeviceIdentity, authenticate
from delve.db.models import Conversation, Message
from delve.events import EventHub, Subscription
from delve.runs.approvals import ApprovalError, ApprovalService
from delve.runs.manager import InvalidRequest, NotFound, RunManager
from delve.serialize import conversation_out

log = logging.getLogger("delve.ws")
router = APIRouter()

HELLO_TIMEOUT_S = 10.0
KEEPALIVE_S = 20.0
MAX_FRAME_BYTES = 256 * 1024


class ProtocolError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@router.websocket("/api/v1/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    app = ws.app
    hub: EventHub = app.state.hub
    sm: async_sessionmaker[AsyncSession] = app.state.sessionmaker
    try:
        hello = await asyncio.wait_for(_receive(ws), HELLO_TIMEOUT_S)
    except TimeoutError:
        await ws.close(4408, "hello timeout")
        return
    except (WebSocketDisconnect, ProtocolError):
        with contextlib.suppress(Exception):
            await ws.close(4400, "expected hello")
        return
    data = hello.get("data") or {}
    if hello.get("type") != "hello" or not isinstance(data, dict):
        await ws.close(4400, "expected hello")
        return
    token = data.get("device_token")
    async with sm() as s:
        device = await authenticate(s, token if isinstance(token, str) else None)
    if device is None:
        await ws.close(4401, "unauthorized")
        return

    sub = hub.subscribe()
    try:
        watermark = await _handshake(ws, hub, data, app.state)
        await _session(ws, sub, watermark, device, app.state.run_manager, sm, app.state.approvals)
    except WebSocketDisconnect:
        pass
    finally:
        hub.unsubscribe(sub)


async def _handshake(ws: WebSocket, hub: EventHub, data: dict[str, Any], state: Any) -> int:
    """Send hello.ok and any replay. Returns the last seq the client has seen; the caller
    subscribed before this, so newer events are already queued and older ones skipped."""
    last_seq = data.get("last_seq")
    epoch = data.get("epoch")
    replay: list[Any] = []
    resync = False
    if isinstance(last_seq, int) and not isinstance(last_seq, bool):
        events = hub.since(last_seq, epoch if isinstance(epoch, str) else None)
        if events is None:
            resync = True
        else:
            replay = events
    await _send(
        ws,
        {
            "type": "hello.ok",
            "data": {
                "server_version": __version__,
                "hermes_version": state.run_manager.hermes_version,
                "epoch": hub.epoch,
                "seq": hub.seq,
                "resync_required": resync,
            },
        },
    )
    sent = last_seq if isinstance(last_seq, int) and not resync else hub.seq
    for env in replay:
        await _send(ws, env.frame())
        sent = env.seq
    return sent


async def _session(
    ws: WebSocket,
    sub: Subscription,
    watermark: int,
    device: DeviceIdentity,
    mgr: RunManager,
    sm: async_sessionmaker[AsyncSession],
    approvals: ApprovalService,
) -> None:
    async def pump() -> None:
        while True:
            env = await sub.queue.get()
            if env.seq > watermark:
                await _send(ws, env.frame())

    async def keepalive() -> None:
        while True:
            await asyncio.sleep(KEEPALIVE_S)
            await _send(ws, {"type": "keepalive", "data": {"ts": time.time()}})

    async def overflow_watch() -> None:
        await sub.closed.wait()
        if sub.overflowed:
            await ws.close(4408, "slow consumer, resume with last_seq")

    tasks = [
        asyncio.create_task(pump()),
        asyncio.create_task(keepalive()),
        asyncio.create_task(overflow_watch()),
    ]
    try:
        while True:
            try:
                frame = await _receive(ws)
            except ProtocolError as exc:
                await _send(ws, _error(None, exc.code, exc.message))
                continue
            await _handle(ws, frame, device, mgr, sm, approvals)
            if any(t.done() for t in tasks):
                break
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t


async def _handle(
    ws: WebSocket,
    frame: dict[str, Any],
    device: DeviceIdentity,
    mgr: RunManager,
    sm: async_sessionmaker[AsyncSession],
    approvals: ApprovalService,
) -> None:
    ftype, fid, data = frame.get("type"), frame.get("id"), frame.get("data") or {}
    if not isinstance(data, dict):
        await _send(ws, _error(fid, "invalid_frame", "data must be an object"))
        return
    try:
        if ftype == "ping":
            await _send(ws, {"type": "pong", "id": fid, "data": {}})
        elif ftype == "chat.send":
            await _send(ws, _ack(fid, await _chat_send(data, mgr)))
        elif ftype == "conversation.set":
            conv_id = _required_str(data, "conversation_id")
            out = await mgr.set_conversation(
                conv_id, thinking=_opt_bool(data, "thinking"), archived=_opt_bool(data, "archived")
            )
            await _send(ws, _ack(fid, {"conversation": out}))
        elif ftype == "run.stop":
            status = await mgr.stop(_required_str(data, "run_id"))
            await _send(ws, _ack(fid, {"status": status}))
        elif ftype == "sync":
            await _send(ws, _ack(fid, await _sync(data, sm)))
        elif ftype == "approval.respond":
            request_id = _required_str(data, "request_id")
            choice = _required_str(data, "choice")
            try:
                out = await approvals.respond(request_id, choice, device.name)
            except ApprovalError as exc:
                await _send(ws, _error(fid, exc.code, exc.message))
                return
            await _send(ws, _ack(fid, {"approval": out}))
        else:
            await _send(ws, _error(fid, "unknown_type", f"unknown frame type {ftype!r}"))
    except ProtocolError as exc:
        await _send(ws, _error(fid, exc.code, exc.message))
    except InvalidRequest as exc:
        await _send(ws, _error(fid, "invalid_request", str(exc)))
    except NotFound as exc:
        await _send(ws, _error(fid, "not_found", str(exc)))


async def _chat_send(data: dict[str, Any], mgr: RunManager) -> dict[str, Any]:
    text = data.get("text")
    if not isinstance(text, str):
        raise ProtocolError("invalid_request", "text must be a string")
    try:
        client_msg_id = uuid.UUID(_required_str(data, "client_msg_id"))
    except ValueError as exc:
        raise ProtocolError("invalid_request", "client_msg_id must be a UUID") from exc
    if data.get("attachment_ids"):
        raise ProtocolError("not_implemented", "attachments arrive in a later step")
    conv_id = data.get("conversation_id")
    if conv_id is not None and not isinstance(conv_id, str):
        raise ProtocolError("invalid_request", "conversation_id must be a string")
    result = await mgr.submit(
        text=text,
        client_msg_id=client_msg_id,
        conversation_id=conv_id,
        thinking=_opt_bool(data, "thinking"),
    )
    return {
        "conversation_id": result.conversation["id"],
        "message_id": result.message["id"],
        "run_id": result.run_id,
        "replayed": not result.created,
    }


SYNC_LIMIT = 200


async def _sync(data: dict[str, Any], sm: async_sessionmaker[AsyncSession]) -> dict[str, Any]:
    """Messages of a conversation after `after_message_id` (oldest first, max 200); for
    older history the client pages GET /api/v1/conversations/{id}/messages."""
    conv_id = _required_str(data, "conversation_id")
    after = data.get("after_message_id")
    if after is not None and (not isinstance(after, int) or isinstance(after, bool)):
        raise ProtocolError("invalid_request", "after_message_id must be an integer")
    async with sm() as s:
        conv = await s.get(Conversation, conv_id)
        if conv is None:
            raise NotFound(f"conversation {conv_id} not found")
        q = select(Message).where(Message.conversation_id == conv_id)
        if after is not None:
            q = q.where(Message.id > after)
        rows = list((await s.execute(q.order_by(Message.id).limit(SYNC_LIMIT + 1))).scalars())
        messages = await messages_with_steps(s, rows[:SYNC_LIMIT])
    return {
        "conversation": conversation_out(conv),
        "messages": messages,
        "has_more": len(rows) > SYNC_LIMIT,
    }


async def _receive(ws: WebSocket) -> dict[str, Any]:
    raw = await ws.receive_text()
    if len(raw) > MAX_FRAME_BYTES:
        raise ProtocolError("frame_too_large", "frame too large")
    try:
        frame = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolError("invalid_json", "frame is not JSON") from exc
    if not isinstance(frame, dict):
        raise ProtocolError("invalid_frame", "frame must be an object")
    return frame


async def _send(ws: WebSocket, frame: dict[str, Any]) -> None:
    await ws.send_text(json.dumps(frame, ensure_ascii=False, default=str))


def _ack(fid: Any, data: dict[str, Any]) -> dict[str, Any]:
    return {"type": "ack", "id": fid, "data": data}


def _error(fid: Any, code: str, message: str) -> dict[str, Any]:
    return {"type": "error", "id": fid, "data": {"code": code, "message": message}}


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ProtocolError("invalid_request", f"{key} is required")
    return value


def _opt_bool(data: dict[str, Any], key: str) -> bool | None:
    value = data.get(key)
    if value is not None and not isinstance(value, bool):
        raise ProtocolError("invalid_request", f"{key} must be a boolean")
    return value
