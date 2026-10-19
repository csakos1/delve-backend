"""JSON shapes shared by REST responses and WebSocket events."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from delve.db.models import Conversation, Message, Run


def iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts else None


def conversation_out(c: Conversation) -> dict[str, Any]:
    pct = None
    if c.context_tokens is not None and c.context_max:
        pct = round(100.0 * c.context_tokens / c.context_max, 1)
    return {
        "id": c.id,
        "title": c.title,
        "created_at": iso(c.created_at),
        "updated_at": iso(c.updated_at),
        "archived": c.archived,
        "thinking": c.thinking,
        "origin": c.origin,
        "last_run_id": c.last_run_id,
        "context": {"tokens": c.context_tokens, "max": c.context_max, "pct": pct},
    }


def message_out(m: Message) -> dict[str, Any]:
    return {
        "id": m.id,
        "conversation_id": m.conversation_id,
        "run_id": m.run_id,
        "role": m.role,
        "content": m.content,
        "interim": m.interim,
        "reasoning": m.reasoning,
        "status": m.status,
        "client_msg_id": str(m.client_msg_id) if m.client_msg_id else None,
        "created_at": iso(m.created_at),
    }


def run_out(r: Run) -> dict[str, Any]:
    return {
        "run_id": r.run_id,
        "conversation_id": r.conversation_id,
        "status": r.status,
        "thinking": r.thinking,
        "origin": r.origin,
        "queued_at": iso(r.queued_at),
        "started_at": iso(r.started_at),
        "ended_at": iso(r.ended_at),
        "usage": r.usage,
        "error": r.error,
    }
