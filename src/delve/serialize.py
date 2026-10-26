"""JSON shapes shared by REST responses and WebSocket events."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from delve.db.models import Approval, Card, Conversation, Message, Run, ToolStep
from delve.runs.steps import offered_choices

CONTEXT_WARN_PCT = 75.0  # the app warns above this (plan 7.1)
PREVIEW_CHARS = 200


def preview_text(text: str | None) -> str | None:
    """A conversation's list preview: whitespace collapsed, at most 200 characters;
    None for an empty text (the caller keeps the previous preview)."""
    if not text:
        return None
    flat = " ".join(text.split())
    return flat[:PREVIEW_CHARS] or None


def iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts else None


def conversation_out(c: Conversation) -> dict[str, Any]:
    pct = None
    if c.context_tokens is not None and c.context_max:
        pct = round(100.0 * c.context_tokens / c.context_max, 1)
    return {
        "id": c.id,
        "title": c.title,
        "preview": c.preview,
        "created_at": iso(c.created_at),
        "updated_at": iso(c.updated_at),
        "archived": c.archived,
        "thinking": c.thinking,
        "origin": c.origin,
        "last_run_id": c.last_run_id,
        "context": {
            "tokens": c.context_tokens,
            "max": c.context_max,
            "pct": pct,
            "warn": pct is not None and pct >= CONTEXT_WARN_PCT,
            "measured_at": iso(c.context_measured_at),
        },
        "totals": {
            "input_tokens": c.tokens_in,
            "output_tokens": c.tokens_out,
            "cache_read_tokens": c.tokens_cache_read,
            "total_tokens": c.tokens_in + c.tokens_out,
        },
    }


def message_out(m: Message, attachments: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """`attachments`: the message's attachment_out() list (B6); empty for most messages."""
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
        "attachments": attachments or [],
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
        "perf": r.perf,
        "error": r.error,
    }


def approval_out(a: Approval, choices: list[str] | None = None) -> dict[str, Any]:
    return {
        "request_id": a.request_id,
        "run_id": a.run_id,
        "conversation_id": a.conversation_id,
        "command": a.command,
        "description": a.description,
        "pattern_keys": a.pattern_keys or [],
        "action_class": a.action_class,
        "status": a.status,
        "choice": a.choice,
        "choices": choices if choices is not None else offered_choices(None),
        "decided_by": a.decided_by,
        "created_at": iso(a.created_at),
        "hermes_deadline": iso(a.hermes_deadline),
        "expires_at": iso(a.expires_at),
        "responded_at": iso(a.responded_at),
    }


def step_out(t: ToolStep, approval: Approval | None = None) -> dict[str, Any]:
    return {
        "id": t.id,
        "run_id": t.run_id,
        "tool": t.tool,
        "status": t.status,
        "preview": t.preview_start,
        "result": t.result,
        "result_raw": t.result_raw,
        "user_summary": t.user_summary,
        "duration_s": t.duration_s,
        "seq_started": t.seq_started,
        "seq_completed": t.seq_completed,
        "approval_request_id": t.approval_request_id,
        "approval": approval_out(approval) if approval is not None else None,
    }


def card_out(c: Card) -> dict[str, Any]:
    return {
        "id": c.id,
        "type": c.type,
        "severity": c.severity,
        "ts": iso(c.ts),
        "origin": c.origin,
        "session_id": c.session_id,
        "payload": c.payload,
        "received_at": iso(c.received_at),
    }
