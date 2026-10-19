"""The WebSocket protocol as data: frame types and the keys of every server event.

The app's models are written against this (and `docs/contract.json`, generated from it).
`EventHub` checks each published event against `SERVER_EVENTS`; a mismatch is logged, and
raised while `STRICT` is set (the test suite sets it), so the declaration cannot drift
from what the code sends.
"""

from __future__ import annotations

from collections.abc import Mapping

STRICT = False

_CONVERSATION = frozenset(
    {"id", "title", "created_at", "updated_at", "archived", "thinking", "origin",
     "last_run_id", "context", "totals"}
)  # fmt: skip
_APPROVAL = frozenset(
    {"request_id", "run_id", "conversation_id", "command", "description", "pattern_keys",
     "action_class", "status", "choice", "choices", "decided_by", "created_at",
     "hermes_deadline", "expires_at", "responded_at"}
)  # fmt: skip

_CONTEXT = frozenset(
    {"conversation_id", "run_id", "tokens", "max", "pct", "warn", "source", "measured_at",
     "final"}
)  # fmt: skip

# Events from the hub: `{"type", "seq", "data"}`; the value is the exact key set of `data`.
SERVER_EVENTS: Mapping[str, frozenset[str]] = {
    "conversation.created": _CONVERSATION,
    "conversation.updated": _CONVERSATION,
    "message.created": frozenset({"conversation_id", "message"}),
    "message.updated": frozenset({"conversation_id", "message"}),
    "message.delta": frozenset({"conversation_id", "run_id", "message_id", "delta"}),
    "message.interim": frozenset({"conversation_id", "run_id", "message_id", "text"}),
    "run.queued": frozenset({"conversation_id", "run_id", "position", "thinking", "origin"}),
    "run.started": frozenset({"conversation_id", "run_id", "queued_id", "thinking"}),
    "run.updated": frozenset({"conversation_id", "run_id", "status"}),
    "run.finished": frozenset({"conversation_id", "run_id", "status", "usage", "error"}),
    "tool.started": frozenset({"conversation_id", "run_id", "step"}),
    "tool.completed": frozenset({"conversation_id", "run_id", "step"}),
    "approval.request": _APPROVAL | {"step_id"},
    "approval.updated": _APPROVAL,
    "reasoning.final": frozenset({"conversation_id", "run_id", "message_id", "text"}),
    "context.updated": _CONTEXT,
    "health.status": frozenset({"hermes", "hermes_version"}),
    "error": frozenset({"code", "message", "ref_id"}),
}

# Frames sent directly on one socket (no `seq`, never replayed).
CONNECTION_FRAMES = ("hello.ok", "ack", "error", "pong", "keepalive")

# Client frames and the keys of their `data` (required, optional).
CLIENT_FRAMES: Mapping[str, tuple[frozenset[str], frozenset[str]]] = {
    "hello": (frozenset({"device_token"}), frozenset({"last_seq", "epoch"})),
    "chat.send": (
        frozenset({"text", "client_msg_id"}),
        frozenset({"conversation_id", "thinking"}),
    ),
    "conversation.set": (frozenset({"conversation_id"}), frozenset({"thinking", "archived"})),
    "run.stop": (frozenset({"run_id"}), frozenset()),
    "approval.respond": (frozenset({"request_id", "choice"}), frozenset()),
    "sync": (frozenset({"conversation_id"}), frozenset({"after_message_id"})),
    "ping": (frozenset(), frozenset()),
}


class ProtocolViolation(AssertionError):
    pass


def check_event(type_: str, data: Mapping[str, object]) -> str | None:
    """None if `data` matches the declaration of `type_`, else what is wrong."""
    want = SERVER_EVENTS.get(type_)
    if want is None:
        return f"undeclared event type {type_!r}"
    got = frozenset(data)
    if got != want:
        missing, extra = sorted(want - got), sorted(got - want)
        return f"{type_}: missing {missing}, unexpected {extra}"
    return None
