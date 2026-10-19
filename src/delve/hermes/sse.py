"""Server-sent events: a small, spec-following parser plus the Hermes event decoding.

Hermes framing (v0.22.2): `: open`, blocks of `id: <seq>` + `data: {json}`, `: keepalive`
every 10 s, `: stream closed` at the end. Key order inside the JSON is not stable.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("delve.hermes.sse")

TERMINAL_EVENTS = frozenset({"run.completed", "run.cancelled", "run.failed", "run.interrupted"})
# reasoning.available carries the final answer, never the thinking text (plan 1.2, 3.4).
DROPPED_EVENTS = frozenset({"reasoning.available"})
KNOWN_EVENTS = frozenset(
    {
        "tool.started",
        "tool.completed",
        "approval.request",
        "approval.responded",
        "message.delta",
        "message.interim",
        "reasoning.available",
        *TERMINAL_EVENTS,
    }
)


@dataclass(frozen=True, slots=True)
class SSEMessage:
    """One dispatched SSE message (a blank-line-terminated block with data)."""

    data: str
    id: str | None = None
    event: str = "message"


@dataclass(slots=True)
class SSEParser:
    """Incremental parser following the WHATWG event-stream rules.

    `feed()` accepts arbitrary chunks (lines may be split across chunks); comment lines
    are counted in `comments` and otherwise ignored.
    """

    last_event_id: str | None = None
    comments: list[str] = field(default_factory=list)
    _buf: str = ""
    _data: list[str] = field(default_factory=list)
    _event: str = ""
    _id: str | None = None
    _has_fields: bool = False

    def feed(self, chunk: str) -> list[SSEMessage]:
        self._buf += chunk
        out: list[SSEMessage] = []
        while True:
            line, sep, rest = _split_line(self._buf)
            if not sep:
                break
            self._buf = rest
            msg = self._line(line)
            if msg is not None:
                out.append(msg)
        return out

    def flush(self) -> list[SSEMessage]:
        """End of stream: an unterminated final block is discarded, per spec."""
        self._buf = ""
        self._reset()
        return []

    def _line(self, line: str) -> SSEMessage | None:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            self.comments.append(line[1:].lstrip(" "))
            return None
        name, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        self._has_fields = True
        if name == "data":
            self._data.append(value)
        elif name == "event":
            self._event = value
        elif name == "id" and "\x00" not in value:
            self._id = value
        # "retry" and unknown fields are ignored
        return None

    def _dispatch(self) -> SSEMessage | None:
        if self._id is not None:
            self.last_event_id = self._id
        if not self._has_fields or not self._data:
            self._reset()
            return None
        msg = SSEMessage(
            data="\n".join(self._data), id=self.last_event_id, event=self._event or "message"
        )
        self._reset()
        return msg

    def _reset(self) -> None:
        self._data = []
        self._event = ""
        self._id = None
        self._has_fields = False


def _split_line(buf: str) -> tuple[str, str, str]:
    """Split off one line terminated by CRLF, LF or CR. A trailing lone CR waits for more."""
    for i, ch in enumerate(buf):
        if ch == "\n":
            return buf[:i], "\n", buf[i + 1 :]
        if ch == "\r":
            if i + 1 == len(buf):
                return buf, "", ""  # might be the first half of CRLF
            if buf[i + 1] == "\n":
                return buf[:i], "\r\n", buf[i + 2 :]
            return buf[:i], "\r", buf[i + 1 :]
    return buf, "", ""


@dataclass(frozen=True, slots=True)
class HermesEvent:
    """A decoded run event. `data` is the full JSON object, read by key."""

    event: str
    seq: int
    run_id: str
    timestamp: float | None
    data: dict[str, Any]

    @property
    def terminal(self) -> bool:
        return self.event in TERMINAL_EVENTS


class HermesProtocolError(ValueError):
    """A message on the event stream does not follow the expected contract."""


def decode_event(msg: SSEMessage) -> HermesEvent | None:
    """Decode one SSE message; returns None for events the backend drops on purpose."""
    try:
        obj = json.loads(msg.data)
    except json.JSONDecodeError as exc:
        raise HermesProtocolError(f"event data is not JSON (id={msg.id})") from exc
    if not isinstance(obj, dict):
        raise HermesProtocolError(f"event data is not an object (id={msg.id})")
    name = obj.get("event")
    if not isinstance(name, str) or not name:
        raise HermesProtocolError(f"event without 'event' field (id={msg.id})")
    seq = obj.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool):
        if msg.id is None or not msg.id.isdigit():
            raise HermesProtocolError(f"event {name!r} without seq")
        seq = int(msg.id)
    if name in DROPPED_EVENTS:
        return None
    if name not in KNOWN_EVENTS:
        log.warning("unknown hermes event", extra={"event_name": name, "seq": seq})
    ts = obj.get("timestamp")
    return HermesEvent(
        event=name,
        seq=seq,
        run_id=str(obj.get("run_id", "")),
        timestamp=float(ts) if isinstance(ts, int | float) else None,
        data=obj,
    )


def parse_events(text: str) -> Iterator[HermesEvent]:
    """Decode a complete SSE document (fixtures, tests)."""
    parser = SSEParser()
    for msg in parser.feed(text):
        ev = decode_event(msg)
        if ev is not None:
            yield ev


async def aiter_events(
    chunks: AsyncIterable[str], *, after_seq: int | None = None
) -> AsyncIterator[HermesEvent]:
    """Decode events from a text stream, skipping seq <= after_seq (reconnect replays)
    and stopping after the first terminal event."""
    parser = SSEParser()
    async for chunk in chunks:
        for msg in parser.feed(chunk):
            ev = decode_event(msg)
            if ev is None or (after_seq is not None and ev.seq <= after_seq):
                continue
            yield ev
            if ev.terminal:
                return


def iter_chunks(text: str, size: int) -> Iterable[str]:
    """Split text into fixed-size chunks (tests: lines split across reads)."""
    return (text[i : i + size] for i in range(0, len(text), size))
