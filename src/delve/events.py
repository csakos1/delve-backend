"""In-process event hub for WebSocket clients (plan 6.7).

Every server event gets a global `seq`. A ring buffer (10 000 events / 1 h) lets a
reconnecting client resume with `last_seq`; the `epoch` changes on every restart, so a
client from before a restart is told to resync instead of being given wrong events.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from delve import protocol

log = logging.getLogger("delve.events")


@dataclass(frozen=True, slots=True)
class Envelope:
    seq: int
    type: str
    data: dict[str, Any]
    ts: float

    def frame(self) -> dict[str, Any]:
        return {"type": self.type, "seq": self.seq, "data": self.data}


@dataclass(eq=False)
class Subscription:
    queue: asyncio.Queue[Envelope]
    overflowed: bool = False
    closed: asyncio.Event = field(default_factory=asyncio.Event)


class EventHub:
    def __init__(
        self,
        *,
        maxlen: int = 10_000,
        max_age_s: float = 3600.0,
        queue_size: int = 1000,
        clock: Callable[[], float] = time.monotonic,
        validate: bool = False,
    ) -> None:
        """`validate` checks every event against `delve.protocol.SERVER_EVENTS`."""
        self._validate = validate
        self.epoch = secrets.token_hex(8)
        self._seq = 0
        self._buf: deque[Envelope] = deque(maxlen=maxlen)
        self._max_age_s = max_age_s
        self._queue_size = queue_size
        self._clock = clock
        self._subs: set[Subscription] = set()

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def subscribers(self) -> int:
        return len(self._subs)

    def publish(self, type: str, data: dict[str, Any]) -> Envelope:
        if self._validate and (problem := protocol.check_event(type, data)) is not None:
            if protocol.STRICT:
                raise protocol.ProtocolViolation(problem)
            log.error("protocol violation", extra={"problem": problem})
        self._seq += 1
        env = Envelope(self._seq, type, data, self._clock())
        self._buf.append(env)
        for sub in list(self._subs):
            try:
                sub.queue.put_nowait(env)
            except asyncio.QueueFull:
                # a slow client is dropped; it resumes from its last seq
                sub.overflowed = True
                sub.closed.set()
                self._subs.discard(sub)
                log.warning("subscriber overflow", extra={"seq": env.seq})
        return env

    def subscribe(self) -> Subscription:
        sub = Subscription(asyncio.Queue(self._queue_size))
        self._subs.add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        self._subs.discard(sub)
        sub.closed.set()

    def since(self, last_seq: int, epoch: str | None = None) -> list[Envelope] | None:
        """Events after `last_seq`, or None if they cannot be replayed (resync)."""
        if epoch is not None and epoch != self.epoch:
            return None
        if last_seq < 0 or last_seq > self._seq:
            return None
        self._prune()
        if last_seq == self._seq:
            return []
        oldest = self._buf[0].seq if self._buf else self._seq + 1
        if last_seq + 1 < oldest:
            return None
        return [e for e in self._buf if e.seq > last_seq]

    def _prune(self) -> None:
        cutoff = self._clock() - self._max_age_s
        while self._buf and self._buf[0].ts < cutoff:
            self._buf.popleft()
