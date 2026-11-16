"""Notification mirroring, phone to desktop (DA2).

The phone's app sends what its notification listener saw - only for the apps the user
chose there - and the desktop agent (delve-infra `bin/delve-desk`, a user service in the
operator's graphical session) fetches it and shows it with the desktop's notification
daemon. The backend only passes it on:

* nothing of a notification is stored: commands wait in memory for the agent, at most
  `QUEUE_TTL_S` seconds, and at most `QUEUE_MAX` of them (the oldest are dropped);
* nothing of a notification is logged: the log lines carry counts;
* the agent gets no Android key, only `id`: 16 hex characters of a hash of the device and
  the key, stable while this process runs. A new post of the same key replaces the
  notification on the desktop, a remove closes it.

Commands for the agent (`GET /api/v1/desk/next`):

    {"verb": "notify.show", "id", "app", "title", "body", "category", "hidden", "icon"}
    {"verb": "notify.close", "id"}

`hidden`: Android hid the content from the listener (a one-time code); title and body are
empty and the agent says so in the notification's text.

`icon`: the posting app's icon, when the phone sent one (a small PNG, API 12) - not the
picture itself but 32 hex characters of its hash, or "". The agent fetches the picture
once (`GET /api/v1/desk/icon/{icon}`) and keeps it; here the newest `ICONS_MAX` pictures
stay in memory. An icon is the app's, the same for every notification of it: it says
which apps post, nothing of what they posted.

Delivery is at most once: a command is gone when it was handed to a poll. An agent that
was away for longer than the TTL starts with an empty desktop, which is what a mirror of
"now" should do.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import logging
import secrets
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("delve.mirror")

QUEUE_MAX = 200
QUEUE_TTL_S = 60.0
# the agent polls with wait <= POLL_MAX_S; "connected" allows one missed round
POLL_MAX_S = 30.0
DESK_FRESH_S = 2 * POLL_MAX_S + 5
STOP_CHECK_S = 1.0
EVENTS_MAX = 32
KEY_MAX = 512
APP_MAX = 64
TITLE_MAX = 256
BODY_MAX = 2000
CATEGORY_MAX = 32
# an app's icon: a PNG of at most ICON_MAX bytes, sent as base64
ICON_MAX = 32 * 1024
ICON_B64_MAX = 4 * ((ICON_MAX + 2) // 3)
ICONS_MAX = 64
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def decode_icon(encoded: str) -> bytes:
    """The PNG behind an event's `icon`; ValueError when it is not one."""
    try:
        png = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("icon is not base64") from exc
    if not png.startswith(PNG_MAGIC) or len(png) > ICON_MAX:
        raise ValueError(f"icon must be a PNG of at most {ICON_MAX} bytes")
    return png


@dataclass(slots=True)
class _Queued:
    expires: float
    command: dict[str, Any]


class MirrorService:
    def __init__(self, *, ttl_s: float = QUEUE_TTL_S, queue_max: int = QUEUE_MAX) -> None:
        self._ttl_s, self._max = ttl_s, queue_max
        self._queue: deque[_Queued] = deque()
        self._wake = asyncio.Event()
        self._closed = False
        self._desk_seen: float | None = None
        self._poll = 0  # the newest poll's number: an older one must not take commands
        # the server is going down (cli.py sets this to uvicorn's flag): end waiting polls
        self.stopping: Callable[[], bool] = lambda: False
        # ids must not be guessable from a key and need not survive a restart
        self._salt = secrets.token_bytes(16)
        # the apps' icons the agent may still ask for: hash -> PNG, the newest last
        self._icons: OrderedDict[str, bytes] = OrderedDict()

    # --- the phone's side -----------------------------------------------------------

    def notification_id(self, device_id: uuid.UUID, key: str) -> str:
        digest = hashlib.sha256(self._salt + device_id.bytes + key.encode("utf-8"))
        return digest.hexdigest()[:16]

    def keep_icon(self, png: bytes) -> str:
        """Keeps an app's icon for the agent; returns the name it goes by."""
        name = hashlib.sha256(png).hexdigest()[:32]
        self._icons[name] = png
        self._icons.move_to_end(name)
        while len(self._icons) > ICONS_MAX:
            self._icons.popitem(last=False)
        return name

    def icon(self, name: str) -> bytes | None:
        return self._icons.get(name)

    def accept(self, device_id: uuid.UUID, events: list[dict[str, Any]]) -> int:
        """Queue the phone's events for the agent; returns how many were queued."""
        now = time.monotonic()
        self._expire(now)
        for event in events:
            nid = self.notification_id(device_id, event["key"])
            if event["op"] == "remove":
                # a show still waiting here is not worth showing; the close goes out all
                # the same: an earlier post of the key may be on the desktop already
                self._drop(nid)
                self._push(now, {"verb": "notify.close", "id": nid})
                continue
            self._drop(nid)  # the newer post of a key wins
            hidden = bool(event.get("hidden"))
            encoded = event.get("icon")
            self._push(
                now,
                {
                    "verb": "notify.show",
                    "id": nid,
                    "app": event["app"],
                    "title": "" if hidden else event.get("title") or "",
                    "body": "" if hidden else event.get("text") or "",
                    "category": event.get("category") or "",
                    "hidden": hidden,
                    "icon": self.keep_icon(decode_icon(encoded)) if encoded else "",
                },
            )
        if self._queue:
            self._wake.set()
        log.info(
            "mirror events",
            extra={"events": len(events), "queued": len(self._queue), "desk": self.connected()},
        )
        return len(events)

    def _push(self, now: float, command: dict[str, Any]) -> None:
        self._queue.append(_Queued(now + self._ttl_s, command))
        while len(self._queue) > self._max:
            self._queue.popleft()

    def _drop(self, nid: str) -> None:
        kept = [q for q in self._queue if q.command["id"] != nid]
        if len(kept) != len(self._queue):
            self._queue = deque(kept)

    def _expire(self, now: float) -> None:
        while self._queue and self._queue[0].expires <= now:
            self._queue.popleft()

    # --- the agent's side -----------------------------------------------------------

    async def next(self, wait_s: float) -> list[dict[str, Any]]:
        """Everything queued, or what arrives within `wait_s` seconds; [] after that."""
        deadline = time.monotonic() + max(0.0, min(wait_s, POLL_MAX_S))
        # An agent that reconnects leaves its old request waiting here (nothing tells a
        # waiting handler that its client went away); the old one must hand nothing to a
        # dead connection, so it ends as soon as a newer poll arrives.
        self._poll += 1
        mine = self._poll
        self._wake.set()
        await asyncio.sleep(0)
        while True:
            if mine != self._poll:
                return []
            now = time.monotonic()
            self._desk_seen = now
            self._expire(now)
            if self._queue:
                out = [q.command for q in self._queue]
                self._queue.clear()
                self._wake.clear()
                return out
            self._wake.clear()
            left = deadline - now
            if left <= 0 or self._closed:
                return []
            # in slices: a server that is shutting down waits for its open requests, and
            # nothing else tells this one to end
            try:
                async with asyncio.timeout(min(left, STOP_CHECK_S)):
                    await self._wake.wait()
            except TimeoutError:
                if self.stopping():
                    return []

    # --- status ---------------------------------------------------------------------

    def _desk_fresh(self, now: float) -> bool:
        return self._desk_seen is not None and now - self._desk_seen <= DESK_FRESH_S

    def connected(self) -> bool:
        """The agent asked for commands within the last two rounds."""
        return self._desk_fresh(time.monotonic())

    def status(self) -> dict[str, Any]:
        return mirror_out(
            connected=self.connected(),
            seen_s=None if self._desk_seen is None else time.monotonic() - self._desk_seen,
        )

    def close(self) -> None:
        """Shutdown: waiting polls return at once."""
        self._closed = True
        self._queue.clear()
        self._wake.set()


def mirror_out(*, connected: bool, seen_s: float | None) -> dict[str, Any]:
    """`GET /api/v1/mirror` and the answer to the phone's events."""
    return {
        "desk": {
            "connected": connected,
            # seconds since the agent last asked; null: not since the backend started
            "last_seen_s": None if seen_s is None else int(seen_s),
        }
    }
