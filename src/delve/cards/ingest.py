"""Card ingest (plan 5.5, B2): the agent's `events.jsonl` into `cards`.

The file is the agent's (`~agent/.hermes/cards/events.jsonl`), appended to by the
`agentcards` plugin and the no-agent scripts; the unit sees it read-only at
`DELVE_CARDS_PATH` (a bind mount, delve-infra drop-in `cards.conf`). Every
`poll_s` the new complete lines are read from the stored offset, each is checked
(envelope + the vendored payload validator), and the valid ones are inserted with
`ON CONFLICT DO NOTHING` (the card id is the dedup key), together with the new
offset in one transaction. A new card is published as `card.new`.

A shorter file or another inode (rotation, replacement) starts over from 0; the ids
keep that idempotent. A line without its newline yet waits for the next poll. Invalid
lines are logged and skipped, never retried.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.cards import schemas
from delve.db.models import Card, CardIngestOffset
from delve.events import EventHub
from delve.serialize import card_out

log = logging.getLogger("delve.cards")

MAX_LINE = 8192  # the plugin's limit
MAX_READ = 4 * 1024 * 1024  # per poll; the rest comes next time
ORIGINS = frozenset({"agent", "script", "backend"})
_ID = re.compile(r"[0-9a-f]{32}")


class InvalidCard(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CardIn:
    id: str
    v: int
    type: str
    severity: str
    ts: datetime
    origin: str
    session_id: str | None
    payload: dict[str, Any]


def parse_line(line: bytes) -> CardIn:
    """One `events.jsonl` line, checked like the plugin checks it when writing. The
    payload's time bounds are taken relative to the card's own `ts`, so history
    older than 30 days still ingests."""
    if len(line) > MAX_LINE:
        raise InvalidCard(f"line longer than {MAX_LINE} bytes")
    try:
        ev = json.loads(line)
    except ValueError as exc:
        raise InvalidCard("not JSON") from exc
    if not isinstance(ev, dict):
        raise InvalidCard("not an object")
    if ev.get("v") != schemas.SCHEMA_VERSION:
        raise InvalidCard(f"unknown schema version {ev.get('v')!r}")
    cid = ev.get("id")
    if not isinstance(cid, str) or not _ID.fullmatch(cid):
        raise InvalidCard("id: 32 lowercase hex digits expected")
    ctype = ev.get("type")
    if ctype not in schemas.VALIDATORS:
        raise InvalidCard(f"unknown type {ctype!r}")
    if ev.get("severity") not in schemas.SEVERITIES:
        raise InvalidCard(f"unknown severity {ev.get('severity')!r}")
    if ev.get("origin") not in ORIGINS:
        raise InvalidCard(f"unknown origin {ev.get('origin')!r}")
    raw_ts = ev.get("ts")
    try:
        ts = datetime.fromisoformat(raw_ts) if isinstance(raw_ts, str) else None
    except ValueError:
        ts = None
    if ts is None or ts.tzinfo is None:
        raise InvalidCard("ts: ISO 8601 with a timezone expected")
    sid = ev.get("session_id")
    if sid is not None and (not isinstance(sid, str) or len(sid) > 128):
        raise InvalidCard("session_id: string of at most 128 characters expected")
    try:
        payload = schemas.VALIDATORS[ctype](ev.get("payload"), ts)  # type: ignore[no-untyped-call]
    except schemas.CardValidationError as exc:
        raise InvalidCard(f"payload: {exc}") from exc
    return CardIn(
        id=cid,
        v=schemas.SCHEMA_VERSION,
        type=ctype,
        severity=ev["severity"],
        ts=ts.astimezone(UTC),
        origin=ev["origin"],
        session_id=sid,
        payload=payload,
    )


class CardIngester:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hub: EventHub,
        path: Path,
        *,
        poll_s: float = 2.0,
    ) -> None:
        self._sm = sessionmaker
        self._hub = hub
        self.path = path
        self._poll_s = poll_s
        self._task: asyncio.Task[None] | None = None
        self._missing_logged = False

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="card-ingest")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.poll()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("card ingest failed", extra={"path": str(self.path)})
            await asyncio.sleep(self._poll_s)

    async def poll(self) -> list[dict[str, Any]]:
        """Ingest what is new; returns the new cards (as published)."""
        try:
            st = os.stat(self.path)
        except OSError as exc:
            if not self._missing_logged:
                log.warning(
                    "cards file not readable",
                    extra={"path": str(self.path), "error": type(exc).__name__},
                )
                self._missing_logged = True
            return []
        self._missing_logged = False
        key = str(self.path)
        async with self._sm() as s:
            row = await s.get(CardIngestOffset, key)
        offset = row.offset_bytes if row is not None else 0
        if row is not None and (row.inode != st.st_ino or st.st_size < offset):
            log.info("cards file replaced or truncated: reading from the start")
            offset = 0
        if st.st_size == offset:
            return []

        chunk = await asyncio.to_thread(_read, self.path, offset, MAX_READ)
        end = chunk.rfind(b"\n")
        if end < 0:
            if len(chunk) >= MAX_READ:  # one absurd line: skip it whole
                log.warning("cards line too long, skipped", extra={"offset": offset})
                end = len(chunk) - 1
            else:
                return []  # the writer is mid-line
        consumed = chunk[: end + 1]
        cards: list[CardIn] = []
        bad = 0
        for n, line in enumerate(consumed.split(b"\n")[:-1]):
            if not line.strip():
                continue
            try:
                cards.append(parse_line(line))
            except InvalidCard as exc:
                bad += 1
                log.warning(
                    "invalid card skipped",
                    extra={"path": key, "line_in_chunk": n, "error": str(exc)},
                )

        async with self._sm() as s:
            new_ids: list[str] = []
            for c in cards:
                inserted = (
                    await s.execute(
                        insert(Card)
                        .values(
                            id=c.id,
                            v=c.v,
                            type=c.type,
                            severity=c.severity,
                            ts=c.ts,
                            origin=c.origin,
                            session_id=c.session_id,
                            payload=c.payload,
                        )
                        .on_conflict_do_nothing(index_elements=["id"])
                        .returning(Card.id)
                    )
                ).scalar_one_or_none()
                if inserted is not None:
                    new_ids.append(inserted)
            stmt = insert(CardIngestOffset).values(
                path=key, inode=st.st_ino, offset_bytes=offset + len(consumed),
                updated_at=datetime.now(UTC),
            )  # fmt: skip
            await s.execute(
                stmt.on_conflict_do_update(
                    index_elements=["path"],
                    set_={
                        "inode": stmt.excluded.inode,
                        "offset_bytes": stmt.excluded.offset_bytes,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            )
            rows = (
                (await s.execute(select(Card).where(Card.id.in_(new_ids)))).scalars().all()
                if new_ids
                else []
            )
            await s.commit()
        by_id = {r.id: r for r in rows}
        out = [card_out(by_id[i]) for i in new_ids]
        for item in out:
            self._hub.publish("card.new", item)
        if new_ids or bad:
            log.info(
                "cards ingested",
                extra={"new": len(new_ids), "known": len(cards) - len(new_ids), "invalid": bad},
            )
        return out


def _read(path: Path, offset: int, limit: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(limit)
