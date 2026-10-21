"""Card ingest (cards/ingest.py), GET /cards, and the water-level push."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.auth import create_device, set_fcm_token
from delve.cards.ingest import CardIngester, InvalidCard, parse_line
from delve.db.models import Card, CardIngestOffset, Device
from delve.events import EventHub
from delve.push import Presence, PushService, notify_settings

from .test_push import FakeGoogle, client_for

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "cards"


def water_event(
    *,
    ts: datetime | None = None,
    origin: str = "script",
    level: float = 98.0,
    card_id: str | None = None,
    **payload: Any,
) -> dict[str, Any]:
    ts = (ts or datetime.now(UTC)).replace(microsecond=0)
    return {
        "v": 1,
        "id": card_id or uuid.uuid4().hex,
        "type": "water",
        "severity": "info",
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "origin": origin,
        "payload": {
            "station": "Siófok",
            "level_cm": level,
            "measured_at": (ts - timedelta(minutes=30)).isoformat(),
            "lake_avg_cm": 101.0,
            "trend_cm_24h": -2.0,
            **payload,
        },
    }


def line(ev: dict[str, Any]) -> bytes:
    return (json.dumps(ev, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


# --- parse_line ---------------------------------------------------------------------


def test_parse_valid_and_history() -> None:
    old = datetime.now(UTC) - timedelta(days=90)  # older than the plugin's 30-day window
    c = parse_line(line(water_event(ts=old))[:-1])
    assert (c.type, c.origin, c.severity) == ("water", "script", "info")
    assert c.ts == old.replace(microsecond=0)
    assert c.payload["level_cm"] == 98.0 and c.payload["station"] == "Siófok"


def test_parse_updates_card() -> None:
    ts = datetime.now(UTC).replace(microsecond=0)
    payload = {"count": 44, "packages": ["clang", "firefox"], "checked_at": ts.isoformat()}
    ev = {**water_event(ts=ts), "type": "updates", "payload": payload}
    card = parse_line(line(ev))
    assert (card.type, card.payload["count"], card.payload["packages"]) == (
        "updates", 44, ["clang", "firefox"]
    )  # fmt: skip
    with pytest.raises(InvalidCard):
        parse_line(line({**ev, "payload": {**ev["payload"], "count": "44"}}))


def test_parse_recorded_fixtures() -> None:
    for f in FIXTURES.glob("*.jsonl"):
        for raw in f.read_bytes().splitlines():
            c = parse_line(raw)
            assert c.origin == "agent" and c.type == "water"


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"v": 2}, "schema version"),
        ({"id": "not-hex"}, "id"),
        ({"type": "disk"}, "unknown type"),
        ({"severity": "fatal"}, "severity"),
        ({"origin": "human"}, "origin"),
        ({"ts": "2026-10-16T17:28:49"}, "timezone"),
        ({"session_id": "x" * 129}, "session_id"),
        ({"payload": {"station": "Siófok"}}, "level_cm"),
        ({"payload": {"station": "S", "level_cm": 1, "measured_at": "x", "extra": 1}}, "payload"),
    ],
)
def test_parse_rejects(change: dict[str, Any], error: str) -> None:
    with pytest.raises(InvalidCard, match=error):
        parse_line(line({**water_event(), **change}))


def test_parse_rejects_garbage() -> None:
    with pytest.raises(InvalidCard, match="JSON"):
        parse_line(b"{oops")
    with pytest.raises(InvalidCard, match="object"):
        parse_line(b"[1]")
    with pytest.raises(InvalidCard, match="longer"):
        parse_line(b"x" * 9000)


# --- the ingester -----------------------------------------------------------------------

pg = pytest.mark.pg


@pytest.fixture
def hub() -> EventHub:
    return EventHub(validate=True)


@pytest.fixture
def events_file(tmp_path: Path) -> Path:
    return tmp_path / "cards" / "events.jsonl"


def append(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as f:
        f.write(data)


async def card_ids(db: async_sessionmaker[AsyncSession]) -> list[str]:
    async with db() as s:
        return list((await s.execute(select(Card.id).order_by(Card.ts))).scalars())


@pg
async def test_ingest_reads_new_complete_lines(
    db: async_sessionmaker[AsyncSession], hub: EventHub, events_file: Path
) -> None:
    ing = CardIngester(db, hub, events_file)
    assert await ing.poll() == []  # no file yet: nothing, no error

    t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=2)
    a, b = water_event(ts=t0), water_event(ts=t0 + timedelta(hours=1), level=97.0)
    append(events_file, line(a) + line(b))
    new = await ing.poll()
    assert [c["id"] for c in new] == [a["id"], b["id"]]
    assert new[1]["payload"]["level_cm"] == 97.0
    sub_events = [e for e in hub.since(0) or [] if e.type == "card.new"]
    assert [e.data["id"] for e in sub_events] == [a["id"], b["id"]]
    assert await ing.poll() == []

    c = water_event()
    raw = line(c)
    append(events_file, raw[:20])  # the writer is mid-line
    assert await ing.poll() == []
    append(events_file, raw[20:])
    assert [x["id"] for x in await ing.poll()] == [c["id"]]

    async with db() as s:
        off = await s.get(CardIngestOffset, str(events_file))
    assert off is not None and off.offset_bytes == os.path.getsize(events_file)  # noqa: ASYNC240


@pg
async def test_ingest_skips_bad_lines_and_known_ids(
    db: async_sessionmaker[AsyncSession], hub: EventHub, events_file: Path
) -> None:
    ing = CardIngester(db, hub, events_file)
    a = water_event()
    append(events_file, line(a) + b"{broken\n" + line({**water_event(), "v": 9}) + line(a))
    new = await ing.poll()
    assert [c["id"] for c in new] == [a["id"]]
    assert await card_ids(db) == [a["id"]]
    assert await ing.poll() == []  # the bad lines are not retried


@pg
async def test_ingest_starts_over_on_a_replaced_file(
    db: async_sessionmaker[AsyncSession], hub: EventHub, events_file: Path
) -> None:
    ing = CardIngester(db, hub, events_file)
    a, b = water_event(), water_event()
    append(events_file, line(a) + line(b))
    await ing.poll()
    # rotation: a new, shorter file with one known and one new card
    c = water_event()
    tmp = events_file.with_suffix(".new")
    tmp.write_bytes(line(b) + line(c))
    os.replace(tmp, events_file)
    assert [x["id"] for x in await ing.poll()] == [c["id"]]
    assert sorted(await card_ids(db)) == sorted([a["id"], b["id"], c["id"]])


@pg
async def test_ingest_task_runs_and_stops(
    db: async_sessionmaker[AsyncSession], hub: EventHub, events_file: Path
) -> None:
    ing = CardIngester(db, hub, events_file, poll_s=0.02)
    await ing.start()
    try:
        a = water_event()
        append(events_file, line(a))
        async with asyncio.timeout(5):
            while a["id"] not in await card_ids(db):  # noqa: ASYNC110
                await asyncio.sleep(0.02)
    finally:
        await ing.close()


# --- REST ---------------------------------------------------------------------------------


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


NOW = datetime.now(UTC).replace(microsecond=0)


@pytest.fixture
async def seeded(db: async_sessionmaker[AsyncSession]) -> list[dict[str, Any]]:
    evs = [water_event(ts=NOW - timedelta(days=d), level=100.0 - d) for d in range(4)]
    async with db() as s:
        for ev in evs:
            c = parse_line(line(ev))
            s.add(Card(id=c.id, v=1, type=c.type, severity=c.severity, ts=c.ts,
                       origin=c.origin, payload=c.payload))  # fmt: skip
        await s.commit()
    return evs


@pg
def test_rest_cards(client: TestClient, token: str, seeded: list[dict[str, Any]]) -> None:
    evs, now = seeded, NOW
    assert client.get("/api/v1/cards").status_code == 401
    data = client.get("/api/v1/cards", headers=_auth(token)).json()["data"]
    assert [c["id"] for c in data] == [e["id"] for e in evs]  # newest first
    assert set(data[0]) == {
        "id", "type", "severity", "ts", "origin", "session_id", "payload", "received_at",
    }  # fmt: skip
    since = (now - timedelta(days=1, hours=1)).isoformat()
    r = client.get("/api/v1/cards", headers=_auth(token), params={"type": "water", "since": since})
    assert len(r.json()["data"]) == 2
    r = client.get("/api/v1/cards", headers=_auth(token), params={"type": "disk"})
    assert r.json()["data"] == []
    r = client.get("/api/v1/cards", headers=_auth(token), params={"limit": 1})
    assert [c["id"] for c in r.json()["data"]] == [evs[0]["id"]]
    r = client.get("/api/v1/cards", headers=_auth(token), params={"since": "2026-10-01T00:00"})
    assert r.status_code == 400


# --- push ---------------------------------------------------------------------------------


@pg
async def test_water_push(
    db: async_sessionmaker[AsyncSession], hub: EventHub, events_file: Path
) -> None:
    google = FakeGoogle()
    push = PushService(db, client_for(google), Presence(), approval_delay_s=0)
    hub.add_listener(push.listener)
    async with db() as s:
        on, _ = await create_device(s, "pixel")
        await set_fcm_token(s, on.id, "fcm-on")
        off, _ = await create_device(s, "tablet")
        await set_fcm_token(s, off.id, "fcm-off")
        await s.execute(  # water is off by default
            update(Device)
            .where(Device.id == on.id)
            .values(notify={**notify_settings(None), "water": True})
        )
        await s.commit()
    await push.start()
    try:
        ing = CardIngester(db, hub, events_file)
        old = water_event(ts=datetime.now(UTC) - timedelta(days=1))
        by_agent = water_event(origin="agent")
        fresh = water_event(level=98.0, lake_avg_trend_cm_24h=-1.5)
        append(events_file, line(old) + line(by_agent) + line(fresh))
        await ing.poll()
        async with asyncio.timeout(5):
            await push.drain()
    finally:
        await push.close()
    (msg,) = google.sends  # only the new scheduled reading, only where water is on
    assert msg["token"] == "fcm-on"
    assert msg["android"] == {"priority": "HIGH", "ttl": f"{12 * 3600}s", "collapse_key": "water"}
    d = msg["data"]
    assert (d["kind"], d["card_id"], d["station"], d["level_cm"], d["lake_avg_cm"]) == (
        "card",
        fresh["id"],
        "Siófok",
        "98",
        "101",
    )
    assert (d["trend_cm_24h"], d["lake_avg_trend_cm_24h"]) == ("-2", "-1.5")
    assert d["measured_at"].startswith(str(datetime.now(UTC).year))
