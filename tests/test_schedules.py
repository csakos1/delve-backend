"""Schedules and the morning brief (B4): cron, firing, catch-up, brief, REST, push."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve import protocol
from delve.app import create_app
from delve.auth import create_device, set_fcm_token
from delve.brief import BriefService, brief_payload, brief_severity, open_incidents
from delve.config import Settings
from delve.db.models import Card, Conversation, Device, Run, Schedule
from delve.events import EventHub
from delve.push import Presence, PushService, notify_settings
from delve.schedules import (
    InvalidSchedule,
    ScheduleConflict,
    ScheduleService,
    check_cron,
    describe,
    next_fire,
)
from delve.system import HelperClient

from .fake_helper import FakeHelper
from .fake_hermes import FakeHermes, StreamingASGITransport, load_json
from .fake_llama import FakeLlama
from .harness import FAST, ROUTE, Harness, make_harness
from .test_api import auth
from .test_push import FakeGoogle, client_for

pg = pytest.mark.pg
TZ = ZoneInfo("Europe/Budapest")
R = load_json("system/helper-responses.json")


def local(*args: int) -> datetime:
    return datetime(*args, tzinfo=TZ).astimezone(UTC)


# --- cron (pure) -----------------------------------------------------------------------


def test_check_cron() -> None:
    assert check_cron("  52  8,12,20 * *   * ") == "52 8,12,20 * * *"
    assert check_cron("*/15 * * * *") == "*/15 * * * *"
    for bad in ("* * * *", "0 0 * * * *", "61 * * * *", "0 0 31 2 *", "a b c d e", "@daily"):
        with pytest.raises(InvalidSchedule) as exc:
            check_cron(bad)
        assert exc.value.code == "invalid_cron", bad
    for often in ("* * * * *", "*/5 * * * *", "0,5 9 * * *"):
        with pytest.raises(InvalidSchedule) as exc:
            check_cron(often)
        assert exc.value.code == "too_frequent", often


def test_next_fire_is_strictly_after_and_follows_local_time() -> None:
    assert next_fire("30 7 * * *", local(2026, 10, 21, 7, 30)) == local(2026, 10, 22, 7, 30)
    assert next_fire("30 7 * * *", local(2026, 10, 21, 7, 29, 59)) == local(2026, 10, 21, 7, 30)
    # across the end of summer time: 07:30 local stays 07:30, UTC moves an hour
    sat = next_fire("30 7 * * *", local(2026, 10, 24, 12, 0))
    assert sat == datetime(2026, 10, 25, 6, 30, tzinfo=UTC)


def test_next_fire_on_dst_days() -> None:
    # 02:30 exists twice on 2026-10-25: fires once
    first = next_fire("30 2 * * *", local(2026, 10, 24, 12, 0))
    assert first == datetime(2026, 10, 25, 0, 30, tzinfo=UTC)  # 02:30 +02:00
    assert next_fire("30 2 * * *", first) == datetime(2026, 10, 26, 1, 30, tzinfo=UTC)
    # 02:30 does not exist on 2027-03-28: fires at 03:00 summer time
    spring = next_fire("30 2 * * *", local(2027, 3, 27, 12, 0))
    assert spring == datetime(2027, 3, 28, 1, 0, tzinfo=UTC)
    assert next_fire("30 2 * * *", spring) == datetime(2027, 3, 29, 0, 30, tzinfo=UTC)


def test_describe() -> None:
    assert describe("52 8,12,20 * * *") == "At 08:52, 12:52, and 20:52 every day"
    assert describe("0 18 * * 0") == "At 18:00 on Sunday"


# --- brief (pure) ----------------------------------------------------------------------

NOW = datetime(2026, 10, 22, 5, 30, tzinfo=UTC)  # 07:30 local
SYSTEM = {"llama": {"status": "running"}, "hermes": {"status": "degraded"}}


def card(ctype: str, payload: dict[str, Any], *, ago: timedelta = timedelta(hours=1)) -> Card:
    return Card(
        id=uuid.uuid4().hex, v=1, type=ctype, severity="info", ts=NOW - ago, origin="script",
        payload=payload,
    )  # fmt: skip


WATER = {"station": "Siófok", "level_cm": 98.0, "lake_avg_cm": 101.0, "trend_cm_24h": -2.0,
         "lake_avg_trend_cm_24h": -1.0, "measured_at": "2026-10-22T05:00:00Z",
         "data_source": "x"}  # fmt: skip


def test_brief_payload_from_every_source() -> None:
    p = brief_payload(
        now=NOW,
        host=R["host.status/ok"],
        incidents=R["agent.incidents/closed"]["incidents"],
        updates=card("updates", {"count": 44, "packages": ["a"], "checked_at": "x"}),
        water=card("water", WATER),
        system=SYSTEM,
    )
    assert p == {
        "date": "2026-10-22",
        "uptime_s": 135358,
        "booted_at": "2026-10-19T20:55:59+00:00",
        "open_incidents": 0,
        "incidents": [],
        "updates_count": 44,
        "updates_checked_at": (NOW - timedelta(hours=1)).isoformat(),
        "water": {k: WATER[k] for k in ("station", "level_cm", "lake_avg_cm", "trend_cm_24h",
                                        "lake_avg_trend_cm_24h", "measured_at")},
        "llama_state": "running",
        "hermes_state": "degraded",
        "missing": [],
            "late_by_s": None,
    }  # fmt: skip
    assert brief_severity(p) == "info"


def test_brief_payload_with_gaps_and_incidents() -> None:
    p = brief_payload(
        now=NOW, host=None, incidents=R["agent.incidents/detected"]["incidents"],
        updates=None, water=None, system={"llama": {"status": "failed"}, "hermes": {}},
    )  # fmt: skip
    assert (p["uptime_s"], p["booted_at"], p["updates_count"], p["water"]) == (None,) * 4
    assert p["missing"] == ["host", "updates", "water"]
    assert p["open_incidents"] == 1
    assert p["incidents"][0]["state"] == "detected" and p["incidents"][0]["job"] == "98de85fe6a1f"
    assert brief_severity(p) == "alert"  # llama failed
    p["llama_state"] = "running"
    assert brief_severity(p) == "warn"


def test_open_incidents_keeps_resolved_and_sorts() -> None:
    raw = [
        {"id": "a", "state": "closed", "last_seen": "2026-10-20"},
        {"id": "b", "state": "resolved", "last_seen": "2026-10-19"},
        {"id": "c", "state": "alerted", "last_seen": "2026-10-21", "error": " x \n y "},
        "junk",
    ]
    out = open_incidents(raw)
    assert [i["id"] for i in out] == ["c", "b"]
    assert out[0]["error"] == "x y"


# --- service ---------------------------------------------------------------------------


class Clock:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class StubSystem:
    async def snapshot(self, *, fresh: bool = False) -> dict[str, Any]:
        return SYSTEM


class Env:
    def __init__(self, h: Harness, svc: ScheduleService, clock: Clock, helper: FakeHelper):
        self.h, self.svc, self.clock, self.helper = h, svc, clock, helper

    def events(self, type_: str) -> list[dict[str, Any]]:
        return [e.data for e in self.h.events() if e.type == type_]

    async def runs(self) -> list[Run]:
        async with self.h.db() as s:
            return list((await s.execute(select(Run).order_by(Run.queued_at))).scalars())

    async def schedule(self, sid: str) -> Schedule:
        async with self.h.db() as s:
            sc = await s.get(Schedule, uuid.UUID(sid))
        assert sc is not None
        return sc


@pytest.fixture
async def sx(db: async_sessionmaker[AsyncSession]) -> AsyncIterator[Env]:
    helper = FakeHelper().start()
    clock = Clock(local(2026, 10, 21, 8, 59, 30))
    async with make_harness(db) as h:
        brief = BriefService(db, h.hub, HelperClient(helper.path), StubSystem())  # type: ignore[arg-type]
        svc = ScheduleService(db, h.mgr, h.hub, brief, tick_s=0.05, catchup_s=3600, clock=clock)
        await svc.ensure_brief()
        try:
            yield Env(h, svc, clock, helper)
        finally:
            await svc.close()
            helper.stop()


PROMPT = {"name": "Uptime", "cron": "0 9 * * *", "prompt": "uptime?"}


@pg
async def test_brief_is_seeded_once_and_listed_first(sx: Env) -> None:
    await sx.svc.ensure_brief()
    await sx.svc.create(PROMPT)
    out = await sx.svc.list()
    assert [(o["kind"], o["name"]) for o in out] == [
        ("brief", "Morning brief"),
        ("prompt", "Uptime"),
    ]
    brief = out[0]
    assert (brief["cron"], brief["description"], brief["enabled"]) == (
        "30 7 * * *", "At 07:30 every day", True
    )  # fmt: skip
    assert protocol.check_event("schedule.updated", brief) is None


@pg
async def test_brief_seed_survives_generic_plans(sx: Env) -> None:
    # Postgres switches a prepared statement to a generic plan after 5 runs on a
    # connection; the ON CONFLICT predicate must still find the partial unique index.
    for _ in range(8):
        await sx.svc.ensure_brief()
    async with sx.h.db() as s:
        kinds = list((await s.execute(select(Schedule.kind))).scalars())
    assert kinds == ["brief"]


@pg
async def test_loop_fires_even_when_the_brief_seed_fails(
    sx: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    await sx.svc.create(PROMPT)
    sx.clock.at = local(2026, 10, 21, 9, 0, 10)

    async def broken() -> None:
        raise RuntimeError("seed failed")

    monkeypatch.setattr(sx.svc, "ensure_brief", broken)
    await sx.svc.start()
    async with asyncio.timeout(5):
        while not await sx.runs():  # noqa: ASYNC110 - polling state the test does not own
            await asyncio.sleep(0.02)
    (run,) = await sx.runs()
    assert run.origin == "schedule"


@pg
async def test_create_validates(sx: Env) -> None:
    cases = [
        ({**PROMPT, "cron": "*/5 * * * *"}, "too_frequent"),
        ({**PROMPT, "cron": "0 25 * * *"}, "invalid_cron"),
        ({**PROMPT, "name": "  "}, "invalid_name"),
        ({**PROMPT, "name": "x" * 65}, "invalid_name"),
        ({**PROMPT, "prompt": " "}, "invalid_prompt"),
        ({**PROMPT, "conversation_mode": "fixed"}, "conversation_required"),
        ({**PROMPT, "conversation_mode": "fixed", "conversation_id": "c_nope"},
         "conversation_not_found"),
    ]  # fmt: skip
    for fields, code in cases:
        with pytest.raises(InvalidSchedule) as exc:
            await sx.svc.create(fields)
        assert exc.value.code == code, fields


@pg
async def test_due_schedule_queues_a_run_once(sx: Env) -> None:
    out = await sx.svc.create({**PROMPT, "thinking": True})
    assert out["next_run_at"] == local(2026, 10, 21, 9, 0).isoformat()
    assert out["last_status"] is None
    sx.h.events()
    await sx.svc.tick()  # 08:59:30: nothing yet
    assert await sx.runs() == []

    sx.clock.at = local(2026, 10, 21, 9, 0, 10)
    await sx.svc.tick()
    (run,) = await sx.runs()
    assert (run.origin, run.schedule_id, run.thinking, run.input) == (
        "schedule", uuid.UUID(out["id"]), True, "uptime?"
    )  # fmt: skip
    async with sx.h.db() as s:
        conv = await s.get(Conversation, run.conversation_id)
    assert conv is not None and (conv.origin, conv.thinking) == ("schedule", True)
    queued = sx.events("run.queued")[-1]
    assert (queued["origin"], queued["thinking"]) == ("schedule", True)
    sc = (await sx.svc.list())[1]
    assert sc["next_run_at"] == local(2026, 10, 22, 9, 0).isoformat()
    assert sc["last_run_at"] == sx.clock.at.isoformat()
    assert (sc["last_status"], sc["last_run_id"]) == ("queued", run.run_id)

    # a crash before next_run_at was stored: the same due time queues nothing new
    async with sx.h.db() as s:
        await s.execute(
            update(Schedule)
            .where(Schedule.id == uuid.UUID(out["id"]))
            .values(next_run_at=local(2026, 10, 21, 9, 0))
        )
        await s.commit()
    await sx.svc.tick()
    assert len(await sx.runs()) == 1


@pg
async def test_scheduled_run_keeps_its_thinking_and_finishes(sx: Env) -> None:
    h = sx.h
    h.fake.enqueue("protocol/a")
    await sx.svc.create(PROMPT)  # thinking off
    sx.clock.at = local(2026, 10, 21, 9, 0, 1)
    await sx.svc.tick()
    await h.mgr.start()
    await h.settle()
    (run,) = await sx.runs()
    assert (run.status, run.thinking) == ("completed", False)
    assert "provider" not in h.fake.calls("POST", "/v1/runs")[0].body
    assert (await sx.svc.list())[1]["last_status"] == "completed"


@pg
async def test_schedule_thinking_wins_over_the_fixed_chat(sx: Env) -> None:
    h = sx.h
    async with h.db() as s:
        s.add(Conversation(id="c_fixed", thinking=False))
        await s.commit()
    h.fake.enqueue("protocol/think")
    session = load_json("protocol/think.session.json")
    session["session"]["id"] = "c_fixed"
    h.fake.sessions["c_fixed"] = session
    h.fake.messages["c_fixed"] = load_json("protocol/think.messages.json")
    out = await sx.svc.create(
        {**PROMPT, "conversation_mode": "fixed", "conversation_id": "c_fixed", "thinking": True}
    )
    await sx.svc.run_now(uuid.UUID(out["id"]))
    await h.mgr.start()
    await h.settle()
    body = h.fake.calls("POST", "/v1/runs")[0].body
    assert body["provider"] == ROUTE.provider
    (run,) = await sx.runs()
    assert (run.status, run.thinking) == ("completed", True)


@pg
async def test_fixed_chat(sx: Env) -> None:
    async with sx.h.db() as s:
        s.add(Conversation(id="c_fixed", title="Balaton", thinking=False, archived=True))
        await s.commit()
    out = await sx.svc.create(
        {**PROMPT, "conversation_mode": "fixed", "conversation_id": "c_fixed", "thinking": True}
    )
    assert (out["conversation_id"], out["conversation_title"]) == ("c_fixed", "Balaton")
    sx.clock.at = local(2026, 10, 21, 9, 0, 1)
    await sx.svc.tick()
    (run,) = await sx.runs()
    assert (run.conversation_id, run.thinking) == ("c_fixed", True)
    async with sx.h.db() as s:
        conv = await s.get(Conversation, "c_fixed")
    assert conv is not None and conv.archived is False and conv.thinking is False


@pg
async def test_catch_up_within_an_hour_skip_after(sx: Env) -> None:
    a = await sx.svc.create(PROMPT)
    b = await sx.svc.create({**PROMPT, "name": "Late", "cron": "0 8 * * *"})  # due 2026-10-22
    async with sx.h.db() as s:  # pretend the backend was down: both due long ago / recently
        await s.execute(
            update(Schedule)
            .where(Schedule.id == uuid.UUID(b["id"]))
            .values(next_run_at=local(2026, 10, 21, 8, 0))
        )
        await s.commit()
    sx.clock.at = local(2026, 10, 21, 9, 40)  # 9:00 is 40 min late, 8:00 is 100 min late
    sx.h.events()
    await sx.svc.tick()
    (run,) = await sx.runs()
    assert run.schedule_id == uuid.UUID(a["id"])
    late = await sx.schedule(b["id"])
    assert late.next_run_at == local(2026, 10, 22, 8, 0) and late.last_run_at is None
    updated = {e["id"]: e for e in sx.events("schedule.updated")}
    assert set(updated) >= {a["id"], b["id"]}  # + the brief, timed on its first pass


@pg
async def test_disable_enable_and_changes(sx: Env) -> None:
    out = await sx.svc.create(PROMPT)
    sid = uuid.UUID(out["id"])
    off = await sx.svc.update(sid, {"enabled": False})
    assert (off["enabled"], off["next_run_at"]) == (False, None)
    sx.clock.at = local(2026, 10, 21, 9, 0, 5)
    await sx.svc.tick()
    assert await sx.runs() == []
    on = await sx.svc.update(sid, {"enabled": True, "cron": "30 9 * * 1-5"})
    assert on["next_run_at"] == local(2026, 10, 21, 9, 30).isoformat()
    renamed = await sx.svc.update(sid, {"name": "Load", "prompt": "load?", "thinking": True})
    assert (renamed["name"], renamed["prompt"], renamed["thinking"]) == ("Load", "load?", True)
    assert renamed["next_run_at"] == on["next_run_at"]  # timing untouched
    await sx.svc.delete(sid)
    assert sx.events("schedule.deleted") == [{"id": str(sid)}]
    assert [o["kind"] for o in await sx.svc.list()] == ["brief"]


@pg
async def test_run_now_keeps_the_timing(sx: Env) -> None:
    out = await sx.svc.create(PROMPT)
    sid = uuid.UUID(out["id"])
    first = await sx.svc.run_now(sid)
    second = await sx.svc.run_now(sid)
    assert first["run_id"] != second["run_id"] and first["card_id"] is None
    assert len(await sx.runs()) == 2
    assert second["schedule"]["next_run_at"] == out["next_run_at"]


async def _seed_cards(db: async_sessionmaker[AsyncSession], now: datetime) -> None:
    async with db() as s:
        s.add_all([
            Card(id=uuid.uuid4().hex, v=1, type="updates", severity="info",
                 ts=now - timedelta(hours=2), origin="script",
                 payload={"count": 44, "packages": [], "checked_at": now.isoformat()}),
            Card(id=uuid.uuid4().hex, v=1, type="water", severity="info",
                 ts=now - timedelta(hours=40), origin="script", payload=WATER),  # too old
        ])  # fmt: skip
        await s.commit()


@pg
async def test_brief_run_now_and_scheduled(sx: Env) -> None:
    db = sx.h.db
    now = sx.clock.at
    await _seed_cards(db, now)
    brief = (await sx.svc.list())[0]
    sx.h.events()
    r = await sx.svc.run_now(uuid.UUID(brief["id"]))
    assert r["run_id"] is None and r["schedule"]["last_status"] == "ok"
    (new,) = sx.events("card.new")
    assert (new["id"], new["type"], new["origin"]) == (r["card_id"], "brief", "backend")
    p = new["payload"]
    assert (p["uptime_s"], p["open_incidents"], p["updates_count"], p["water"]) == (
        135358, 0, 44, None
    )  # fmt: skip
    assert p["missing"] == ["water"] and new["severity"] == "warn"
    assert sx.helper.calls == ["host.status", "agent.incidents"]

    # the scheduled one: 07:30 tomorrow, one card even if fired twice for the same time
    sx.helper.incidents = "detected"
    sx.clock.at = local(2026, 10, 22, 7, 30, 2)
    for _ in range(2):
        async with db() as s:
            await s.execute(
                update(Schedule).where(Schedule.kind == "brief")
                .values(next_run_at=local(2026, 10, 22, 7, 30))
            )  # fmt: skip
            await s.commit()
        await sx.svc.tick()
    async with db() as s:
        cards = list((await s.execute(select(Card).where(Card.type == "brief"))).scalars())
    assert len(cards) == 2
    scheduled = max(cards, key=lambda c: c.ts)
    assert scheduled.payload["open_incidents"] == 1 and scheduled.payload["date"] == "2026-10-22"


@pg
async def test_missed_brief_is_written_late_the_same_day(sx: Env) -> None:
    """N74: the machine was off at 07:30; the brief comes after the start settles."""
    db = sx.h.db

    async def due(at: datetime) -> None:
        async with db() as s:
            await s.execute(update(Schedule).where(Schedule.kind == "brief").values(next_run_at=at))
            await s.commit()

    async def briefs() -> list[Card]:
        async with db() as s:
            return list((await s.execute(select(Card).where(Card.type == "brief"))).scalars())

    await due(local(2026, 10, 22, 7, 30))
    sx.clock.at = local(2026, 10, 22, 9, 24)
    sx.svc._started_at = sx.clock.at  # just started
    await sx.svc.tick()
    assert await briefs() == []  # settling: the agent's own catch-up runs first
    brief = (await sx.svc.list())[0]
    assert brief["next_run_at"] == local(2026, 10, 22, 7, 30).isoformat()
    sx.clock.at = local(2026, 10, 22, 9, 27, 30)
    await sx.svc.tick()
    (card,) = await briefs()
    assert card.payload["late_by_s"] == 7050 and card.payload["date"] == "2026-10-22"
    brief = (await sx.svc.list())[0]
    assert brief["next_run_at"] == local(2026, 10, 23, 7, 30).isoformat()
    assert brief["last_status"] == "ok"

    # more than 6 hours late: skipped as before
    await due(local(2026, 10, 23, 7, 30))
    sx.clock.at = local(2026, 10, 23, 13, 31)
    await sx.svc.tick()
    assert len(await briefs()) == 1
    assert (await sx.svc.list())[0]["next_run_at"] == local(2026, 10, 24, 7, 30).isoformat()


@pg
async def test_brief_without_helper_still_writes(sx: Env) -> None:
    sx.helper.stop()
    brief = (await sx.svc.list())[0]
    r = await sx.svc.run_now(uuid.UUID(brief["id"]))
    assert r["schedule"]["last_status"] == "ok"
    (new,) = sx.events("card.new")
    assert new["payload"]["missing"] == ["host", "incidents", "updates", "water"]


@pg
async def test_brief_rules(sx: Env) -> None:
    sid = uuid.UUID((await sx.svc.list())[0]["id"])
    with pytest.raises(InvalidSchedule) as exc:
        await sx.svc.update(sid, {"name": "Brief", "cron": "0 7 * * *"})
    assert exc.value.code == "brief_fields"
    with pytest.raises(ScheduleConflict):
        await sx.svc.delete(sid)
    out = await sx.svc.update(sid, {"cron": "0 7 * * 1-5"})
    assert out["next_run_at"] == local(2026, 10, 22, 7, 0).isoformat()


@pg
async def test_loop_fires_and_recreates_a_missing_brief(sx: Env) -> None:
    async with sx.h.db() as s:
        await s.execute(delete(Schedule))
        await s.commit()
    await sx.svc.create({**PROMPT, "cron": "1 9 * * *"})
    sx.clock.at = local(2026, 10, 21, 9, 1, 0)
    await sx.svc.start()
    async with asyncio.timeout(5):
        while len(await sx.runs()) < 1 or sx.svc.ticks < 2:  # noqa: ASYNC110
            await asyncio.sleep(0.02)
    assert [o["kind"] for o in await sx.svc.list()] == ["brief", "prompt"]


# --- push ------------------------------------------------------------------------------


@pg
async def test_brief_push(db: async_sessionmaker[AsyncSession]) -> None:
    hub = EventHub(validate=True)
    google = FakeGoogle()
    push = PushService(db, client_for(google), Presence(), approval_delay_s=0)
    hub.add_listener(push.listener)
    helper = FakeHelper().start()
    async with db() as s:
        on, _ = await create_device(s, "pixel")
        await set_fcm_token(s, on.id, "fcm-on")
        off, _ = await create_device(s, "tablet")
        await set_fcm_token(s, off.id, "fcm-off")
        await s.execute(
            update(Device)
            .where(Device.id == on.id)
            .values(notify={**notify_settings(None), "brief": True})
        )
        s.add(Card(id=uuid.uuid4().hex, v=1, type="water", severity="info",
                   ts=datetime.now(UTC), origin="script", payload=WATER))  # fmt: skip
        await s.commit()
    await push.start()
    try:
        brief = BriefService(db, hub, HelperClient(helper.path), StubSystem())  # type: ignore[arg-type]
        await brief.publish(uuid.uuid4().hex)
        async with asyncio.timeout(5):
            await push.drain()
    finally:
        await push.close()
        helper.stop()
    (msg,) = google.sends
    assert msg["token"] == "fcm-on"
    assert msg["android"] == {"priority": "HIGH", "ttl": f"{12 * 3600}s", "collapse_key": "brief"}
    d = msg["data"]
    assert (d["kind"], d["type"], d["uptime_s"], d["open_incidents"], d["level_cm"]) == (
        "card", "brief", "135358", "0", "98"
    )  # fmt: skip
    assert (d["updates_count"], d["llama_state"]) == ("", "running")


# --- REST ------------------------------------------------------------------------------


@pytest.fixture
def sched_client(
    env: pytest.MonkeyPatch, migrated_url: str, db: async_sessionmaker[AsyncSession]
) -> Iterator[tuple[TestClient, FakeHelper]]:
    fl = FakeLlama()
    fl.set(task=1, processing=False, tokens=300)
    helper = FakeHelper(fl).start()
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("HERMES_API_KEY", "fake-key")
    env.setenv("DELVE_HELPER_SOCKET", str(helper.path))
    env.setenv("DELVE_SCHEDULE_TICK_S", "0.05")
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(FakeHermes().app),
        llama_transport=StreamingASGITransport(fl.app),
        timing=FAST,
        system_options={"ready_poll_s": 0.02, "watch_s": 0.05, "cache_s": 0.0},
    )
    try:
        with TestClient(app) as c:
            yield c, helper
    finally:
        helper.stop()


@pg
def test_api_schedules(sched_client: tuple[TestClient, FakeHelper], token: str) -> None:
    c, _ = sched_client
    assert c.get("/api/v1/schedules").status_code == 401
    a = auth(token)
    (brief,) = c.get("/api/v1/schedules", headers=a).json()["data"]
    assert brief["kind"] == "brief" and brief["next_run_at"] is not None

    r = c.post("/api/v1/schedules", headers=a, json={**PROMPT, "thinking": True})
    assert r.status_code == 201, r.text
    sid = r.json()["id"]
    assert r.json()["thinking"] is True and r.json()["description"] == "At 09:00 every day"
    for body, code in (
        ({**PROMPT, "cron": "* * * * *"}, "too_frequent"),
        ({**PROMPT, "conversation_mode": "fixed"}, "conversation_required"),
    ):
        r = c.post("/api/v1/schedules", headers=a, json=body)
        assert (r.status_code, r.json()["detail"]["code"]) == (422, code)
    assert c.post("/api/v1/schedules", headers=a, json={**PROMPT, "extra": 1}).status_code == 422
    assert (
        c.post("/api/v1/schedules", headers=a, json={**PROMPT, "thinking": "yes"}).status_code
        == 422
    )

    r = c.patch(f"/api/v1/schedules/{sid}", headers=a, json={"enabled": False})
    assert r.status_code == 200 and r.json()["next_run_at"] is None
    assert c.patch(f"/api/v1/schedules/{sid}", headers=a, json={}).status_code == 422
    assert c.patch(f"/api/v1/schedules/{sid}", headers=a, json={"name": None}).status_code == 422
    r = c.patch(f"/api/v1/schedules/{brief['id']}", headers=a, json={"prompt": "x"})
    assert (r.status_code, r.json()["detail"]["code"]) == (422, "brief_fields")
    missing = uuid.uuid4()
    assert (
        c.patch(f"/api/v1/schedules/{missing}", headers=a, json={"enabled": True}).status_code
        == 404
    )

    r = c.post(f"/api/v1/schedules/{sid}/run", headers=a)
    assert r.status_code == 200 and r.json()["run_id"].startswith(("q_", "run_"))
    r = c.post(f"/api/v1/schedules/{brief['id']}/run", headers=a)
    assert r.status_code == 200 and len(r.json()["card_id"]) == 32
    assert r.json()["schedule"]["last_status"] == "ok"
    assert c.post(f"/api/v1/schedules/{missing}/run", headers=a).status_code == 404

    r = c.delete(f"/api/v1/schedules/{brief['id']}", headers=a)
    assert (r.status_code, r.json()["detail"]["code"]) == (409, "brief_fixed")
    assert c.delete(f"/api/v1/schedules/{sid}", headers=a).status_code == 204
    assert c.delete(f"/api/v1/schedules/{sid}", headers=a).status_code == 404
    assert len(c.get("/api/v1/schedules", headers=a).json()["data"]) == 1
