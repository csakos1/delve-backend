"""Scheduled prompts and the morning brief (B4, plan 6.15, decisions 110, N20, N22).

A schedule is a 5-field cron expression in Europe/Budapest (DST by `zoneinfo`, through
`cronsim`). A `prompt` schedule queues its prompt as a run in the common queue (origin
`schedule`) - in a new conversation each time or in a fixed one, with its own thinking
setting. The single `brief` schedule (seeded by migration 0007, re-created if missing)
writes the morning brief card (delve.brief) instead.

Due work: every `tick_s` (and whenever a schedule changes) the enabled schedules whose
`next_run_at` has passed are fired once, then the next time is computed from now - missed
times in between are not repeated. One that is late by more than `catchup_s` (the backend
was down at the time; N20: one hour) is skipped and logged instead - except the brief
(N74): the same local day and at most `brief_late_s` late, it is written once the backend
has been up for `brief_settle_s`, with `late_by_s`. A queued scheduled run waits like any
other run, also while llama-server is held.

Firing is idempotent: the run's `client_msg_id` and the brief's card id are derived from
the schedule and the due time, so a crash between queueing and storing `next_run_at` does
not queue twice.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from cronsim import CronSim, CronSimError
from cronsim.explain import explain
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.brief import BriefService, brief_card_id
from delve.db.models import Conversation, Run, Schedule
from delve.events import EventHub
from delve.runs.manager import MAX_INPUT_CHARS, NotFound, RunManager
from delve.serialize import iso

log = logging.getLogger("delve.schedules")

TZ = ZoneInfo("Europe/Budapest")
KINDS = ("prompt", "brief")
MODES = ("new", "fixed")
NAME_MAX = 64
MIN_GAP = timedelta(minutes=10)  # a schedule may not fire more often than this
MAX_SCHEDULES = 50
BRIEF_NAME = "Morning brief"
BRIEF_CRON = "30 7 * * *"  # N22: daily 07:30
BRIEF_FIELDS = frozenset({"cron", "enabled"})
_RUN_NS = uuid.UUID("8f3c2e74-6d1b-4c55-9a39-2f8e0c1b7d42")


class InvalidSchedule(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ScheduleNotFound(LookupError):
    pass


class ScheduleConflict(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# --- cron ----------------------------------------------------------------------------


def check_cron(expr: str, *, now: datetime | None = None) -> str:
    """Normalize and validate: 5 fields, a valid expression, fires at least 10 min apart."""
    if not isinstance(expr, str):
        raise InvalidSchedule("invalid_cron", "cron must be a string")
    fields = expr.split()
    if len(fields) != 5:
        raise InvalidSchedule("invalid_cron", "cron needs exactly 5 fields")
    norm = " ".join(fields)
    start = (now or datetime.now(UTC)).astimezone(TZ)
    try:
        it = CronSim(norm, start)
        times = [next(it) for _ in range(8)]
    except (CronSimError, StopIteration, ValueError) as exc:
        raise InvalidSchedule("invalid_cron", f"invalid cron expression: {exc}") from exc
    gaps = [b.astimezone(UTC) - a.astimezone(UTC) for a, b in itertools.pairwise(times)]
    if gaps and min(gaps) < MIN_GAP:
        raise InvalidSchedule(
            "too_frequent", f"a schedule may run at most every {int(MIN_GAP.seconds / 60)} min"
        )
    return norm


def next_fire(expr: str, after: datetime) -> datetime:
    """The first time strictly after `after`, in UTC."""
    t = next(CronSim(expr, after.astimezone(TZ)))
    if t <= after:  # cronsim starts at `after` itself when it is on a full minute
        t = next(CronSim(expr, (after + timedelta(minutes=1)).astimezone(TZ)))
    return t.astimezone(UTC)


def describe(expr: str) -> str:
    try:
        return explain(expr)
    except Exception:  # explain() covers what CronSim accepts; never fail a listing
        return expr


def run_message_id(schedule_id: uuid.UUID, due: datetime) -> uuid.UUID:
    return uuid.uuid5(_RUN_NS, f"{schedule_id}/{due.astimezone(UTC).isoformat()}")


# --- output --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LastRun:
    run_id: str
    status: str
    conversation_id: str
    error: str | None


def schedule_out(
    sc: Schedule, last: LastRun | None = None, conversation_title: str | None = None
) -> dict[str, Any]:
    status, error, run_id = sc.last_status, sc.last_error, None
    if sc.kind == "prompt" and last is not None and status is None:
        status, error, run_id = last.status, last.error, last.run_id
    elif sc.kind == "prompt" and last is not None:
        run_id = last.run_id
    return {
        "id": str(sc.id),
        "kind": sc.kind,
        "name": sc.name,
        "cron": sc.cron,
        "description": describe(sc.cron),
        "prompt": sc.prompt,
        "conversation_mode": sc.conversation_mode,
        "conversation_id": sc.conversation_id,
        "conversation_title": conversation_title,
        "thinking": sc.thinking,
        "enabled": sc.enabled,
        "next_run_at": iso(sc.next_run_at),
        "last_run_at": iso(sc.last_run_at),
        "last_status": status,
        "last_error": error,
        "last_run_id": run_id,
        "created_at": iso(sc.created_at),
        "updated_at": iso(sc.updated_at),
    }


async def _render(s: AsyncSession, rows: list[Schedule]) -> list[dict[str, Any]]:
    ids = [r.id for r in rows if r.kind == "prompt"]
    last: dict[uuid.UUID, LastRun] = {}
    if ids:
        rank = (
            func.row_number()
            .over(partition_by=Run.schedule_id, order_by=(Run.queued_at.desc(), Run.run_id.desc()))
            .label("rank")
        )
        ranked = (
            select(Run.schedule_id, Run.run_id, Run.status, Run.conversation_id, Run.error, rank)
            .where(Run.schedule_id.in_(ids))
            .subquery()
        )
        q = select(
            ranked.c.schedule_id, ranked.c.run_id, ranked.c.status, ranked.c.conversation_id,
            ranked.c.error,
        ).where(ranked.c.rank == 1)  # fmt: skip
        for sid, rid, status, conv, err in (await s.execute(q)).all():
            if sid is not None:
                last[sid] = LastRun(rid, status, conv, err)
    conv_ids = {r.conversation_id for r in rows if r.conversation_id}
    titles: dict[str, str | None] = {}
    if conv_ids:
        for cid, title, preview in (
            await s.execute(
                select(Conversation.id, Conversation.title, Conversation.preview).where(
                    Conversation.id.in_(conv_ids)
                )
            )
        ).all():
            titles[cid] = title or preview
    return [schedule_out(r, last.get(r.id), titles.get(r.conversation_id or "")) for r in rows]


# --- service -------------------------------------------------------------------------


class ScheduleService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        manager: RunManager,
        hub: EventHub,
        brief: BriefService,
        *,
        tick_s: float = 30.0,
        catchup_s: float = 3600.0,
        brief_late_s: float = 6 * 3600.0,
        brief_settle_s: float = 180.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._sm = sessionmaker
        self._mgr = manager
        self._hub = hub
        self._brief = brief
        self._tick_s = tick_s
        self._catchup = timedelta(seconds=catchup_s)
        # N74: a missed brief is still written later the same day, after the start settles
        # (the agent's own catch-up runs - updates, water - come first)
        self._brief_late = timedelta(seconds=brief_late_s)
        self._brief_settle = timedelta(seconds=brief_settle_s)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._started_at = self._clock()
        self._lock = asyncio.Lock()
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.ticks = 0  # completed loop passes (tests)

    # --- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        self._started_at = self._clock()
        try:
            await self.ensure_brief()
        except Exception as exc:  # a database that is not up yet: the loop retries
            log.warning("schedules not readable yet", extra={"error": type(exc).__name__})
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="schedules")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    def wake(self) -> None:
        self._wakeup.set()

    async def ensure_brief(self) -> None:
        stmt = (
            insert(Schedule)
            .values(
                id=uuid.uuid4(),
                kind="brief",
                name=BRIEF_NAME,
                cron=BRIEF_CRON,
                conversation_mode="new",
                enabled=True,
            )
            .on_conflict_do_nothing(
                # A literal predicate: as a bind parameter (`kind = $7`) it matches the partial
                # unique index only while Postgres plans the statement per call; from the 6th
                # run on a connection the generic plan cannot infer the index ("no unique or
                # exclusion constraint matching the ON CONFLICT specification").
                index_elements=[Schedule.kind],
                index_where=text("kind = 'brief'"),
            )
        )
        async with self._sm() as s:
            await s.execute(stmt)
            await s.commit()

    # --- reads -----------------------------------------------------------------

    async def list(self) -> list[dict[str, Any]]:
        async with self._sm() as s:
            rows = list(
                (
                    await s.execute(
                        select(Schedule).order_by(
                            (Schedule.kind == "brief").desc(), Schedule.created_at, Schedule.id
                        )
                    )
                ).scalars()
            )
            return await _render(s, rows)

    async def get(self, schedule_id: uuid.UUID) -> dict[str, Any]:
        async with self._sm() as s:
            sc = await s.get(Schedule, schedule_id)
            if sc is None:
                raise ScheduleNotFound(str(schedule_id))
            return (await _render(s, [sc]))[0]

    # --- writes ----------------------------------------------------------------

    async def _check_conversation(
        self, s: AsyncSession, mode: str, conversation_id: str | None
    ) -> str | None:
        if mode not in MODES:
            raise InvalidSchedule("invalid_mode", "conversation_mode is new or fixed")
        if mode == "new":
            return None
        if not conversation_id:
            raise InvalidSchedule("conversation_required", "a fixed chat needs conversation_id")
        if await s.get(Conversation, conversation_id) is None:
            raise InvalidSchedule("conversation_not_found", "that conversation does not exist")
        return conversation_id

    @staticmethod
    def _name(v: Any) -> str:
        name = " ".join(str(v or "").split())
        if not name or len(name) > NAME_MAX:
            raise InvalidSchedule("invalid_name", f"name: 1-{NAME_MAX} characters")
        return name

    @staticmethod
    def _prompt(v: Any) -> str:
        if not isinstance(v, str) or not v.strip() or len(v) > MAX_INPUT_CHARS:
            raise InvalidSchedule("invalid_prompt", "prompt: a non-empty text")
        return v.strip()

    async def create(self, fields: dict[str, Any]) -> dict[str, Any]:
        now = self._clock()
        cron = check_cron(fields["cron"], now=now)
        async with self._lock, self._sm() as s:
            count = (await s.execute(select(func.count()).select_from(Schedule))).scalar_one()
            if count >= MAX_SCHEDULES:
                raise ScheduleConflict("too_many", f"at most {MAX_SCHEDULES} schedules")
            mode = fields.get("conversation_mode", "new")
            enabled = bool(fields.get("enabled", True))
            sc = Schedule(
                id=uuid.uuid4(),
                kind="prompt",
                name=self._name(fields.get("name")),
                cron=cron,
                prompt=self._prompt(fields.get("prompt")),
                conversation_mode=mode,
                conversation_id=await self._check_conversation(
                    s, mode, fields.get("conversation_id")
                ),
                thinking=bool(fields.get("thinking", False)),
                enabled=enabled,
                next_run_at=next_fire(cron, now) if enabled else None,
            )
            s.add(sc)
            await s.commit()
            out = (await _render(s, [sc]))[0]
        log.info("schedule created", extra={"schedule_id": out["id"], "cron": cron})
        self._hub.publish("schedule.updated", out)
        self.wake()
        return out

    async def update(self, schedule_id: uuid.UUID, fields: dict[str, Any]) -> dict[str, Any]:
        now = self._clock()
        async with self._lock, self._sm() as s:
            sc = await s.get(Schedule, schedule_id, with_for_update=True)
            if sc is None:
                raise ScheduleNotFound(str(schedule_id))
            if sc.kind == "brief" and set(fields) - BRIEF_FIELDS:
                raise InvalidSchedule(
                    "brief_fields", "the morning brief only takes cron and enabled"
                )
            timing_changed = False
            if "cron" in fields:
                cron = check_cron(fields["cron"], now=now)
                timing_changed = cron != sc.cron
                sc.cron = cron
            if "enabled" in fields:
                timing_changed = timing_changed or bool(fields["enabled"]) != sc.enabled
                sc.enabled = bool(fields["enabled"])
            if "name" in fields:
                sc.name = self._name(fields["name"])
            if "prompt" in fields:
                sc.prompt = self._prompt(fields["prompt"])
            if "thinking" in fields:
                sc.thinking = bool(fields["thinking"])
            if "conversation_mode" in fields or "conversation_id" in fields:
                mode = fields.get("conversation_mode", sc.conversation_mode)
                conv = fields.get(
                    "conversation_id", sc.conversation_id if mode == "fixed" else None
                )
                sc.conversation_id = await self._check_conversation(s, mode, conv)
                sc.conversation_mode = mode
            if timing_changed or (sc.enabled and sc.next_run_at is None):
                sc.next_run_at = next_fire(sc.cron, now) if sc.enabled else None
            sc.updated_at = now
            await s.commit()
            out = (await _render(s, [sc]))[0]
        self._hub.publish("schedule.updated", out)
        self.wake()
        return out

    async def delete(self, schedule_id: uuid.UUID) -> None:
        async with self._lock, self._sm() as s:
            sc = await s.get(Schedule, schedule_id)
            if sc is None:
                raise ScheduleNotFound(str(schedule_id))
            if sc.kind == "brief":
                raise ScheduleConflict(
                    "brief_fixed", "the morning brief can be turned off, not deleted"
                )
            await s.delete(sc)
            await s.commit()
        log.info("schedule deleted", extra={"schedule_id": str(schedule_id)})
        self._hub.publish("schedule.deleted", {"id": str(schedule_id)})

    async def run_now(self, schedule_id: uuid.UUID) -> dict[str, Any]:
        """Fire once now; the regular timing stays as it is."""
        now = self._clock()
        async with self._lock:
            async with self._sm() as s:
                sc = await s.get(Schedule, schedule_id)
                if sc is None:
                    raise ScheduleNotFound(str(schedule_id))
            ref = await self._fire(sc, now, manual=True)
            return {"schedule": await self.get(schedule_id), **ref}

    # --- firing ----------------------------------------------------------------

    async def _fire(
        self, sc: Schedule, due: datetime, *, manual: bool, late: bool = False
    ) -> dict[str, Any]:
        """Queue the prompt or write the brief, then store the outcome. Returns the refs."""
        now = self._clock()
        ref: dict[str, Any] = {"run_id": None, "card_id": None}
        status: str | None = None
        error: str | None = None
        if sc.kind == "brief":
            card_id = uuid.uuid4().hex if manual else brief_card_id(sc.id, due)
            try:
                late_by = int((now - due).total_seconds()) if late else None
                card = await self._brief.publish(card_id, now, late_by_s=late_by)
                ref["card_id"], status = card["id"], "ok"
            except Exception as exc:
                log.exception("brief failed", extra={"schedule_id": str(sc.id)})
                status, error = "failed", type(exc).__name__
        else:
            msg_id = uuid.uuid4() if manual else run_message_id(sc.id, due)
            try:
                res = await self._mgr.submit(
                    text=sc.prompt or "",
                    client_msg_id=msg_id,
                    conversation_id=sc.conversation_id if sc.conversation_mode == "fixed" else None,
                    thinking=sc.thinking,
                    origin="schedule",
                    schedule_id=sc.id,
                )
                ref["run_id"] = res.run_id
            except NotFound:
                status, error = "failed", "conversation_missing"
            except Exception as exc:
                log.exception("scheduled run not queued", extra={"schedule_id": str(sc.id)})
                status, error = "failed", type(exc).__name__
        async with self._sm() as s:
            row = await s.get(Schedule, sc.id, with_for_update=True)
            if row is None:  # deleted meanwhile
                return ref
            row.last_run_at, row.last_status, row.last_error = now, status, error
            if not manual:
                row.next_run_at = next_fire(row.cron, now) if row.enabled else None
            row.updated_at = now
            await s.commit()
            out = (await _render(s, [row]))[0]
        log.info(
            "schedule fired",
            extra={"schedule_id": str(sc.id), "kind": sc.kind, "manual": manual,
                   "due": due.isoformat(), "status": status or "queued", **ref},
        )  # fmt: skip
        self._hub.publish("schedule.updated", out)
        return ref

    async def tick(self) -> datetime | None:
        """One pass: fire what is due, skip what was missed. Returns the next due time."""
        now = self._clock()
        async with self._lock:
            async with self._sm() as s:
                rows = list(
                    (await s.execute(select(Schedule).where(Schedule.enabled.is_(True)))).scalars()
                )
                fixed = []
                for sc in rows:
                    if sc.next_run_at is None:  # new, re-created brief, or after a bad cron
                        try:
                            sc.next_run_at = next_fire(sc.cron, now)
                        except (CronSimError, StopIteration, ValueError):
                            sc.enabled = False
                            sc.last_status, sc.last_error = "failed", "invalid_cron"
                        sc.updated_at = now
                        fixed.append(sc)
                if fixed:
                    await s.commit()
                    for out in await _render(s, fixed):
                        self._hub.publish("schedule.updated", out)
            for sc in rows:
                at = sc.next_run_at
                if not sc.enabled or at is None or at > now:
                    continue
                if now - at > self._catchup:
                    if sc.kind == "brief" and self._brief_still_due(at, now):
                        if now - self._started_at >= self._brief_settle:
                            await self._fire(sc, at, manual=False, late=True)
                        continue  # else: next tick
                    await self._skip(sc, now)
                else:
                    await self._fire(sc, at, manual=False)
        async with self._sm() as s:
            nxt: datetime | None = (
                await s.execute(
                    select(func.min(Schedule.next_run_at)).where(Schedule.enabled.is_(True))
                )
            ).scalar_one()
        return nxt

    def _brief_still_due(self, at: datetime, now: datetime) -> bool:
        return (
            now - at <= self._brief_late and at.astimezone(TZ).date() == now.astimezone(TZ).date()
        )

    async def _skip(self, sc: Schedule, now: datetime) -> None:
        async with self._sm() as s:
            row = await s.get(Schedule, sc.id, with_for_update=True)
            if row is None or row.next_run_at is None:
                return
            missed = row.next_run_at
            row.next_run_at = next_fire(row.cron, now)
            row.updated_at = now
            await s.commit()
            out = (await _render(s, [row]))[0]
        log.warning(
            "schedule missed",
            extra={"schedule_id": str(sc.id), "missed": missed.isoformat(),
                   "next": out["next_run_at"]},
        )  # fmt: skip
        self._hub.publish("schedule.updated", out)

    async def _loop(self) -> None:
        while True:
            nxt: datetime | None = None
            try:
                await self.ensure_brief()
            except asyncio.CancelledError:
                raise
            except Exception:  # never let it hold back the schedules that exist
                log.exception("morning brief schedule not ensured")
            try:
                nxt = await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("schedule tick failed")
            self.ticks += 1
            wait = self._tick_s
            if nxt is not None:
                wait = max(0.05, min(wait, (nxt - self._clock()).total_seconds()))
            self._wakeup.clear()
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(wait):
                    await self._wakeup.wait()
