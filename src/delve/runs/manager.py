"""Run queue and lifecycle (plan 6.4).

One global FIFO because llama runs with `-np 1`: at most one run is active
(`starting|running|waiting_for_approval|stopping|reconnecting`). A run is queued as
`q_<hex>` and renamed to Hermes' `run_<hex>` after the 202. Events are stored in
`run_events` (PK run_id+seq), so a restarted backend can rebuild a run's state and resume
its stream after the last stored seq.

Assistant message content (N27, option a): while streaming, `content` holds the streamed
text; when the run completes it is replaced by `run.completed.output`, and the
`message.interim` texts go to `messages.interim`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import (
    ACTIVE_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    Approval,
    Conversation,
    Message,
    Run,
    RunEvent,
    SystemState,
)
from delve.events import EventHub
from delve.hermes.client import (
    HermesClient,
    HermesError,
    HermesUnavailable,
    StreamEnded,
    new_reasoning,
    route_applied,
)
from delve.hermes.models import RunStatus, SessionMessage
from delve.hermes.sse import HermesEvent, HermesProtocolError
from delve.llama import LlamaClient
from delve.runs import late, steps
from delve.runs.context import ContextMeter, Measurement
from delve.serialize import (
    CONTEXT_WARN_PCT,
    approval_out,
    conversation_out,
    message_out,
    step_out,
)

log = logging.getLogger("delve.runs")

MAX_INPUT_CHARS = 100_000
# Hermes answers that are worth retrying the submission for (same Idempotency-Key)
RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
EVENT_TO_STATUS = {
    "run.completed": "completed",
    "run.cancelled": "cancelled",
    "run.failed": "failed",
    "run.interrupted": "failed",
}
HERMES_TO_STATUS = {
    "completed": "completed",
    "cancelled": "cancelled",
    "failed": "failed",
    "interrupted": "failed",
}
MESSAGE_STATUS_FOR = {"completed": "final", "cancelled": "cancelled", "failed": "failed"}


class NotFound(LookupError):
    pass


class InvalidRequest(ValueError):
    pass


class LateApprovalError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class Timing:
    """Tunables; tests shrink them."""

    flush_interval_s: float = 1.5
    submit_timeout_s: float = 120.0  # keep retrying POST /v1/runs this long
    resolve_timeout_s: float = 300.0  # keep retrying GET /v1/runs/{id} this long
    max_backoff_s: float = 10.0
    max_stream_failures: int = 20  # consecutive reconnects without a new event
    title_delays_s: tuple[float, ...] = (1.5, 3.0, 6.0)  # Hermes titles ~1.2 s after a run
    idle_poll_s: float = 5.0
    maintenance_s: float = 15.0  # Hermes health poll + approval expiry sweep
    context_poll_s: float = 1.0  # llama /slots sampling while a run is active


@dataclass(frozen=True, slots=True)
class SubmitResult:
    conversation: dict[str, Any]
    message: dict[str, Any]
    run_id: str
    created: bool  # False: the client_msg_id was seen before (idempotent replay)


@dataclass
class _Ctx:
    """State of the active run; rebuilt from run_events after a restart."""

    run_id: str
    conversation_id: str
    message_id: int
    thinking: bool
    status: str
    text: list[str] = field(default_factory=list)
    interim: list[str] = field(default_factory=list)
    last_seq: int | None = None
    dirty: bool = False
    last_flush: float = 0.0
    reconnects: int = 0
    origin: str = "app"
    meter: ContextMeter | None = None
    tool_steps: int = 0  # tool.started events seen (a run without any made one llama call)
    started_mono: float = field(default_factory=time.monotonic)
    first_delta_s: float | None = None
    measure_first_delta: bool = True  # not for a run resumed after a restart


class RunManager:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hermes: HermesClient,
        hub: EventHub,
        *,
        timing: Timing | None = None,
        approval_timeout_s: int = 300,
        expected_version: str | None = None,
        llama: LlamaClient | None = None,
    ) -> None:
        self._sm = sessionmaker
        self._llama = llama
        self._approval_timeout_s = approval_timeout_s  # mirrors Hermes approvals.timeout
        self._expected_version = expected_version
        self.hermes_ok: bool | None = None  # None until the first health poll
        self.hermes_version: str | None = None
        self._maint: asyncio.Task[None] | None = None
        self._hermes = hermes
        self._hub = hub
        self._t = timing or Timing()
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._bg: set[asyncio.Task[None]] = set()
        self._stop_requested: set[str] = set()  # queue ids stopped while starting
        self._renamed: dict[str, str] = {}  # q_ id -> run_ id, for late stop requests
        self.active_run_id: str | None = None
        self._ctx: _Ctx | None = None
        self.idle = asyncio.Event()  # set while nothing is queued or active (tests)

    # --- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._worker(), name="run-manager")
        if self._maint is None:
            self._maint = asyncio.create_task(self._maintenance(), name="run-maintenance")

    async def close(self) -> None:
        tasks = [t for t in (self._task, self._maint, *self._bg) if t is not None]
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._task = self._maint = None
        self._bg.clear()

    def wake(self) -> None:
        self._wakeup.set()

    # --- commands --------------------------------------------------------------

    async def submit(
        self,
        *,
        text: str,
        client_msg_id: uuid.UUID,
        conversation_id: str | None = None,
        thinking: bool | None = None,
        origin: str = "app",
    ) -> SubmitResult:
        if not text.strip():
            raise InvalidRequest("empty message")
        if len(text) > MAX_INPUT_CHARS:
            raise InvalidRequest(f"message longer than {MAX_INPUT_CHARS} characters")
        async with self._sm() as s:
            prev = await self._replay(s, client_msg_id)
            if prev is not None:
                return prev
            created_conv = conversation_id is None
            if conversation_id is None:
                conv = Conversation(id=f"c_{uuid.uuid4().hex}", thinking=bool(thinking))
                s.add(conv)
            else:
                found = await s.get(Conversation, conversation_id)
                if found is None:
                    raise NotFound(f"conversation {conversation_id} not found")
                conv = found
                conv.archived = False
                conv.updated_at = now()
            await s.flush()
            run_id = f"q_{uuid.uuid4().hex}"
            msg = Message(
                conversation_id=conv.id,
                run_id=run_id,
                role="user",
                content=text,
                client_msg_id=client_msg_id,
            )
            run = Run(
                run_id=run_id,
                conversation_id=conv.id,
                idempotency_key=uuid.uuid4(),
                status="queued",
                input=text,
                thinking=conv.thinking,
                origin=origin,
            )
            s.add_all([msg, run])
            try:
                await s.commit()
            except IntegrityError:
                # the same client_msg_id raced in through another connection
                await s.rollback()
                prev = await self._replay(s, client_msg_id)
                if prev is None:
                    raise
                return prev
            position = await self._position(s, run)
            conv_out, msg_out = conversation_out(conv), message_out(msg)
        self._hub.publish(
            "conversation.created" if created_conv else "conversation.updated", conv_out
        )
        self._hub.publish("message.created", {"conversation_id": conv.id, "message": msg_out})
        self._hub.publish(
            "run.queued",
            {
                "run_id": run_id,
                "conversation_id": conv.id,
                "position": position,
                "thinking": conv.thinking,
                "origin": origin,
            },
        )
        self.idle.clear()
        self.wake()
        return SubmitResult(conv_out, msg_out, run_id, created=True)

    async def stop(self, run_id: str) -> str:
        """Stop a queued or active run. Returns the run's status after the request."""
        run_id = self._renamed.get(run_id, run_id)
        async with self._sm() as s:
            run = await s.get(Run, run_id)
            if run is None:
                raise NotFound(f"run {run_id} not found")
            status = run.status
            if status == "queued":
                run.status, run.ended_at = "cancelled", now()
                given_back = await late.followup_not_started(s, run_id, "cancelled")
                await s.commit()
                for a in given_back:
                    self._hub.publish("approval.updated", approval_out(a))
                self._hub.publish(
                    "run.finished",
                    {"run_id": run_id, "conversation_id": run.conversation_id,
                     "status": "cancelled", "usage": None, "error": None},
                )  # fmt: skip
                return "cancelled"
            if status in TERMINAL_RUN_STATUSES or status == "stopping":
                return status
            if status == "starting":
                self._stop_requested.add(run_id)
                return "stopping"
            run.status = "stopping"
            await s.commit()
            conv_id = run.conversation_id
        if self._ctx is not None and self._ctx.run_id == run_id:
            self._ctx.status = "stopping"
        self._hub.publish(
            "run.updated", {"run_id": run_id, "conversation_id": conv_id, "status": "stopping"}
        )
        try:
            await self._hermes.stop_run(run_id)
        except (HermesError, HermesUnavailable) as exc:
            # the stream (or its recovery) settles the run either way
            log.warning("stop request failed", extra={"run_id": run_id, "error": str(exc)})
        return "stopping"

    async def set_conversation(
        self, conversation_id: str, *, thinking: bool | None = None, archived: bool | None = None
    ) -> dict[str, Any]:
        async with self._sm() as s:
            conv = await s.get(Conversation, conversation_id)
            if conv is None:
                raise NotFound(f"conversation {conversation_id} not found")
            changed = False
            if thinking is not None and conv.thinking != thinking:
                conv.thinking, changed = thinking, True
            if archived is not None and conv.archived != archived:
                conv.archived, changed = archived, True
            if changed:
                conv.updated_at = now()
            await s.commit()
            out = conversation_out(conv)
        if changed:
            self._hub.publish("conversation.updated", out)
        return out

    async def start_followup(self, request_id: str, choice: str, decided_by: str) -> dict[str, Any]:
        """Late `once`/`session` for an approval Hermes gave up on (plan 6.5 step 4).

        Queues a follow-up run in the same conversation; the approval becomes
        `late_approved` and follows the run (see delve.runs.late).
        """
        if choice not in ("once", "session"):
            raise InvalidRequest("a late approval is once or session")
        async with self._sm() as s:
            a = (
                await s.execute(
                    select(Approval).where(Approval.request_id == request_id).with_for_update()
                )
            ).scalar_one_or_none()
            if a is None:
                raise NotFound(f"approval {request_id} not found")
            if a.status != "expired_in_hermes":
                raise LateApprovalError("approval_not_late", f"approval is {a.status}")
            if a.expires_at <= now():
                a.status = "expired"
                await s.commit()
                self._hub.publish("approval.updated", approval_out(a))
                raise LateApprovalError("approval_expired", "the approval has expired")
            conv = _must(await s.get(Conversation, a.conversation_id))
            conv.archived, conv.updated_at = False, now()
            qid = f"q_{uuid.uuid4().hex}"
            text = late.late_prompt(a.command)
            msg = Message(
                conversation_id=conv.id,
                run_id=qid,
                role="system",
                content=text,
                client_msg_id=uuid.uuid4(),
            )
            run = Run(
                run_id=qid,
                conversation_id=conv.id,
                idempotency_key=uuid.uuid4(),
                status="queued",
                input=text,
                thinking=conv.thinking,
                origin="late_approval",
            )
            a.status, a.choice, a.decided_by = "late_approved", choice, decided_by
            a.responded_at, a.followup_run_id = now(), qid
            s.add_all([msg, run])
            await s.commit()
            position = await self._position(s, run)
            out = approval_out(a)
            conv_out, msg_out = conversation_out(conv), message_out(msg)
        self._hub.publish("approval.updated", out)
        self._hub.publish("conversation.updated", conv_out)
        self._hub.publish("message.created", {"conversation_id": conv.id, "message": msg_out})
        self._hub.publish(
            "run.queued",
            {"run_id": qid, "conversation_id": conv.id, "position": position,
             "thinking": conv.thinking, "origin": "late_approval"},
        )  # fmt: skip
        log.info("late approval queued", extra={"request_id": request_id, "run_id": qid})
        self.idle.clear()
        self.wake()
        return out

    # --- housekeeping ----------------------------------------------------------

    async def _maintenance(self) -> None:
        while True:
            for tick in (self._health_tick, self._expire_tick):
                try:
                    await tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("maintenance step failed")
            await asyncio.sleep(self._t.maintenance_s)

    async def _health_tick(self) -> None:
        """Poll Hermes /health; on recovery reconcile approvals (plan 6.11)."""
        try:
            health = await self._hermes.health()
            ok, version = health.status == "ok", health.version
        except (HermesError, HermesUnavailable, ValueError):
            ok, version = False, self.hermes_version
        was = self.hermes_ok
        self.hermes_ok, self.hermes_version = ok, version
        if was == ok or (was is None and ok):  # the first good poll is not news
            return
        if ok and self._expected_version and version != self._expected_version:
            log.warning(
                "unexpected hermes version",
                extra={"version": version, "expected": self._expected_version},
            )
        self._hub.publish(
            "health.status", {"hermes": "ok" if ok else "down", "hermes_version": version}
        )
        log.info("hermes health changed", extra={"ok": ok, "version": version})
        if ok and was is False:
            await self._reconcile()

    async def _expire_tick(self) -> None:
        async with self._sm() as s:
            expired = await late.expire_due(s, now())
            await s.commit()
            outs = [approval_out(a) for a in expired]
        for o in outs:
            self._hub.publish("approval.updated", o)

    async def _reconcile(self) -> None:
        async with self._sm() as s:
            changed = await late.reconcile_pending(s, self.active_run_id)
            await s.commit()
            outs = [approval_out(a) for a in changed]
        for o in outs:
            self._hub.publish("approval.updated", o)
        if outs:
            log.info("pending approvals reconciled", extra={"count": len(outs)})

    # --- helpers for submit ----------------------------------------------------

    async def _replay(self, s: AsyncSession, client_msg_id: uuid.UUID) -> SubmitResult | None:
        msg = (
            await s.execute(select(Message).where(Message.client_msg_id == client_msg_id))
        ).scalar_one_or_none()
        if msg is None:
            return None
        conv = _must(await s.get(Conversation, msg.conversation_id))
        return SubmitResult(
            conversation_out(conv), message_out(msg), msg.run_id or "", created=False
        )

    async def _position(self, s: AsyncSession, run: Run) -> int:
        """Runs ahead of this one: the active run plus earlier queued ones (the order of
        `GET /api/v1/runs/open`)."""
        ahead = await s.scalar(
            select(func.count())
            .select_from(Run)
            .where(
                (Run.status.in_(ACTIVE_RUN_STATUSES))
                | (
                    (Run.status == "queued")
                    & (
                        (Run.queued_at < run.queued_at)
                        | ((Run.queued_at == run.queued_at) & (Run.run_id < run.run_id))
                    )
                )
            )
        )
        return int(ahead or 0)

    # --- worker ----------------------------------------------------------------

    async def _worker(self) -> None:
        delay = 1.0
        while True:
            try:
                await self._recover()
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("recovery failed; retrying")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
        while True:
            # clear before reading the queue, so a submit right after the read still wakes us
            self._wakeup.clear()
            try:
                qid = await self._next_queued()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("queue read failed")
                await asyncio.sleep(self._t.idle_poll_s)
                continue
            if qid is None:
                self.idle.set()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wakeup.wait(), self._t.idle_poll_s)
                continue
            self.idle.clear()
            try:
                await self._execute(qid)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("run failed unexpectedly", extra={"run_id": qid})
                await self._fail_quietly(self.active_run_id or qid, "internal_error")
            finally:
                self.active_run_id = None

    async def _next_queued(self) -> str | None:
        async with self._sm() as s:
            return await s.scalar(
                select(Run.run_id)
                .where(Run.status == "queued")
                .order_by(Run.queued_at, Run.run_id)
                .limit(1)
            )

    async def _recover(self) -> None:
        """Resume runs that were active when the backend stopped (plan 6.11, partial).

        A `q_` run in `starting` is resubmitted with its Idempotency-Key, so Hermes
        returns the run it may already have created. A run with a Hermes id is resumed
        from its stored events; if Hermes no longer knows it, it fails as `run_lost`.
        """
        async with self._sm() as s:
            runs = (
                (
                    await s.execute(
                        select(Run)
                        .where(Run.status.in_(ACTIVE_RUN_STATUSES))
                        .order_by(Run.queued_at)
                    )
                )
                .scalars()
                .all()
            )
        for run in runs:
            log.info("recovering run", extra={"run_id": run.run_id, "status": run.status})
            self.active_run_id = run.run_id
            try:
                if run.run_id.startswith("q_"):
                    await self._execute(run.run_id, resume=True)
                else:
                    ctx = await self._rebuild(run.run_id)
                    if ctx is not None:
                        await self._consume(ctx)
            finally:
                self.active_run_id = None
        await self._reconcile()

    # --- one run ---------------------------------------------------------------

    async def _execute(self, qid: str, *, resume: bool = False) -> None:
        async with self._sm() as s:
            run = await s.get(Run, qid)
            if run is None or run.status != ("starting" if resume else "queued"):
                return
            conv = _must(await s.get(Conversation, run.conversation_id))
            run.thinking = conv.thinking  # the state when the run starts decides the route
            run.status, run.started_at = "starting", run.started_at or now()
            await s.commit()
            conv_id, thinking, text, key = conv.id, run.thinking, run.input, run.idempotency_key
            instructions, origin = run.instructions, run.origin
        self.active_run_id = qid
        meter = self._new_meter()
        if meter is not None:
            await meter.prepare()  # before submitting: tasks seen now are not this run's

        created = await self._create_with_retry(
            qid, conv_id, text=text, thinking=thinking, key=key, instructions=instructions
        )
        if created is None:
            return
        run_id = created

        async with self._sm() as s:
            await s.execute(
                update(Run).where(Run.run_id == qid).values(run_id=run_id, status="running")
            )
            await s.execute(update(Message).where(Message.run_id == qid).values(run_id=run_id))
            await s.execute(
                update(Conversation)
                .where(Conversation.id == conv_id)
                .values(last_run_id=run_id, updated_at=now())
            )
            msg = Message(
                conversation_id=conv_id, run_id=run_id, role="assistant", status="streaming"
            )
            s.add(msg)
            followed = await late.followup_started(s, qid, run_id)
            await s.commit()
            msg_out = message_out(msg)
        self._renamed[qid] = run_id
        self.active_run_id = run_id
        self._hub.publish(
            "run.started",
            {"run_id": run_id, "queued_id": qid, "conversation_id": conv_id, "thinking": thinking},
        )
        self._hub.publish("message.created", {"conversation_id": conv_id, "message": msg_out})
        for a in followed:
            self._hub.publish("approval.updated", approval_out(a))

        ctx = _Ctx(
            run_id,
            conv_id,
            msg.id,
            thinking,
            "running",
            last_flush=time.monotonic(),
            origin=origin,
            meter=meter,
        )
        if qid in self._stop_requested:
            self._stop_requested.discard(qid)
            self._ctx = ctx
            await self.stop(run_id)
        await self._consume(ctx)

    async def _create_with_retry(
        self,
        qid: str,
        conv_id: str,
        *,
        text: str,
        thinking: bool,
        key: uuid.UUID,
        instructions: str | None,
    ) -> str | None:
        """POST /v1/runs; returns the Hermes run id, or None if the run was settled."""
        deadline = time.monotonic() + self._t.submit_timeout_s
        attempt = 0
        while True:
            if qid in self._stop_requested:
                self._stop_requested.discard(qid)
                await self._settle_unstarted(qid, conv_id, "cancelled", None)
                return None
            try:
                created = await self._hermes.create_run(
                    session_id=conv_id,
                    input=text,
                    thinking=thinking,
                    idempotency_key=key,
                    instructions=instructions,
                )
                return created.run_id
            except HermesError as exc:
                if exc.status not in RETRYABLE_STATUS:
                    await self._settle_unstarted(
                        qid, conv_id, "failed", f"hermes_{exc.status}_{exc.code or 'error'}"
                    )
                    return None
                reason = f"{exc.status} {exc.code}"
            except HermesUnavailable as exc:
                reason = str(exc)
            attempt += 1
            if time.monotonic() >= deadline:
                await self._settle_unstarted(qid, conv_id, "failed", "hermes_unavailable")
                return None
            log.warning("run submission retry", extra={"run_id": qid, "reason": reason})
            await asyncio.sleep(min(2.0 ** (attempt - 1) * 0.5, self._t.max_backoff_s))

    async def _settle_unstarted(
        self, qid: str, conv_id: str, status: str, error: str | None
    ) -> None:
        async with self._sm() as s:
            await s.execute(
                update(Run)
                .where(Run.run_id == qid)
                .values(status=status, ended_at=now(), error=error)
            )
            given_back = await late.followup_not_started(s, qid, status)
            await s.commit()
        for a in given_back:
            self._hub.publish("approval.updated", approval_out(a))
        self._hub.publish(
            "run.finished",
            {"run_id": qid, "conversation_id": conv_id, "status": status, "usage": None,
             "error": error},
        )  # fmt: skip

    async def _rebuild(self, run_id: str) -> _Ctx | None:
        async with self._sm() as s:
            run = await s.get(Run, run_id)
            if run is None:
                return None
            msg = (
                await s.execute(
                    select(Message)
                    .where(Message.run_id == run_id, Message.role == "assistant")
                    .order_by(Message.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if msg is None:
                msg = Message(
                    conversation_id=run.conversation_id, run_id=run_id, role="assistant",
                    status="streaming",
                )  # fmt: skip
                s.add(msg)
                await s.commit()
            events = (
                (
                    await s.execute(
                        select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.seq)
                    )
                )
                .scalars()
                .all()
            )
            ctx = _Ctx(
                run_id, run.conversation_id, msg.id, run.thinking, run.status, origin=run.origin
            )
        for ev in events:
            ctx.last_seq = ev.seq
            if ev.event == "message.delta" and isinstance(ev.payload.get("delta"), str):
                ctx.text.append(ev.payload["delta"])
            elif ev.event == "message.interim" and isinstance(ev.payload.get("text"), str):
                ctx.interim.append(ev.payload["text"])
            elif ev.event == "tool.started":
                ctx.tool_steps += 1
        ctx.last_flush = time.monotonic()
        ctx.measure_first_delta = False
        self.active_run_id = run_id
        return ctx

    # --- streaming -------------------------------------------------------------

    async def _consume(self, ctx: _Ctx) -> None:
        self._ctx = ctx
        if ctx.meter is None and (meter := self._new_meter()) is not None:
            await meter.prepare(exclude_current=False)  # resumed run: its call may be live
            ctx.meter = meter
        if ctx.meter is not None:
            ctx.meter.on_sample = lambda m: self._publish_context(ctx, m, final=False)
            ctx.meter.start()
        try:
            await self._consume_inner(ctx)
        finally:
            self._ctx = None
            if ctx.meter is not None:
                await ctx.meter.stop()

    def _new_meter(self) -> ContextMeter | None:
        if self._llama is None:
            return None
        return ContextMeter(self._llama, interval_s=self._t.context_poll_s)

    def _publish_context(self, ctx: _Ctx, m: Measurement, *, final: bool) -> None:
        self._hub.publish(
            "context.updated",
            {"conversation_id": ctx.conversation_id, "run_id": ctx.run_id,
             "tokens": m.tokens, "max": m.max, "pct": m.pct,
             "warn": m.pct >= CONTEXT_WARN_PCT, "source": m.source,
             "measured_at": now().isoformat(), "final": final},
        )  # fmt: skip

    async def _consume_inner(self, ctx: _Ctx) -> None:
        failures = 0
        while True:
            try:
                async for ev in self._hermes.stream_events(ctx.run_id, after_seq=ctx.last_seq):
                    failures = 0
                    await self._on_event(ctx, ev)
                    if ev.terminal:
                        await self._finish(
                            ctx,
                            EVENT_TO_STATUS[ev.event],
                            output=_str(ev.data.get("output")),
                            usage=_dict(ev.data.get("usage")),
                            runtime=_dict(ev.data.get("runtime")),
                            error=_error_text(ev.data),
                        )
                        return
                raise StreamEnded(ctx.run_id, ctx.last_seq)  # defensive: no terminal event
            except (StreamEnded, HermesUnavailable, HermesProtocolError, HermesError) as exc:
                failures += 1
                ctx.reconnects += 1
                log.warning(
                    "event stream interrupted",
                    extra={"run_id": ctx.run_id, "last_seq": ctx.last_seq, "error": str(exc)},
                )
            await self._flush(ctx, force=True)
            prev_status = ctx.status
            await self._set_status(ctx, "reconnecting")
            status = await self._resolve(ctx.run_id)
            if status is None:
                await self._finish(ctx, "failed", error="run_lost")
                return
            if status.terminal:
                await self._finish(
                    ctx,
                    HERMES_TO_STATUS.get(status.status, "failed"),
                    output=status.output,
                    usage=status.usage.model_dump(exclude_none=True) if status.usage else None,
                    runtime=status.runtime,
                    error=status.turn_exit_reason if status.status != "completed" else None,
                )
                return
            if failures >= self._t.max_stream_failures:
                await self._finish(ctx, "failed", error="stream_unstable")
                return
            await self._set_status(ctx, _live_status(status.status, prev_status))
            await asyncio.sleep(min(0.25 * 2 ** (failures - 1), self._t.max_backoff_s))

    async def _resolve(self, run_id: str) -> RunStatus | None:
        """GET /v1/runs/{id}, retrying while Hermes is down. None = Hermes lost the run."""
        deadline = time.monotonic() + self._t.resolve_timeout_s
        attempt = 0
        while True:
            try:
                return await self._hermes.get_run(run_id)
            except HermesError as exc:
                if exc.status == 404:
                    return None
                if exc.status not in RETRYABLE_STATUS:
                    raise
            except HermesUnavailable:
                pass
            attempt += 1
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(0.5 * 2 ** (attempt - 1), self._t.max_backoff_s))

    async def _on_event(self, ctx: _Ctx, ev: HermesEvent) -> None:
        """Store the event and its effects in one transaction, then publish."""
        name, data = ev.event, ev.data
        base = {"run_id": ctx.run_id, "conversation_id": ctx.conversation_id}
        out: list[tuple[str, dict[str, Any]]] = []
        new_status: str | None = None
        auto: tuple[str, str] | None = None
        async with self._sm() as s:
            inserted = await s.scalar(
                pg_insert(RunEvent)
                .values(run_id=ctx.run_id, seq=ev.seq, event=name, payload=data)
                .on_conflict_do_nothing()
                .returning(RunEvent.seq)
            )
            if inserted is None:  # already processed before a restart
                ctx.last_seq = ev.seq
                return
            if name == "tool.started":
                ctx.tool_steps += 1
                step = await steps.record_tool_started(s, ctx.run_id, ev.seq, data)
                out.append(("tool.started", {**base, "step": step_out(step)}))
            elif name == "tool.completed":
                step, expired = await steps.record_tool_completed(s, ctx.run_id, ev.seq, data)
                out.append(("tool.completed", {**base, "step": step_out(step, expired)}))
                if expired is not None:  # Hermes timed the approval out (plan 3.7)
                    out.append(("approval.updated", approval_out(expired)))
                if ctx.status == "waiting_for_approval":
                    new_status = "running"
            elif name == "approval.request":
                recorded = await steps.record_approval_request(
                    s,
                    run_id=ctx.run_id,
                    conversation_id=ctx.conversation_id,
                    data=data,
                    received_at=now(),
                    hermes_timeout=timedelta(seconds=self._approval_timeout_s),
                )
                if recorded is not None:
                    req, req_step = recorded
                    payload = approval_out(req, steps.offered_choices(data.get("choices")))
                    payload["step_id"] = req_step.id if req_step is not None else None
                    out.append(("approval.request", payload))
                    if ctx.origin == "late_approval":
                        orig, same = await late.match_followup(s, ctx.run_id, req)
                        if same and orig is not None:
                            auto = (req.request_id, orig.request_id)
                        elif orig is not None:
                            out.append(("approval.updated", approval_out(orig)))
                new_status = "waiting_for_approval"
            elif name == "approval.responded":
                answered = await steps.record_approval_responded(s, data)
                if answered is not None:
                    out.append(("approval.updated", approval_out(answered)))
                if ctx.status == "waiting_for_approval":
                    new_status = "running"
            await s.commit()
        ctx.last_seq = ev.seq

        if name == "message.delta":
            delta = data.get("delta")
            if isinstance(delta, str) and delta:
                if ctx.first_delta_s is None and ctx.measure_first_delta:
                    ctx.first_delta_s = round(time.monotonic() - ctx.started_mono, 3)
                ctx.text.append(delta)
                ctx.dirty = True
                self._hub.publish(
                    "message.delta", {**base, "message_id": ctx.message_id, "delta": delta}
                )
                await self._flush(ctx)
            return
        await self._flush(ctx, force=True)
        if name == "message.interim":
            text = data.get("text")
            if isinstance(text, str) and text.strip():
                ctx.interim.append(text)
                out.append(
                    ("message.interim", {**base, "message_id": ctx.message_id, "text": text})
                )
        for type_, payload in out:
            self._hub.publish(type_, payload)
        if new_status is not None:
            await self._set_status(ctx, new_status)
        if auto is not None:
            await self._auto_approve(ctx, *auto)

    async def _auto_approve(self, ctx: _Ctx, request_id: str, orig_request_id: str) -> None:
        """Same command as the late-approved one: answer `once` for the user."""
        try:
            await self._hermes.respond_approval(ctx.run_id, request_id, "once")
        except (HermesError, HermesUnavailable) as exc:
            # the request stays pending; the user can still answer it by hand
            log.warning(
                "late approval auto-answer failed",
                extra={"run_id": ctx.run_id, "request_id": request_id, "error": str(exc)},
            )
            return
        async with self._sm() as s:
            changed = await late.auto_approved(s, request_id, orig_request_id)
            await s.commit()
            outs = [approval_out(a) for a in changed]
        for o in outs:
            self._hub.publish("approval.updated", o)

    async def _flush(self, ctx: _Ctx, *, force: bool = False) -> None:
        if not ctx.dirty:
            return
        if not force and time.monotonic() - ctx.last_flush < self._t.flush_interval_s:
            return
        async with self._sm() as s:
            await s.execute(
                update(Message)
                .where(Message.id == ctx.message_id)
                .values(content="".join(ctx.text))
            )
            await s.commit()
        ctx.dirty, ctx.last_flush = False, time.monotonic()

    async def _set_status(self, ctx: _Ctx, status: str) -> None:
        if ctx.status == status:
            return
        if ctx.status == "stopping" and status in ("running", "waiting_for_approval"):
            return  # a stop is in flight; only the terminal event moves it on
        ctx.status = status
        async with self._sm() as s:
            await s.execute(update(Run).where(Run.run_id == ctx.run_id).values(status=status))
            await s.commit()
        self._hub.publish(
            "run.updated",
            {"run_id": ctx.run_id, "conversation_id": ctx.conversation_id, "status": status},
        )

    async def _finish(
        self,
        ctx: _Ctx,
        status: str,
        *,
        output: str | None = None,
        usage: dict[str, Any] | None = None,
        runtime: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        streamed = "".join(ctx.text)
        content = output if status == "completed" and output is not None else streamed
        interim = "\n\n".join(ctx.interim) if ctx.interim else None
        measured: Measurement | None = None
        perf: dict[str, Any] = {"sse_reconnects": ctx.reconnects}
        if ctx.first_delta_s is not None:
            perf["first_delta_s"] = ctx.first_delta_s
        if ctx.meter is not None:
            # read the slot before anything else: Hermes' title call follows within ~0.2 s
            measured, llama_perf = await ctx.meter.finish(usage=usage, tool_steps=ctx.tool_steps)
            perf.update(llama_perf)
        route_ok: bool | None = None
        if status == "completed" and ctx.thinking:
            route_ok = route_applied(runtime, self._hermes.route)
            perf["route_applied"] = route_ok
        async with self._sm() as s:
            run = _must(await s.get(Run, ctx.run_id))
            run.status, run.ended_at, run.error = status, now(), error
            run.usage, run.perf = usage, perf
            msg = _must(await s.get(Message, ctx.message_id))
            msg.content, msg.interim, msg.status = content, interim, MESSAGE_STATUS_FOR[status]
            open_steps, open_approvals = await steps.settle_open(s, ctx.run_id, status, now())
            if ctx.origin == "late_approval":
                open_approvals += await late.settle_followup(s, ctx.run_id, status)
            conv = _must(await s.get(Conversation, ctx.conversation_id))
            conv.updated_at = now()
            conv.tokens_in += _int(usage, "input_tokens")
            conv.tokens_out += _int(usage, "output_tokens")
            conv.tokens_cache_read += _int(usage, "cache_read_tokens")
            if measured is not None:
                conv.context_tokens, conv.context_max = measured.tokens, measured.max
                conv.context_measured_at = now()
            await s.commit()
            msg_out, conv_out = message_out(msg), conversation_out(conv)
        ctx.status = status
        base = {"run_id": ctx.run_id, "conversation_id": ctx.conversation_id}
        for step in open_steps:
            self._hub.publish("tool.completed", {**base, "step": step_out(step)})
        for approval in open_approvals:
            self._hub.publish("approval.updated", approval_out(approval))
        self._hub.publish(
            "message.updated", {"conversation_id": ctx.conversation_id, "message": msg_out}
        )
        self._hub.publish(
            "run.finished",
            {"run_id": ctx.run_id, "conversation_id": ctx.conversation_id, "status": status,
             "usage": usage, "error": error},
        )  # fmt: skip
        if measured is not None:
            self._publish_context(ctx, measured, final=True)
        self._hub.publish("conversation.updated", conv_out)
        log.info(
            "run finished",
            extra={"run_id": ctx.run_id, "status": status, "error": error,
                   "reconnects": ctx.reconnects},
        )  # fmt: skip
        if route_ok is not None:
            await self._record_route(ctx, route_ok, runtime)
        if status == "completed" and ctx.thinking:
            await self._sync_reasoning(ctx)
        self._spawn(self._sync_title(ctx.conversation_id))

    async def _fail_quietly(self, run_id: str, error: str) -> None:
        with contextlib.suppress(Exception):
            async with self._sm() as s:
                await s.execute(
                    update(Run)
                    .where(Run.run_id == run_id, Run.status.not_in(TERMINAL_RUN_STATUSES))
                    .values(status="failed", ended_at=now(), error=error)
                )
                await s.execute(
                    update(Message)
                    .where(Message.run_id == run_id, Message.status == "streaming")
                    .values(status="failed")
                )
                await s.commit()

    # --- post-completed ----------------------------------------------------------

    async def _record_route(self, ctx: _Ctx, ok: bool, runtime: dict[str, Any] | None) -> None:
        """Decision 138: every thinking run checks that the per-run route was used."""
        value = {"ok": ok, "run_id": ctx.run_id, "at": now().isoformat(), "runtime": runtime}
        async with self._sm() as s:
            await s.execute(
                pg_insert(SystemState)
                .values(key="thinking_route", value=value)
                .on_conflict_do_update(
                    index_elements=[SystemState.key], set_={"value": value, "updated_at": now()}
                )
            )
            await s.commit()
        if not ok:
            log.error(
                "thinking route not applied",
                extra={"run_id": ctx.run_id, "runtime": runtime},
            )
            self._hub.publish(
                "error",
                {"code": "thinking_route_not_applied", "ref_id": ctx.run_id,
                 "message": "The thinking run did not use the thinking provider."},
            )  # fmt: skip

    async def _sync_reasoning(self, ctx: _Ctx) -> None:
        """Plan 6.4/3: the thinking text comes from /messages after the run."""
        try:
            hermes_msgs = await self._hermes.get_session_messages(ctx.conversation_id)
        except (HermesError, HermesUnavailable) as exc:
            log.warning("reasoning sync failed", extra={"run_id": ctx.run_id, "error": str(exc)})
            return
        tail = _after_last_user(hermes_msgs)
        async with self._sm() as s:
            known = set(
                (
                    await s.execute(
                        select(Message.hermes_message_id).where(
                            Message.conversation_id == ctx.conversation_id,
                            Message.hermes_message_id.is_not(None),
                        )
                    )
                ).scalars()
            )
            fresh = new_reasoning(tail, {k for k in known if k})
            if not fresh:
                return
            text = "\n\n".join(m.thinking_text or "" for _, m in fresh)
            last_assistant = [str(m.id) for m in tail if m.role == "assistant"]
            hermes_id = last_assistant[-1] if last_assistant else fresh[-1][0]
            msg = _must(await s.get(Message, ctx.message_id))
            msg.reasoning = text
            if hermes_id not in known:
                msg.hermes_message_id = hermes_id
            await s.commit()
        self._hub.publish(
            "reasoning.final",
            {"run_id": ctx.run_id, "conversation_id": ctx.conversation_id,
             "message_id": ctx.message_id, "text": text},
        )  # fmt: skip

    async def _sync_title(self, conversation_id: str) -> None:
        for delay in self._t.title_delays_s:
            await asyncio.sleep(delay)
            try:
                info = await self._hermes.get_session(conversation_id)
            except HermesError as exc:
                if exc.status == 404:
                    return
                continue
            except HermesUnavailable:
                continue
            if not info.title:
                continue
            async with self._sm() as s:
                conv = await s.get(Conversation, conversation_id)
                if conv is None or conv.title == info.title:
                    return
                conv.title = info.title
                await s.commit()
                out = conversation_out(conv)
            self._hub.publish("conversation.updated", out)
            return

    def _spawn(self, coro: Any) -> None:
        task: asyncio.Task[None] = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def wait_background(self) -> None:
        """Wait for post-run tasks (tests)."""
        while self._bg:
            tasks = list(self._bg)
            await asyncio.gather(*tasks, return_exceptions=True)
            # done callbacks run on the next loop turn; drop finished tasks here so this
            # loop cannot spin on already-done tasks without ever yielding
            self._bg.difference_update(tasks)


def _must[T](obj: T | None) -> T:
    if obj is None:
        raise RuntimeError("row disappeared")  # FK / single-writer invariant broken
    return obj


def _after_last_user(msgs: Sequence[SessionMessage]) -> list[SessionMessage]:
    """This run's messages: everything after the last user message (runs are serial)."""
    ordered = sorted(msgs, key=lambda m: (0, m.id, "") if isinstance(m.id, int) else (1, 0, m.id))
    last_user = max((i for i, m in enumerate(ordered) if m.role == "user"), default=-1)
    return ordered[last_user + 1 :]


def _live_status(hermes_status: str, previous: str) -> str:
    if previous == "stopping" or hermes_status == "stopping":
        return "stopping"
    if hermes_status == "waiting_for_approval":
        return "waiting_for_approval"
    return "running"


def _int(d: dict[str, Any] | None, key: str) -> int:
    v = (d or {}).get(key)
    return v if isinstance(v, int) and not isinstance(v, bool) and v > 0 else 0


def _str(v: Any) -> str | None:
    return v if isinstance(v, str) else None


def _dict(v: Any) -> dict[str, Any] | None:
    return v if isinstance(v, dict) else None


def _error_text(data: dict[str, Any]) -> str | None:
    if data.get("event") == "run.completed":
        return None
    err = data.get("error")
    if isinstance(err, str) and err:
        return err[:500]
    if isinstance(err, dict):
        return str(err.get("message") or err.get("code") or "error")[:500]
    reason = data.get("turn_exit_reason")
    return reason if isinstance(reason, str) else None
