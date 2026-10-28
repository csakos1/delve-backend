"""The system view and llama-server control (B3).

The backend's unit has no capabilities, no devices and its own user namespace, so the
unit states, `nvidia-smi` and starting/stopping llama-server come from the privileged
helper (`delve-infra/bin/delve-helper`, one JSON line per connection on a unix socket).
What loopback HTTP can answer is read directly: llama `/health` and `/props`, Hermes
`/health/detailed`. The queue and the last generation speed come from the database.

Stopping llama-server on purpose holds the run queue (queued runs wait instead of
failing) until it is started again from here, or until the maintenance watch sees it
healthy after a start from elsewhere. The intent survives a backend restart
(`system_state['llama_intent']`).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import ACTIVE_RUN_STATUSES, Run, SystemState
from delve.events import EventHub
from delve.hermes.client import HermesClient, HermesError, HermesUnavailable
from delve.llama import LlamaClient, LlamaUnavailable, Props
from delve.runs.manager import NotFound, RunManager

log = logging.getLogger("delve.system")

INTENT_KEY = "llama_intent"
SLOTS_KEY = "llama_slots"  # the slot count from the last /props, for a stopped llama (N51)
STATUS_TIMEOUT_S = 15.0  # helper's own systemctl show limit is 10 s
CONTROL_TIMEOUT_S = 45.0  # stop/start (25 s) + the status read after it (10 s)
MAX_RESPONSE = 64 * 1024

LLAMA_STATES = ("running", "loading", "starting", "stopping", "stopped", "failed", "down",
                "unknown")  # fmt: skip
HERMES_STATES = ("ok", "degraded", "down", "unknown")
GPU_KEYS = ("index", "name", "temp_c", "mem_used_mib", "mem_total_mib", "util_pct",
            "power_w", "power_limit_w")  # fmt: skip


class HelperUnavailable(Exception):
    """The helper socket is missing, refused the connection or did not answer."""


class HelperFailed(Exception):
    """The helper answered with an error (bad verb, systemctl or nvidia-smi failure)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class SystemConflict(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class HelperClient:
    def __init__(self, path: Path, *, connect_timeout_s: float = 2.0) -> None:
        self.path = path
        self._connect_timeout_s = connect_timeout_s

    async def call(
        self, verb: str, *, arg: str | None = None, timeout_s: float = STATUS_TIMEOUT_S
    ) -> dict[str, Any]:
        try:
            async with asyncio.timeout(self._connect_timeout_s):
                reader, writer = await asyncio.open_unix_connection(
                    str(self.path), limit=MAX_RESPONSE
                )
        except (OSError, TimeoutError) as exc:
            raise HelperUnavailable(f"{self.path}: {type(exc).__name__}") from exc
        try:
            req = {"verb": verb} if arg is None else {"verb": verb, "arg": arg}
            writer.write(json.dumps(req).encode() + b"\n")
            async with asyncio.timeout(timeout_s):
                await writer.drain()
                line = await reader.readline()
        except (OSError, TimeoutError, ValueError) as exc:  # ValueError: line over the limit
            raise HelperUnavailable(f"{verb}: {type(exc).__name__}") from exc
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        try:
            out = json.loads(line)
        except ValueError as exc:
            raise HelperUnavailable(f"{verb}: no answer") from exc
        if not isinstance(out, dict):
            raise HelperUnavailable(f"{verb}: unexpected answer")
        if out.get("ok") is not True:
            raise HelperFailed(str(out.get("error") or "failed"), str(out.get("message") or ""))
        data = out.get("data")
        if not isinstance(data, dict):
            raise HelperUnavailable(f"{verb}: answer without data")
        return data


# --- building the view (pure, used by the contract too) ------------------------------


def _iso_unix(ts: Any) -> str | None:
    if isinstance(ts, int) and not isinstance(ts, bool) and ts > 0:
        return datetime.fromtimestamp(ts, UTC).isoformat()
    return None


def llama_state(unit: dict[str, Any] | None, healthy: bool | None) -> str:
    """The unit's state from the helper, refined by `/health` while it is active."""
    if unit is not None:
        state = unit.get("active_state")
        if state == "active":
            return "running" if healthy else "loading"
        return {
            "activating": "starting",
            "reloading": "loading",
            "deactivating": "stopping",
            "inactive": "stopped",
            "failed": "failed",
        }.get(str(state), "unknown")
    if healthy is True:
        return "running"
    if healthy is False:
        return "loading"
    return "down"  # no helper and no answer: stopped or crashed, cannot tell


def hermes_issues(detail: dict[str, Any] | None) -> list[dict[str, Any]]:
    readiness = detail.get("readiness") if detail else None
    checks = readiness.get("checks") if isinstance(readiness, dict) else None
    if not isinstance(checks, dict):
        return []
    out = []
    for name, check in sorted(checks.items()):
        if not isinstance(check, dict) or check.get("status") == "ok":
            continue
        text = None
        used = check.get("used_percent")
        if isinstance(used, int | float) and not isinstance(used, bool):
            text = f"{used:.0f}% used"
        else:
            for key in ("error", "message", "detail"):
                if isinstance(check.get(key), str):
                    text = check[key][:200]
                    break
        out.append({"check": str(name), "status": str(check.get("status")), "detail": text})
    return out


def hermes_state(ok: bool | None, detail: dict[str, Any] | None) -> str:
    if ok is None:
        return "unknown"
    if not ok:
        return "down"
    if detail is None:
        return "ok"
    return "ok" if detail.get("status") == "ok" else "degraded"


def gpu_out(raw: dict[str, Any]) -> dict[str, Any]:
    return {k: raw.get(k) for k in GPU_KEYS}


@dataclass
class Parts:
    checked_at: datetime
    helper_errors: list[str] = field(default_factory=list)
    hermes_ok: bool | None = None
    hermes_version: str | None = None
    hermes_detail: dict[str, Any] | None = None
    hermes_unit: dict[str, Any] | None = None
    llama_unit: dict[str, Any] | None = None
    llama_healthy: bool | None = None
    props: Props | None = None
    known_slots: int | None = None
    slot_busy: bool | None = None
    gen_tps: float | None = None
    intent: str | None = None
    active_runs: int = 0
    queued_runs: int = 0
    gpus: list[dict[str, Any]] | None = None


def system_out(p: Parts) -> dict[str, Any]:
    state = llama_state(p.llama_unit, p.llama_healthy)
    unit = p.llama_unit or {}
    if state in ("stopped", "failed"):
        since = _iso_unix(unit.get("inactive_since"))
    else:
        since = _iso_unix(unit.get("active_since"))
    live = state == "running" and p.props is not None
    busy: int | None = None
    if state == "running" and p.slot_busy is not None:
        busy = 1 if p.slot_busy else 0
    elif state in ("stopped", "failed"):
        busy = 0
    h_unit = p.hermes_unit or {}
    return {
        "checked_at": p.checked_at.isoformat(),
        "helper": {
            "ok": not p.helper_errors,
            "error": p.helper_errors[0] if p.helper_errors else None,
        },
        "hermes": {
            "status": hermes_state(p.hermes_ok, p.hermes_detail),
            "version": p.hermes_version,
            "since": _iso_unix(h_unit.get("active_since"))
            if h_unit.get("active_state") == "active"
            else None,
            "issues": hermes_issues(p.hermes_detail) if p.hermes_ok else [],
        },
        "llama": {
            "status": state,
            "intent": p.intent,
            "since": since,
            "model": p.props.model if live and p.props else None,
            "quant": p.props.quant if live and p.props else None,
            "ctx": p.props.n_ctx if live and p.props else None,
            "slots": p.props.slots if p.props else p.known_slots,
            "slots_busy": busy,
            "gen_tps": p.gen_tps,
        },
        "queue": {"active": p.active_runs, "queued": p.queued_runs},
        "gpus": [gpu_out(g) for g in p.gpus] if p.gpus is not None else None,
    }


# --- the service ---------------------------------------------------------------------


class SystemService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        manager: RunManager,
        hub: EventHub,
        hermes: HermesClient,
        llama: LlamaClient,
        helper: HelperClient,
        *,
        start_wait_s: float = 180.0,
        ready_poll_s: float = 1.0,
        watch_s: float = 15.0,
        cache_s: float = 2.0,
    ) -> None:
        self._sm = sessionmaker
        self._mgr = manager
        self._hub = hub
        self._hermes = hermes
        self._llama = llama
        self._helper = helper
        self._start_wait_s = start_wait_s
        self._ready_poll_s = ready_poll_s
        self._watch_s = watch_s
        self._cache_s = cache_s
        self._control = asyncio.Lock()
        self._snap_lock = asyncio.Lock()
        self._cached: tuple[float, dict[str, Any]] | None = None
        self._known_slots: int | None = None
        self._saved_slots: int | None = None
        self._slot_error: str | None = None  # the last /slots failure, logged once
        self._starting: asyncio.Task[None] | None = None
        self._watch: asyncio.Task[None] | None = None
        self.intent: str | None = None  # "stopped" | "starting" | None
        self._intent_loaded = False

    # --- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """Before the run worker starts: a stop from before the restart still holds.

        A database that is not reachable yet does not stop the server from starting; the
        watch loop reads the stored intent once it is.
        """
        await self._load_intent()
        if self._watch is None:
            self._watch = asyncio.create_task(self._watch_loop(), name="system-watch")

    async def _load_intent(self) -> None:
        try:
            async with self._sm() as s:
                row = await s.get(SystemState, INTENT_KEY)
        except Exception as exc:
            log.warning("llama intent not readable yet", extra={"error": type(exc).__name__})
            return
        self._intent_loaded = True
        with contextlib.suppress(Exception):
            async with self._sm() as s:
                slots = await s.get(SystemState, SLOTS_KEY)
            value = slots.value if slots is not None else None
            n = value.get("slots") if isinstance(value, dict) else None
            if isinstance(n, int) and not isinstance(n, bool) and n > 0:
                self._known_slots = self._known_slots or n
                self._saved_slots = n
        if row is not None and isinstance(row.value, dict) and row.value.get("state") == "stopped":
            self.intent = "stopped"
            self._mgr.hold(True)
            log.info("llama-server stopped on purpose; queue held", extra=dict(row.value))

    async def close(self) -> None:
        tasks = [t for t in (self._watch, self._starting) if t is not None]
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._watch = self._starting = None

    # --- view ------------------------------------------------------------------

    async def snapshot(self, *, fresh: bool = False) -> dict[str, Any]:
        async with self._snap_lock:
            if not fresh and self._cached and time.monotonic() - self._cached[0] < self._cache_s:
                return self._cached[1]
            out = system_out(await self._gather())
            self._cached = (time.monotonic(), out)
            if self._known_slots is not None and self._known_slots != self._saved_slots:
                await self._save_slots(self._known_slots)
            return out

    async def _save_slots(self, n: int) -> None:
        stmt = insert(SystemState).values(key=SLOTS_KEY, value={"slots": n})
        stmt = stmt.on_conflict_do_update(
            index_elements=[SystemState.key],
            set_={"value": stmt.excluded.value, "updated_at": func.now()},
        )
        try:
            async with self._sm() as s:
                await s.execute(stmt)
                await s.commit()
            self._saved_slots = n
        except Exception as exc:
            log.warning("slot count not saved", extra={"error": type(exc).__name__})

    async def publish(self) -> dict[str, Any]:
        snap = await self.snapshot(fresh=True)
        self._hub.publish("system.updated", snap)
        return snap

    async def _helper_call(self, verb: str, errors: list[tuple[int, str]], order: int) -> Any:
        try:
            return await self._helper.call(verb, timeout_s=STATUS_TIMEOUT_S)
        except HelperUnavailable:
            errors.append((order, "helper_unavailable"))
        except HelperFailed as exc:
            errors.append((order, f"{verb}: {exc.message or exc.code}"[:200]))
        return None

    async def _gather(self) -> Parts:
        errors: list[tuple[int, str]] = []
        p = Parts(checked_at=datetime.now(UTC), intent=self.intent)
        p.hermes_ok, p.hermes_version = self._mgr.hermes_ok, self._mgr.hermes_version

        async def llama_part() -> None:
            try:
                p.llama_healthy = await self._llama.health()
            except LlamaUnavailable:
                p.llama_healthy = None
                return
            if not p.llama_healthy:
                return
            props, slot = await asyncio.gather(
                self._llama.props(), self._llama.slot(), return_exceptions=True
            )
            if isinstance(props, Props):
                p.props = props
                self._known_slots = props.slots or self._known_slots
            if not isinstance(slot, BaseException):
                p.slot_busy = slot.processing
                self._slot_error = None
            else:
                err = f"{type(slot).__name__}: {slot}"[:200]
                if err != self._slot_error:  # once per kind of failure, not every 10 s
                    log.warning("llama /slots not readable", extra={"error": err})
                self._slot_error = err

        async def hermes_part() -> None:
            if p.hermes_ok is False:
                return
            try:
                p.hermes_detail = await self._hermes.health_detailed()
            except (HermesError, HermesUnavailable):
                p.hermes_detail = None

        async def helper_part() -> None:
            llama_unit, hermes_unit, gpus = await asyncio.gather(
                self._helper_call("llama.status", errors, 0),
                self._helper_call("hermes.status", errors, 1),
                self._helper_call("gpu.status", errors, 2),
            )
            p.llama_unit, p.hermes_unit = llama_unit, hermes_unit
            if isinstance(gpus, dict) and isinstance(gpus.get("gpus"), list):
                p.gpus = [g for g in gpus["gpus"] if isinstance(g, dict)]

        await asyncio.gather(llama_part(), hermes_part(), helper_part(), self._db_part(p))
        p.known_slots = self._known_slots
        # one unreachable socket shows up three times: say it once
        p.helper_errors = list(dict.fromkeys(msg for _, msg in sorted(errors)))
        return p

    async def _db_part(self, p: Parts) -> None:
        async with self._sm() as s:
            rows = (
                await s.execute(
                    select(Run.status, func.count())
                    .where(Run.status.in_((*ACTIVE_RUN_STATUSES, "queued")))
                    .group_by(Run.status)
                )
            ).all()
            perfs = (
                await s.execute(
                    select(Run.perf)
                    .where(Run.status == "completed", Run.perf.is_not(None))
                    .order_by(Run.ended_at.desc().nulls_last())
                    .limit(5)
                )
            ).scalars()
            for perf in perfs:
                tps = perf.get("gen_tps") if isinstance(perf, dict) else None
                if isinstance(tps, int | float) and not isinstance(tps, bool) and tps > 0:
                    p.gen_tps = round(float(tps), 1)
                    break
        counts = {status: int(n) for status, n in rows}
        p.queued_runs = counts.pop("queued", 0)
        p.active_runs = sum(counts.values())

    # --- control ---------------------------------------------------------------

    async def stop_llama(self, *, force: bool, by: str) -> dict[str, Any]:
        async with self._control:
            starting, self._starting = self._starting, None
            if starting is not None:
                starting.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await starting
            was_held = self._mgr.held
            self._mgr.hold(True)  # first: no queued run may start while we decide
            active = self._mgr.active_run_id
            if active is not None and not force:
                self._mgr.hold(was_held)
                raise SystemConflict(
                    "run_active", "a run is active; stopping llama-server would end it"
                )
            if active is not None:
                with contextlib.suppress(NotFound):
                    await self._mgr.stop(active)
            try:
                await self._helper.call("llama.stop", timeout_s=CONTROL_TIMEOUT_S)
            except (HelperUnavailable, HelperFailed):
                self._mgr.hold(was_held)
                raise
            await self._set_intent(by)
            self.intent = "stopped"
            log.info("llama-server stopped", extra={"by": by, "stopped_run": active})
        return await self.publish()

    async def start_llama(self, *, by: str) -> dict[str, Any]:
        async with self._control:
            await self._helper.call("llama.start", timeout_s=CONTROL_TIMEOUT_S)
            # the queue stays held (and the stored intent stays) until the model is loaded
            if self._starting is None:
                self.intent = "starting"
                self._starting = asyncio.create_task(self._wait_ready(), name="llama-ready")
            log.info("llama-server starting", extra={"by": by})
        return await self.publish()

    async def _wait_ready(self) -> None:
        deadline = time.monotonic() + self._start_wait_s
        ready = False
        while time.monotonic() < deadline:
            with contextlib.suppress(LlamaUnavailable):
                if await self._llama.health():
                    ready = True
                    break
            await asyncio.sleep(self._ready_poll_s)
        if not ready:
            log.warning("llama-server not ready in time; releasing the queue anyway")
        await self._release()
        self._starting = None
        try:
            await self.publish()
        except Exception:
            log.exception("system snapshot after start failed")

    async def _release(self) -> None:
        async with self._sm() as s:
            await s.execute(delete(SystemState).where(SystemState.key == INTENT_KEY))
            await s.commit()
        self.intent = None
        self._mgr.hold(False)

    async def _set_intent(self, by: str) -> None:
        value = {"state": "stopped", "at": datetime.now(UTC).isoformat(), "by": by}
        stmt = insert(SystemState).values(key=INTENT_KEY, value=value)
        stmt = stmt.on_conflict_do_update(
            index_elements=[SystemState.key],
            set_={"value": stmt.excluded.value, "updated_at": func.now()},
        )
        async with self._sm() as s:
            await s.execute(stmt)
            await s.commit()

    async def _watch_loop(self) -> None:
        """A llama-server started from the terminal (or a reboot) releases the queue."""
        while True:
            await asyncio.sleep(self._watch_s)
            if not self._intent_loaded and not self._control.locked():
                await self._load_intent()
            if self.intent != "stopped" or self._control.locked():
                continue
            try:
                healthy = await self._llama.health()
            except LlamaUnavailable:
                continue
            except Exception:
                log.exception("system watch failed")
                continue
            if healthy and self.intent == "stopped":
                log.info("llama-server is up again; queue released")
                try:
                    await self._release()
                    await self.publish()
                except Exception:
                    log.exception("releasing the queue failed")
