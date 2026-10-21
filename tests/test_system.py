"""System view and llama-server control (B3): helper client, view, queue hold, API, CLI."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve import cli, protocol
from delve.app import create_app
from delve.config import Settings
from delve.db.models import Run, SystemState
from delve.llama import LlamaClient, Props, model_label
from delve.system import (
    INTENT_KEY,
    SLOTS_KEY,
    HelperClient,
    HelperFailed,
    HelperUnavailable,
    Parts,
    SystemConflict,
    SystemService,
    hermes_issues,
    hermes_state,
    llama_state,
    system_out,
)

from .fake_helper import FakeHelper
from .fake_hermes import StreamingASGITransport, load_json
from .fake_llama import FakeLlama
from .harness import FAST, Harness, make_harness
from .test_api import auth

T0 = datetime(2026, 10, 21, 0, 0, tzinfo=UTC)
R = load_json("system/helper-responses.json")
DETAIL = load_json("system/hermes-health-detailed.json")
PROPS = Props.parse(load_json("system/llama-props.json"))


# --- pure parts -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "ftype", "want"),
    [
        ("/home/operator/models/Qwen3.8-27B-UD-IQ4_XS.gguf", None, ("Qwen3.8-27B", "IQ4_XS")),
        ("/m/Llama-3.1-8B-Instruct-Q4_K_M.gguf", None, ("Llama-3.1-8B-Instruct", "Q4_K_M")),
        ("/m/gemma-3-4b-it-bf16.gguf", None, ("gemma-3-4b-it", "BF16")),
        ("/m/mystery.gguf", "IQ4_XS - 4.25 bpw", ("mystery", "IQ4_XS")),
        ("/m/mystery", None, ("mystery", None)),
    ],
)
def test_model_label(path: str, ftype: str | None, want: tuple[str, str | None]) -> None:
    assert model_label(path, ftype) == want


def test_props_from_recording() -> None:
    assert Props(model="Qwen3.8-27B", quant="IQ4_XS", n_ctx=65536, slots=1) == PROPS
    with pytest.raises(KeyError):
        Props.parse({"total_slots": 1})


@pytest.mark.parametrize(
    ("state", "healthy", "want"),
    [
        ("active", True, "running"),
        ("active", False, "loading"),
        ("active", None, "loading"),
        ("activating", None, "starting"),
        ("deactivating", None, "stopping"),
        ("inactive", None, "stopped"),
        ("failed", None, "failed"),
        ("maintenance", None, "unknown"),
    ],
)
def test_llama_state_from_unit(state: str, healthy: bool | None, want: str) -> None:
    assert llama_state({"active_state": state}, healthy) == want


def test_llama_state_without_helper() -> None:
    assert [llama_state(None, h) for h in (True, False, None)] == ["running", "loading", "down"]


def test_hermes_degraded_by_disk() -> None:
    assert hermes_issues(DETAIL) == [{"check": "disk", "status": "degraded", "detail": "96% used"}]
    assert hermes_state(True, DETAIL) == "degraded"
    assert hermes_state(True, {**DETAIL, "status": "ok"}) == "ok"
    assert hermes_state(True, None) == "ok"  # detail not readable: the plain health decides
    assert (hermes_state(False, DETAIL), hermes_state(None, None)) == ("down", "unknown")
    broken = {"readiness": {"checks": {"db": {"status": "error", "error": "locked"}}}}
    assert hermes_issues(broken) == [{"check": "db", "status": "error", "detail": "locked"}]
    assert hermes_issues({}) == [] and hermes_issues(None) == []


def test_view_running_from_recordings() -> None:
    out = system_out(
        Parts(
            checked_at=T0, hermes_ok=True, hermes_version="0.22.2", hermes_detail=DETAIL,
            hermes_unit=R["hermes.status/running"], llama_unit=R["llama.status/running"],
            llama_healthy=True, props=PROPS, slot_busy=False, gen_tps=21.4,
            active_runs=1, queued_runs=1, gpus=R["gpu.status/running"]["gpus"],
        )
    )  # fmt: skip
    assert protocol.check_event("system.updated", out) is None
    assert out["helper"] == {"ok": True, "error": None}
    assert out["hermes"] == {
        "status": "degraded", "version": "0.22.2", "since": "2026-10-20T22:31:58+00:00",
        "issues": [{"check": "disk", "status": "degraded", "detail": "96% used"}],
    }  # fmt: skip
    assert out["llama"] == {
        "status": "running", "intent": None, "since": "2026-10-20T23:03:54+00:00",
        "model": "Qwen3.8-27B", "quant": "IQ4_XS", "ctx": 65536, "slots": 1,
        "slots_busy": 0, "gen_tps": 21.4,
    }  # fmt: skip
    assert out["queue"] == {"active": 1, "queued": 1}
    assert [(g["name"], g["mem_used_mib"], g["mem_total_mib"]) for g in out["gpus"]] == [
        ("NVIDIA GeForce RTX 4070 SUPER", 11381, 12282),
        ("NVIDIA GeForce RTX 3050", 6693, 8192),
    ]
    assert out["gpus"][0]["power_w"] == 40.39


def test_view_stopped() -> None:
    out = system_out(
        Parts(
            checked_at=T0, hermes_ok=True, llama_unit=R["llama.status/stopped"],
            llama_healthy=None, known_slots=1, intent="stopped",
            gpus=R["gpu.status/stopped"]["gpus"],
        )
    )  # fmt: skip
    llama = out["llama"]
    assert (llama["status"], llama["intent"]) == ("stopped", "stopped")
    assert llama["since"] == "2026-10-20T22:34:28+00:00"  # InactiveEnterTimestamp
    assert (llama["model"], llama["ctx"], llama["slots"], llama["slots_busy"]) == (None, None, 1, 0)
    assert [g["mem_used_mib"] for g in out["gpus"]] == [1266, 15]


def test_view_without_helper() -> None:
    out = system_out(
        Parts(checked_at=T0, helper_errors=["helper_unavailable"], llama_healthy=True, props=PROPS)
    )
    assert out["helper"] == {"ok": False, "error": "helper_unavailable"}
    assert out["llama"]["status"] == "running" and out["llama"]["model"] == "Qwen3.8-27B"
    assert out["gpus"] is None and out["hermes"]["status"] == "unknown"


# --- helper client ------------------------------------------------------------------------


@pytest.fixture
def helper() -> Iterator[FakeHelper]:
    fake = FakeHelper().start()
    try:
        yield fake
    finally:
        fake.stop()


async def test_helper_client(helper: FakeHelper) -> None:
    client = HelperClient(helper.path)
    assert await client.call("llama.status") == R["llama.status/running"]
    assert (await client.call("gpu.status"))["gpus"][1]["name"] == "NVIDIA GeForce RTX 3050"
    helper.fail["gpu.status"] = ("failed", "nvidia-smi: No devices were found")
    with pytest.raises(HelperFailed) as exc:
        await client.call("gpu.status")
    assert (exc.value.code, exc.value.message) == ("failed", "nvidia-smi: No devices were found")
    with pytest.raises(HelperFailed, match="unknown_verb"):
        await client.call("reboot")
    assert helper.calls == ["llama.status", "gpu.status", "gpu.status", "reboot"]


async def test_helper_client_unreachable(helper: FakeHelper) -> None:
    with pytest.raises(HelperUnavailable):
        await HelperClient(helper.dir / "missing.sock").call("llama.status")
    helper.silent.add("llama.status")
    with pytest.raises(HelperUnavailable):
        await HelperClient(helper.path).call("llama.status", timeout_s=0.2)


async def test_helper_client_garbage() -> None:
    import tempfile
    from pathlib import Path

    path = Path(tempfile.mkdtemp(prefix="dh")) / "g.sock"

    async def reply(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        writer.write(b"not json\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(reply, str(path))
    async with server:
        with pytest.raises(HelperUnavailable, match="no answer"):
            await HelperClient(path).call("llama.status")


# --- queue hold (run manager) -----------------------------------------------------------

pg = pytest.mark.pg


@pg
async def test_hold_keeps_queued_runs_waiting(h: Harness) -> None:
    h.fake.enqueue("protocol/a")
    h.mgr.hold(True)
    await h.mgr.start()
    _, qid = await h.send("hello")
    await asyncio.sleep(0.3)  # a few idle polls
    assert (await h.run(qid)).status == "queued" and h.mgr.active_run_id is None
    assert h.mgr.held
    h.mgr.hold(False)
    await h.settle()
    async with h.db() as s:
        run = (await s.execute(select(Run))).scalar_one()
    assert run.status == "completed"


# --- the service on a live database -------------------------------------------------------


class Sys:
    def __init__(self, h: Harness, llama: FakeLlama, helper: FakeHelper, svc: SystemService):
        self.h, self.llama, self.helper, self.svc = h, llama, helper, svc

    def events(self, type_: str) -> list[dict[str, Any]]:
        return [e.data for e in self.h.events() if e.type == type_]


@pytest.fixture
async def sysenv(db: async_sessionmaker[AsyncSession]) -> AsyncIterator[Sys]:
    fl = FakeLlama()
    fl.set(task=1, processing=False, tokens=300)
    llama = LlamaClient("http://llama", transport=StreamingASGITransport(fl.app))
    helper = FakeHelper(fl).start()
    async with make_harness(db, llama=llama) as h:
        svc = SystemService(
            db, h.mgr, h.hub, h.hermes, llama, HelperClient(helper.path),
            start_wait_s=5.0, ready_poll_s=0.02, watch_s=0.05, cache_s=0.0,
        )  # fmt: skip
        h.mgr.hermes_ok, h.mgr.hermes_version = True, "0.22.2"
        try:
            yield Sys(h, fl, helper, svc)
        finally:
            await svc.close()
            helper.stop()
            await llama.aclose()


async def _intent_row(db: async_sessionmaker[AsyncSession]) -> dict[str, Any] | None:
    async with db() as s:
        row = await s.get(SystemState, INTENT_KEY)
    return dict(row.value) if row else None


async def _until(cond: Any) -> None:
    async with asyncio.timeout(5):
        while not await cond():  # noqa: ASYNC110 - polling state the test does not own
            await asyncio.sleep(0.01)


@pg
async def test_snapshot_running(sysenv: Sys) -> None:
    h = sysenv.h
    h.fake.enqueue("protocol/a")
    await h.mgr.start()
    await h.send("hi")
    await h.settle()
    async with h.db() as s:  # a completed run with a measured speed
        run = (await s.execute(select(Run))).scalar_one()
        run.perf = {**(run.perf or {}), "gen_tps": 21.43}
        await s.commit()
    snap = await sysenv.svc.snapshot()
    assert protocol.check_event("system.updated", snap) is None
    assert snap["helper"] == {"ok": True, "error": None}
    assert snap["hermes"]["status"] == "degraded" and snap["hermes"]["since"]
    llama = snap["llama"]
    assert (llama["status"], llama["model"], llama["quant"], llama["ctx"]) == (
        "running", "Qwen3.8-27B", "IQ4_XS", 65536,
    )  # fmt: skip
    assert (llama["slots"], llama["slots_busy"], llama["gen_tps"]) == (1, 0, 21.4)
    assert snap["queue"] == {"active": 0, "queued": 0} and len(snap["gpus"]) == 2
    assert sorted(sysenv.helper.calls) == ["gpu.status", "hermes.status", "llama.status"]


@pg
async def test_snapshot_cache(sysenv: Sys) -> None:
    sysenv.svc._cache_s = 60.0
    a = await sysenv.svc.snapshot()
    b = await sysenv.svc.snapshot()
    assert a is b and len(sysenv.helper.calls) == 3
    await sysenv.svc.snapshot(fresh=True)
    assert len(sysenv.helper.calls) == 6


@pg
async def test_snapshot_without_helper(sysenv: Sys) -> None:
    sysenv.helper.stop()
    snap = await sysenv.svc.snapshot()
    assert snap["helper"] == {"ok": False, "error": "helper_unavailable"}
    assert snap["llama"]["status"] == "running" and snap["gpus"] is None


@pg
async def test_snapshot_reports_gpu_failure(sysenv: Sys) -> None:
    sysenv.helper.fail["gpu.status"] = ("failed", "nvidia-smi: No devices were found")
    snap = await sysenv.svc.snapshot()
    assert snap["helper"]["error"] == "gpu.status: nvidia-smi: No devices were found"
    assert snap["gpus"] is None and snap["llama"]["status"] == "running"


@pg
async def test_stop_refused_while_a_run_is_active(sysenv: Sys) -> None:
    h = sysenv.h
    h.fake.enqueue("protocol/f", pause_before_seq=1)
    await h.mgr.start()
    await h.send("sleep 60")
    async with asyncio.timeout(5):
        while not (h.mgr.active_run_id or "").startswith("run_"):  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    with pytest.raises(SystemConflict) as exc:
        await sysenv.svc.stop_llama(force=False, by="pixel")
    assert exc.value.code == "run_active"
    assert not h.mgr.held and "llama.stop" not in sysenv.helper.calls
    assert await _intent_row(h.db) is None

    # force: the run is stopped first, then the server
    snap = await sysenv.svc.stop_llama(force=True, by="pixel")
    await h.settle()
    async with h.db() as s:
        run = (await s.execute(select(Run))).scalar_one()
    assert run.status == "cancelled"
    assert snap["llama"]["status"] == "stopped" and snap["llama"]["intent"] == "stopped"
    assert h.mgr.held
    row = await _intent_row(h.db)
    assert row is not None and row["state"] == "stopped" and row["by"] == "pixel"


@pg
async def test_stop_hold_start_release(sysenv: Sys) -> None:
    h, svc = sysenv.h, sysenv.svc
    await h.mgr.start()
    snap = await svc.stop_llama(force=False, by="pixel")
    assert snap["llama"]["status"] == "stopped" and snap["queue"] == {"active": 0, "queued": 0}
    assert sysenv.events("system.updated")[-1]["llama"]["status"] == "stopped"

    # a message sent while the model is down waits instead of failing
    h.fake.enqueue("protocol/a")
    _, qid = await h.send("hello")
    await asyncio.sleep(0.3)
    assert (await h.run(qid)).status == "queued"
    assert (await svc.snapshot(fresh=True))["queue"] == {"active": 0, "queued": 1}

    # start: still held (and stored) while the model loads
    snap = await svc.start_llama(by="pixel")
    assert snap["llama"]["status"] == "loading" and snap["llama"]["intent"] == "starting"
    await asyncio.sleep(0.2)
    assert h.mgr.held and (await h.run(qid)).status == "queued"
    assert await _intent_row(h.db) is not None

    sysenv.llama.health = "ok"  # model loaded
    async with asyncio.timeout(5):
        while h.mgr.held:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    await h.settle()
    async with h.db() as s:
        run = (await s.execute(select(Run))).scalar_one()
    assert run.status == "completed"
    assert await _intent_row(h.db) is None and svc.intent is None
    last = sysenv.events("system.updated")[-1]["llama"]
    assert (last["status"], last["intent"]) == ("running", None)
    assert sysenv.helper.calls.count("llama.stop") == 1
    assert sysenv.helper.calls.count("llama.start") == 1


@pg
async def test_start_gives_up_waiting(sysenv: Sys) -> None:
    svc = sysenv.svc
    svc._start_wait_s = 0.2
    await svc.stop_llama(force=False, by="pixel")
    await svc.start_llama(by="pixel")  # stays "loading"
    await _until(lambda: _not(sysenv.h.mgr.held))
    assert svc.intent is None and await _intent_row(sysenv.h.db) is None


async def _not(value: bool) -> bool:
    return not value


@pg
async def test_stop_while_starting_cancels_the_wait(sysenv: Sys) -> None:
    svc = sysenv.svc
    await svc.stop_llama(force=False, by="pixel")
    await svc.start_llama(by="pixel")
    snap = await svc.stop_llama(force=False, by="pixel")
    assert snap["llama"]["intent"] == "stopped" and sysenv.h.mgr.held
    sysenv.llama.health = "ok"
    await asyncio.sleep(0.2)  # the cancelled wait must not release the queue
    assert sysenv.h.mgr.held


@pg
async def test_helper_failure_restores_the_queue(sysenv: Sys) -> None:
    sysenv.helper.fail["llama.stop"] = ("failed", "Failed to connect to bus: No medium found")
    with pytest.raises(HelperFailed):
        await sysenv.svc.stop_llama(force=False, by="pixel")
    assert not sysenv.h.mgr.held and await _intent_row(sysenv.h.db) is None
    sysenv.helper.stop()
    with pytest.raises(HelperUnavailable):
        await sysenv.svc.start_llama(by="pixel")
    assert sysenv.svc.intent is None


@pg
async def test_intent_survives_a_restart_and_a_manual_start_releases(sysenv: Sys) -> None:
    h = sysenv.h
    await sysenv.svc.stop_llama(force=False, by="pixel")
    await sysenv.svc.close()

    # a new process: the stored intent holds the queue before the worker starts
    h.mgr.hold(False)
    fresh = SystemService(
        h.db, h.mgr, h.hub, h.hermes, sysenv.svc._llama, HelperClient(sysenv.helper.path),
        watch_s=0.05, cache_s=0.0,
    )  # fmt: skip
    sysenv.svc = fresh
    await fresh.start()
    assert h.mgr.held and fresh.intent == "stopped"
    await asyncio.sleep(0.2)
    assert h.mgr.held  # still down: the watch keeps holding

    sysenv.llama.health = "ok"  # started from the terminal
    await _until(lambda: _not(h.mgr.held))
    assert fresh.intent is None and await _intent_row(h.db) is None
    seen: list[dict[str, Any]] = []

    async def released_published() -> bool:  # the event follows the release
        seen.extend(sysenv.events("system.updated"))
        return bool(seen) and seen[-1]["llama"]["intent"] is None

    await _until(released_published)


@pg
async def test_slot_count_survives_a_restart_and_slot_errors_are_logged_once(
    sysenv: Sys, caplog: pytest.LogCaptureFixture
) -> None:
    h = sysenv.h
    sysenv.llama.down = True  # /slots answers 503, /health and /props still fine
    with caplog.at_level("WARNING", logger="delve.system"):
        for _ in range(3):
            snap = await sysenv.svc.snapshot(fresh=True)
    assert (snap["llama"]["slots"], snap["llama"]["slots_busy"]) == (1, None)
    assert [r.message for r in caplog.records].count("llama /slots not readable") == 1
    async with h.db() as s:
        row = await s.get(SystemState, SLOTS_KEY)
    assert row is not None and row.value == {"slots": 1}

    # a new process with llama-server stopped still knows the slot count
    await sysenv.svc.stop_llama(force=False, by="pixel")
    await sysenv.svc.close()
    fresh = SystemService(
        h.db, h.mgr, h.hub, h.hermes, sysenv.svc._llama, HelperClient(sysenv.helper.path),
        watch_s=0.05, cache_s=0.0,
    )  # fmt: skip
    sysenv.svc = fresh
    await fresh.start()
    llama = (await fresh.snapshot(fresh=True))["llama"]
    assert (llama["status"], llama["slots"], llama["slots_busy"]) == ("stopped", 1, 0)


# --- REST ---------------------------------------------------------------------------------


@pytest.fixture
def sys_client(
    env: pytest.MonkeyPatch,
    migrated_url: str,
    db: async_sessionmaker[AsyncSession],
) -> Iterator[tuple[TestClient, FakeHelper, FakeLlama]]:
    from .fake_hermes import FakeHermes

    fl = FakeLlama()
    fl.set(task=1, processing=False, tokens=300)
    helper = FakeHelper(fl).start()
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("HERMES_API_KEY", "fake-key")
    env.setenv("DELVE_HELPER_SOCKET", str(helper.path))
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(FakeHermes().app),
        llama_transport=StreamingASGITransport(fl.app),
        timing=FAST,
        system_options={"ready_poll_s": 0.02, "watch_s": 0.05, "cache_s": 0.0},
    )
    try:
        with TestClient(app) as c:
            yield c, helper, fl
    finally:
        helper.stop()


@pg
def test_api_system(sys_client: tuple[TestClient, FakeHelper, FakeLlama], token: str) -> None:
    c, helper, fl = sys_client
    assert c.get("/api/v1/system").status_code == 401
    r = c.get("/api/v1/system", headers=auth(token))
    assert r.status_code == 200
    body = r.json()
    assert protocol.check_event("system.updated", body) is None
    assert body["llama"]["model"] == "Qwen3.8-27B"

    r = c.post("/api/v1/system/llama", json={"action": "stop"}, headers=auth(token))
    assert r.status_code == 200, r.text
    assert r.json()["llama"]["status"] == "stopped"

    fl.health = "loading"
    helper.start_to = "loading"
    r = c.post("/api/v1/system/llama", json={"action": "start"}, headers=auth(token))
    assert r.status_code == 200 and r.json()["llama"]["intent"] == "starting"
    fl.health = "ok"


@pg
@pytest.mark.parametrize(
    "body",
    [{}, {"action": "restart"}, {"action": "stop", "force": "yes"}, {"action": "stop", "x": 1}],
)
def test_api_llama_validation(
    sys_client: tuple[TestClient, FakeHelper, FakeLlama], token: str, body: dict[str, Any]
) -> None:
    c, helper, _ = sys_client
    assert c.post("/api/v1/system/llama", json=body, headers=auth(token)).status_code == 422
    assert "llama.stop" not in helper.calls


@pg
def test_api_llama_errors(sys_client: tuple[TestClient, FakeHelper, FakeLlama], token: str) -> None:
    c, helper, _ = sys_client
    helper.fail["llama.stop"] = ("failed", "Failed to connect to bus: No medium found")
    r = c.post("/api/v1/system/llama", json={"action": "stop"}, headers=auth(token))
    assert r.status_code == 502
    assert r.json()["detail"] == {
        "code": "helper_failed", "message": "Failed to connect to bus: No medium found"
    }  # fmt: skip
    helper.stop()
    r = c.post("/api/v1/system/llama", json={"action": "stop"}, headers=auth(token))
    assert r.status_code == 503 and r.json()["detail"]["code"] == "helper_unavailable"
    body = c.get("/api/v1/system", headers=auth(token)).json()
    assert body["helper"]["ok"] is False


# --- CLI ----------------------------------------------------------------------------------


def test_cli_helper(
    helper: FakeHelper, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DELVE_HELPER_SOCKET", str(helper.path))
    assert cli.main(["helper", "gpu.status"]) == 0
    assert json.loads(capsys.readouterr().out)["gpus"][0]["mem_total_mib"] == 12282
    assert cli.main(["helper", "host.status"]) == 0
    assert json.loads(capsys.readouterr().out)["uptime_s"] == 135358
    assert cli.main(["helper", "agent.incidents"]) == 0
    assert json.loads(capsys.readouterr().out)["incidents"][0]["state"] == "closed"
    helper.fail["llama.status"] = ("unit_not_found", "the unit does not exist for that user")
    assert cli.main(["helper", "llama.status"]) == 1
    assert "unit_not_found" in capsys.readouterr().err
    monkeypatch.setenv("DELVE_HELPER_SOCKET", str(helper.dir / "missing.sock"))
    assert cli.main(["helper", "llama.status"]) == 2
    assert "not reachable" in capsys.readouterr().err


@pg
async def test_intent_read_later_when_the_database_was_not_up(sysenv: Sys) -> None:
    h = sysenv.h
    await sysenv.svc.stop_llama(force=False, by="pixel")
    await sysenv.svc.close()
    h.mgr.hold(False)

    calls = 0

    def flaky() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionRefusedError(111, "Connection refused")
        return h.db()

    fresh = SystemService(
        flaky,  # type: ignore[arg-type]
        h.mgr, h.hub, h.hermes, sysenv.svc._llama, HelperClient(sysenv.helper.path),
        watch_s=0.05, cache_s=0.0,
    )  # fmt: skip
    sysenv.svc = fresh
    await fresh.start()  # does not raise
    assert not h.mgr.held
    await _until(lambda: _is(h.mgr.held))
    assert fresh.intent == "stopped"


async def _is(value: bool) -> bool:
    return value
