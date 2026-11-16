"""System view and llama-server control (B3): helper client, view, queue hold, API, CLI."""

from __future__ import annotations

import asyncio
import json
import time
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
from delve.runs.manager import ActiveModel
from delve.system import (
    INTENT_KEY,
    SLOTS_KEY,
    SWITCH_KEY,
    HelperClient,
    HelperFailed,
    HelperUnavailable,
    Parts,
    SystemConflict,
    SystemService,
    hermes_issues,
    hermes_state,
    llama_state,
    loaded_label,
    model_out,
    model_text,
    switch_out,
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
    assert Props(
        model="Qwen3.8-27B", quant="IQ4_XS", n_ctx=65536, slots=1,
        file="Qwen3.8-27B-UD-IQ4_XS.gguf", vision=True,
    ) == PROPS  # fmt: skip
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
        # without the helper's preset listing: only what /props says
        "preset": None, "vision": True, "thinking": None, "models": None, "switch": None,
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
    assert sorted(sysenv.helper.calls) == [
        "gpu.status", "hermes.status", "llama.models", "llama.status",
    ]  # fmt: skip


@pg
async def test_snapshot_cache(sysenv: Sys) -> None:
    sysenv.svc._cache_s = 60.0
    a = await sysenv.svc.snapshot()
    b = await sysenv.svc.snapshot()
    assert a is b and len(sysenv.helper.calls) == 4
    await sysenv.svc.snapshot(fresh=True)  # the preset listing is kept for half a minute
    assert len(sysenv.helper.calls) == 7


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


# --- model presets and switching (M) ------------------------------------------------------

PRESETS = R["llama.models/presets"]
MAIN = "qwen38-27b-iq4xs"


def test_models_from_the_recorded_listing() -> None:
    rows = {m["name"]: m for m in (model_out(m, PRESETS["active"]) for m in PRESETS["models"])}
    assert list(rows) == [MAIN, "test-64k", "test-crash", "test-missing", "test-text"]
    assert rows[MAIN] == {
        "name": MAIN, "model": "Qwen3.8-27B", "quant": "IQ4_XS", "note": None, "ctx": 98304,
        "vision": True, "thinking": "toggle", "size_bytes": 14252845984, "usable": True,
        "reason": None, "active": True,
    }  # fmt: skip
    # a label replaces the name from the file, the quantization stays
    assert (rows["test-64k"]["model"], rows["test-64k"]["quant"]) == ("Qwen3.8-27B 64K", "IQ4_XS")
    assert rows["test-64k"]["note"] == "test: same model, 64K context"
    assert (rows["test-text"]["vision"], rows["test-text"]["thinking"]) == (False, "none")
    missing = rows["test-missing"]
    assert (missing["usable"], missing["reason"], missing["size_bytes"]) == (
        False, "model_missing", None,
    )  # fmt: skip
    assert not any(m["active"] for n, m in rows.items() if n != MAIN)
    assert model_out({"name": "x", "thinking": "sometimes"}, None)["thinking"] == "toggle"
    assert model_out({"name": "x"}, None)["model"] == "x"
    assert model_text("Qwen3.8-27B", "IQ4_XS") == "Qwen3.8-27B · IQ4_XS"
    assert model_text("mystery", None) == "mystery"


def test_view_with_presets() -> None:
    def view(active: str, **kw: Any) -> dict[str, Any]:
        parts = Parts(checked_at=T0, presets={**PRESETS, "active": active}, **kw)
        return system_out(parts)["llama"]

    running = {"llama_unit": R["llama.status/running"], "llama_healthy": True, "props": PROPS}
    llama = view(MAIN, **running)
    assert (llama["preset"], llama["model"], llama["quant"]) == (MAIN, "Qwen3.8-27B", "IQ4_XS")
    assert (llama["vision"], llama["thinking"], llama["switch"]) == (True, "toggle", None)
    assert [m["name"] for m in llama["models"] if m["active"]] == [MAIN]
    # the loaded file is the selected preset's file: its label is the name shown
    assert view("test-64k", **running)["model"] == "Qwen3.8-27B 64K"
    # the pointer names another file than the one loaded: the file name speaks
    other = {**PRESETS, "active": "test-crash"}
    assert loaded_label(PROPS, other) == ("Qwen3.8-27B", "IQ4_XS")
    # stopped: what Start will load
    stopped = view("test-text", llama_unit=R["llama.status/stopped"], intent="stopped")
    assert (stopped["status"], stopped["preset"], stopped["model"]) == (
        "stopped", "test-text", None,
    )  # fmt: skip
    assert (stopped["vision"], stopped["thinking"]) == (False, "none")
    sw = {"state": "failed", "from": MAIN, "to": "test-crash", "by": "pixel",
          "at": T0.isoformat(), "error": "exited", "elapsed_s": 31}  # fmt: skip
    assert view(MAIN, switch=sw, **running)["switch"] == sw
    # the unit in systemd's restart loop reads as starting
    assert llama_state(R["llama.status/auto-restart"], None) == "starting"


async def _switched(sysenv: Sys, state: str | None) -> None:
    async with asyncio.timeout(5):
        while (sysenv.svc.switch or {}).get("state") != state or sysenv.svc._starting:  # noqa: ASYNC110
            await asyncio.sleep(0.01)


async def _switch_row(db: async_sessionmaker[AsyncSession]) -> dict[str, Any] | None:
    async with db() as s:
        row = await s.get(SystemState, SWITCH_KEY)
    return dict(row.value) if row else None


@pg
async def test_snapshot_lists_the_models(sysenv: Sys) -> None:
    llama = (await sysenv.svc.snapshot())["llama"]
    assert llama["preset"] == MAIN and len(llama["models"]) == 5
    assert (llama["vision"], llama["thinking"]) == (True, "toggle")
    am = await sysenv.svc.active_model()
    assert am == ActiveModel("Qwen3.8-27B · IQ4_XS", True, "toggle")

    # an older helper without the verb: no list, and no error on the screen
    sysenv.helper.models_verb = False
    sysenv.svc._presets = None
    snap = await sysenv.svc.snapshot(fresh=True)
    assert snap["llama"]["models"] is None and snap["llama"]["preset"] is None
    assert snap["helper"] == {"ok": True, "error": None}
    assert snap["llama"]["model"] == "Qwen3.8-27B" and snap["llama"]["vision"] is True
    assert (await sysenv.svc.active_model()).label == "Qwen3.8-27B · IQ4_XS"


@pg
async def test_switch_loads_another_preset(sysenv: Sys) -> None:
    h, svc, helper = sysenv.h, sysenv.svc, sysenv.helper
    h.mgr.active_model = svc.active_model
    await h.mgr.start()
    snap = await svc.switch_llama("test-64k", force=False, by="pixel")
    assert helper.calls[:5] == [
        "llama.models", "llama.status", "llama.stop", "llama.select test-64k", "llama.start",
    ]  # fmt: skip
    # then the snapshot's reads, in any order (the listing is read again: it changed)
    assert sorted(helper.calls[5:]) == [
        "gpu.status", "hermes.status", "llama.models", "llama.status",
    ]  # fmt: skip
    sw = snap["llama"]["switch"]
    assert (sw["state"], sw["from"], sw["to"], sw["by"]) == ("switching", MAIN, "test-64k", "pixel")
    assert (snap["llama"]["status"], snap["llama"]["intent"]) == ("loading", "starting")
    assert snap["llama"]["preset"] == "test-64k" and h.mgr.held
    assert (await _switch_row(h.db) or {}).get("state") == "switching"

    # a message sent meanwhile waits for the new model
    h.fake.enqueue("protocol/a")
    _, qid = await h.send("hello")
    await asyncio.sleep(0.2)
    assert (await h.run(qid)).status == "queued"
    with pytest.raises(SystemConflict) as exc:
        await svc.switch_llama(MAIN, force=False, by="pixel")
    assert exc.value.code == "switch_in_progress"

    sysenv.llama.health = "ok"  # loaded
    await _switched(sysenv, None)
    await h.settle()
    assert not h.mgr.held and svc.intent is None and await _switch_row(h.db) is None
    last = sysenv.events("system.updated")[-1]["llama"]
    assert (last["status"], last["model"], last["ctx"], last["switch"]) == (
        "running", "Qwen3.8-27B 64K", 65536, None,
    )  # fmt: skip
    async with h.db() as s:
        run = (await s.execute(select(Run))).scalar_one()
    assert (run.status, run.model) == ("completed", "Qwen3.8-27B 64K · IQ4_XS")


@pg
async def test_switch_refusals(sysenv: Sys) -> None:
    h, svc, helper = sysenv.h, sysenv.svc, sysenv.helper

    async def code(model: str, *, force: bool = False) -> str:
        with pytest.raises(SystemConflict) as exc:
            await svc.switch_llama(model, force=force, by="pixel")
        return exc.value.code

    assert await code("nope") == "unknown_model"
    assert await code("test-missing") == "model_not_usable"
    assert await code(MAIN) == "already_active"
    assert not any(
        c.startswith(("llama.stop", "llama.select", "llama.start")) for c in helper.calls
    )

    h.fake.enqueue("protocol/f", pause_before_seq=1)
    await h.mgr.start()
    await h.send("sleep 60")
    async with asyncio.timeout(5):
        while not (h.mgr.active_run_id or "").startswith("run_"):  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    assert await code("test-64k") == "run_active"
    assert not h.mgr.held and "llama.stop" not in helper.calls and svc.switch is None

    # force: the run is stopped first
    helper.start_to = "ok"
    await svc.switch_llama("test-64k", force=True, by="pixel")
    await _switched(sysenv, None)
    await h.settle()
    async with h.db() as s:
        run = (await s.execute(select(Run))).scalar_one()
    assert run.status == "cancelled" and helper.active == "test-64k"


@pg
async def test_failed_switch_goes_back(sysenv: Sys) -> None:
    h, svc, helper = sysenv.h, sysenv.svc, sysenv.helper
    pushes: list[tuple[str, dict[str, Any]]] = []
    svc._notify_switch = lambda state, **kw: pushes.append((state, kw))
    helper.start_to = "ok"  # the previous model loads at once when it is started again
    await h.mgr.start()
    await svc.switch_llama("test-crash", force=False, by="pixel")
    await _switched(sysenv, "failed")
    assert helper.active == MAIN and not h.mgr.held and svc.intent is None
    moves = [
        c for c in helper.calls if c.split()[0] in ("llama.stop", "llama.select", "llama.start")
    ]
    assert moves == [
        "llama.stop", "llama.select test-crash", "llama.start",
        "llama.stop", f"llama.select {MAIN}", "llama.start",
    ]  # fmt: skip
    sw = svc.switch or {}
    assert (sw["state"], sw["from"], sw["to"], sw["error"]) == (
        "failed", MAIN, "test-crash", "exited",
    )  # fmt: skip
    assert isinstance(sw["elapsed_s"], int) and await _switch_row(h.db) == sw
    assert pushes == [
        ("failed", {"to": "Broken on purpose", "back": "Qwen3.8-27B · IQ4_XS", "at": sw["at"]})
    ]
    last = sysenv.events("system.updated")[-1]["llama"]
    assert (last["status"], last["preset"], last["switch"]["state"]) == ("running", MAIN, "failed")

    # it stays until it is dismissed, also across a backend restart
    fresh = SystemService(h.db, h.mgr, h.hub, h.hermes, svc._llama, svc._helper)
    await fresh._load_intent()
    assert fresh.switch == sw
    snap = await svc.dismiss_switch()
    assert snap["llama"]["switch"] is None and await _switch_row(h.db) is None


@pg
async def test_one_failed_start_is_not_a_failed_switch(sysenv: Sys) -> None:
    """A start can fail for lack of free GPU memory and succeed on systemd's retry."""
    svc, helper = sysenv.svc, sysenv.helper
    helper.crash.add("test-64k")
    await svc.switch_llama("test-64k", force=False, by="pixel")
    async with asyncio.timeout(5):
        while helper.restarts < 2:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    helper.crash.discard("test-64k")  # the next retry loads
    helper.state, sysenv.llama.health = "running", "ok"
    await _switched(sysenv, None)
    assert helper.active == "test-64k" and svc.switch is None and not sysenv.h.mgr.held


@pg
async def test_failed_switch_with_no_way_back(sysenv: Sys) -> None:
    h, svc, helper = sysenv.h, sysenv.svc, sysenv.helper
    pushes: list[str] = []
    svc._notify_switch = lambda state, **kw: pushes.append(state)
    helper.crash.add(MAIN)
    await h.mgr.start()
    await svc.switch_llama("test-crash", force=False, by="pixel")
    await _switched(sysenv, "down")
    assert pushes == ["down"] and helper.active == MAIN
    # like a stop on purpose: the queue waits
    assert h.mgr.held and svc.intent == "stopped"
    assert (await _intent_row(h.db) or {}).get("state") == "stopped"
    assert (await svc.dismiss_switch())["llama"]["switch"]["state"] == "down"

    # the operator picks a preset that works and starts it
    helper.crash.clear()
    helper.state = "stopped"
    await svc.switch_llama("test-64k", force=False, by="pixel")
    assert helper.active == "test-64k" and "llama.start" not in helper.calls[-4:]
    helper.start_to = "ok"
    await svc.start_llama(by="pixel")
    async with asyncio.timeout(5):
        while h.mgr.held or svc._starting:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    assert svc.switch is None and await _switch_row(h.db) is None and svc.intent is None


@pg
async def test_select_while_stopped_only_moves_the_pointer(sysenv: Sys) -> None:
    svc, helper = sysenv.svc, sysenv.helper
    await svc.stop_llama(force=False, by="pixel")
    n = len(helper.calls)
    snap = await svc.switch_llama("test-text", force=False, by="pixel")
    assert [c for c in helper.calls[n:] if c.startswith("llama.s")] == [
        "llama.status", "llama.select test-text", "llama.status",
    ]  # fmt: skip
    llama = snap["llama"]
    assert (llama["status"], llama["preset"], llama["switch"]) == ("stopped", "test-text", None)
    assert (llama["vision"], llama["thinking"], llama["model"]) == (False, "none", None)
    am = await svc.active_model()  # nothing loaded: the selected preset answers
    assert am == ActiveModel(None, False, "none")
    # selecting it again is fine while stopped
    await svc.switch_llama("test-text", force=False, by="pixel")

    helper.start_to = "ok"
    await svc.start_llama(by="pixel")
    async with asyncio.timeout(5):
        while sysenv.h.mgr.held:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    llama = (await svc.snapshot(fresh=True))["llama"]
    assert (llama["status"], llama["model"], llama["vision"]) == (
        "running", "Qwen3.8-27B text", False,
    )  # fmt: skip
    assert await svc.active_model() == ActiveModel("Qwen3.8-27B text · IQ4_XS", False, "none")


@pg
async def test_stop_during_a_switch_ends_it(sysenv: Sys) -> None:
    svc = sysenv.svc
    await svc.switch_llama("test-64k", force=False, by="pixel")
    assert (svc.switch or {}).get("state") == "switching"
    snap = await svc.stop_llama(force=False, by="pixel")
    assert snap["llama"]["switch"] is None and svc.switch is None and svc._starting is None
    assert (snap["llama"]["status"], snap["llama"]["preset"]) == ("stopped", "test-64k")
    assert await _switch_row(sysenv.h.db) is None and sysenv.h.mgr.held


@pg
async def test_a_switch_cut_short_by_a_restart_is_dropped(sysenv: Sys) -> None:
    svc = sysenv.svc
    await svc._set_switch(
        switch_out("switching", src=MAIN, to="test-64k", by="pixel", at=T0.isoformat())
    )
    fresh = SystemService(sysenv.h.db, sysenv.h.mgr, sysenv.h.hub, sysenv.h.hermes,
                          svc._llama, svc._helper)  # fmt: skip
    await fresh._load_intent()
    assert fresh.switch is None and await _switch_row(sysenv.h.db) is None


@pg
async def test_a_half_done_switch_starts_what_is_selected(sysenv: Sys) -> None:
    h, svc, helper = sysenv.h, sysenv.svc, sysenv.helper
    helper.fail["llama.select"] = ("failed", "ln: failed to create symbolic link")
    helper.start_to = "ok"
    with pytest.raises(HelperFailed):
        await svc.switch_llama("test-64k", force=False, by="pixel")
    async with asyncio.timeout(5):
        while h.mgr.held or svc._starting:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    assert helper.active == MAIN and helper.state == "running" and svc.switch is None


@pg
def test_api_switch(sys_client: tuple[TestClient, FakeHelper, FakeLlama], token: str) -> None:
    c, helper, _ = sys_client

    def post(body: dict[str, Any]) -> Any:
        return c.post("/api/v1/system/llama", json=body, headers=auth(token))

    for bad in ({"action": "switch"}, {"action": "stop", "model": MAIN},
                {"action": "switch", "model": "../x"}, {"action": "switch", "model": "A"},
                {"action": "dismiss", "model": MAIN}):  # fmt: skip
        assert post(bad).status_code == 422, bad
    r = post({"action": "switch", "model": "nope"})
    assert r.status_code == 404 and r.json()["detail"]["code"] == "unknown_model"
    r = post({"action": "switch", "model": "test-missing"})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "model_not_usable"
    assert post({"action": "switch", "model": MAIN}).json()["detail"]["code"] == "already_active"

    helper.start_to = "ok"
    r = post({"action": "switch", "model": "test-64k"})
    assert r.status_code == 200, r.text
    assert r.json()["llama"]["switch"]["to"] == "test-64k"
    body: dict[str, Any] = {}
    for _ in range(200):
        body = c.get("/api/v1/system", headers=auth(token)).json()
        if body["llama"]["switch"] is None and body["llama"]["status"] == "running":
            break
        time.sleep(0.02)
    assert (body["llama"]["preset"], body["llama"]["ctx"]) == ("test-64k", 65536)
    assert protocol.check_event("system.updated", body) is None
    assert post({"action": "dismiss"}).status_code == 200
