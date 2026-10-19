"""Tool steps and approvals end to end: fake Hermes replaying recorded runs, live database."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from sqlalchemy import select

from delve.db.models import Approval, ToolStep
from delve.runs.approvals import ApprovalError, ApprovalService

from .fake_hermes import load_json
from .harness import Harness, sse_events

pytestmark = pytest.mark.pg


async def steps_of(h: Harness, run_id: str) -> list[ToolStep]:
    async with h.db() as s:
        q = select(ToolStep).where(ToolStep.run_id == run_id).order_by(ToolStep.id)
        return list((await s.execute(q)).scalars())


async def approval(h: Harness, request_id: str) -> Approval | None:
    async with h.db() as s:
        return await s.get(Approval, request_id)


def request_id_of(name: str) -> str:
    return next(e for e in sse_events(name) if e["event"] == "approval.request")["request_id"]  # type: ignore[no-any-return]


def run_id_of(name: str) -> str:
    return load_json(f"{name}.create.json")["run_id"]  # type: ignore[no-any-return]


async def wait_for_pending(h: Harness, request_id: str) -> None:
    async with asyncio.timeout(5):
        while True:
            a = await approval(h, request_id)
            if a is not None and a.status == "pending":
                return
            await asyncio.sleep(0.01)


async def test_tool_step_recorded(h: Harness) -> None:
    h.fake.enqueue("protocol/a")
    await h.send()
    await h.mgr.start()
    await h.settle()
    (step,) = await steps_of(h, run_id_of("protocol/a"))
    assert (step.tool, step.status, step.preview_start) == ("terminal", "ok", "uptime")
    assert step.result and step.result["exit_code"] == 0 and step.result_raw is None
    assert (step.seq_started, step.seq_completed) == (0, 1)
    assert step.duration_s == pytest.approx(0.22)
    evs = h.events()
    started = next(e.data for e in evs if e.type == "tool.started")
    done = next(e.data for e in evs if e.type == "tool.completed")
    assert started["step"]["status"] == "running" and started["step"]["id"] == step.id
    assert done["step"]["status"] == "ok" and done["step"]["result"]["exit_code"] == 0


async def test_denied_through_hermes(h: Harness) -> None:
    h.fake.enqueue("protocol/b")
    conv_id, _ = await h.send("rm -r /tmp/x")
    await h.mgr.start()
    await h.settle()
    rid, run_id = request_id_of("protocol/b"), run_id_of("protocol/b")
    a = await approval(h, rid)
    assert a is not None
    assert (a.status, a.choice, a.decided_by) == ("denied", "deny", None)
    assert (a.run_id, a.conversation_id, a.command) == (run_id, conv_id, "rm -r /tmp/x")
    assert a.action_class == "delete_kill" and a.pattern_keys == ["delete in root path"]
    assert a.hermes_deadline - a.created_at == timedelta(seconds=300)
    assert a.expires_at - a.created_at == timedelta(minutes=30)
    assert a.responded_at is not None
    (step,) = await steps_of(h, run_id)
    assert step.status == "denied" and step.approval_request_id == rid
    assert step.user_summary == "You denied this command — it did not run."

    evs = h.events()
    req = next(e.data for e in evs if e.type == "approval.request")
    assert req["choices"] == ["once", "session", "deny"]  # no "always"
    assert req["step_id"] == step.id and req["status"] == "pending"
    upd = [e.data["status"] for e in evs if e.type == "approval.updated"]
    assert upd == ["denied"]
    assert [e.data["status"] for e in evs if e.type == "run.updated"] == [
        "waiting_for_approval",
        "running",
    ]


async def test_hermes_timeout_expires_approval(h: Harness) -> None:
    h.fake.enqueue("protocol/d")
    await h.send()
    await h.mgr.start()
    await h.settle()
    a = await approval(h, request_id_of("protocol/d"))
    assert a is not None and a.status == "expired_in_hermes" and a.choice is None
    (step,) = await steps_of(h, run_id_of("protocol/d"))
    assert step.status == "timeout" and step.user_summary
    upd = [e.data["status"] for e in h.events() if e.type == "approval.updated"]
    assert upd == ["expired_in_hermes"]


async def test_deny_rule_needs_no_approval(h: Harness) -> None:
    h.fake.enqueue("runs/t1")
    await h.send()
    await h.mgr.start()
    await h.settle()
    (step,) = await steps_of(h, run_id_of("runs/t1"))
    assert step.status == "blocked" and step.approval_request_id is None
    async with h.db() as s:
        assert (await s.execute(select(Approval))).first() is None


async def test_answer_once_from_the_app(h: Harness) -> None:
    rid, run_id = request_id_of("protocol/c"), run_id_of("protocol/c")
    h.fake.enqueue(
        "protocol/c",
        pause_before_seq=2,  # Hermes waits for the answer after approval.request
        approvals={rid: load_json(f"protocol/approval-once-{run_id}.json")},
    )
    await h.send("rm -r /tmp/x")
    await h.mgr.start()
    await wait_for_pending(h, rid)
    svc = ApprovalService(h.db, h.hermes, h.hub)
    out = await svc.respond(rid, "once", "pixel")
    assert out["status"] == "approved_once" and out["decided_by"] == "pixel"
    await h.settle()

    post = h.fake.calls("POST", f"/v1/runs/{run_id}/approval")
    assert [p.body for p in post] == [{"request_id": rid, "choice": "once"}]
    a = await approval(h, rid)
    assert a is not None
    assert (a.status, a.choice, a.decided_by) == ("approved_once", "once", "pixel")
    (step,) = await steps_of(h, run_id)
    assert step.status == "ok" and step.result and "approved by the user" in step.result["approval"]

    with pytest.raises(ApprovalError) as exc:
        await svc.respond(rid, "deny", "pixel")
    assert exc.value.code == "approval_already_decided"


async def test_refused_choices_never_reach_hermes(h: Harness) -> None:
    svc = ApprovalService(h.db, h.hermes, h.hub)
    for choice in ("always", "ALWAYS", ""):
        with pytest.raises(ApprovalError) as exc:
            await svc.respond("whatever", choice, "pixel")
        assert exc.value.code == "invalid_choice" and exc.value.http_status == 400
    with pytest.raises(ApprovalError) as exc:
        await svc.respond("nope", "once", "pixel")
    assert exc.value.http_status == 404
    assert h.fake.requests == []


async def test_answer_after_hermes_gave_up(h: Harness) -> None:
    rid, run_id = request_id_of("protocol/d"), run_id_of("protocol/d")
    h.fake.enqueue(
        "protocol/d",
        pause_before_seq=2,
        approvals={rid: load_json(f"protocol/approval-once-{run_id}.json")},  # not_active
    )
    await h.send()
    await h.mgr.start()
    await wait_for_pending(h, rid)
    svc = ApprovalService(h.db, h.hermes, h.hub)
    with pytest.raises(ApprovalError) as exc:
        await svc.respond(rid, "once", "pixel")
    assert exc.value.code == "approval_expired_in_hermes"
    a = await approval(h, rid)
    assert a is not None and a.status == "expired_in_hermes"

    h.fake.runs[run_id].release.set()
    await h.settle()
    a = await approval(h, rid)
    assert a is not None and a.status == "expired_in_hermes" and a.decided_by is None
    with pytest.raises(ApprovalError) as exc:
        await svc.respond(rid, "once", "pixel")
    assert exc.value.code == "approval_expired_in_hermes"  # late approval: plan 12/5


async def test_stop_while_waiting_cancels_approval(h: Harness) -> None:
    run_id, rid = run_id_of("protocol/b"), request_id_of("protocol/b")
    events = sse_events("protocol/b")[:2]
    cancelled = {"event": "run.cancelled", "run_id": run_id, "timestamp": 0.0,
                 "completed": False, "interrupted": True,
                 "turn_exit_reason": "interrupted_by_user", "seq": 2}  # fmt: skip

    text = ": open\n\n" + "".join(
        f"id: {e['seq']}\ndata: {json.dumps(e)}\n\n" for e in [*events, cancelled]
    )
    sc = h.fake.enqueue("protocol/b", pause_before_seq=2)
    sc.text = text
    _, qid = await h.send()
    await h.mgr.start()
    await wait_for_pending(h, rid)
    assert await h.mgr.stop(qid) == "stopping"
    await h.settle()
    a = await approval(h, rid)
    assert a is not None and a.status == "cancelled"
    (step,) = await steps_of(h, run_id)
    assert step.status == "cancelled"
    evs = h.events()
    assert [e.data["status"] for e in evs if e.type == "approval.updated"] == ["cancelled"]
    assert next(e.data for e in evs if e.type == "run.finished")["status"] == "cancelled"
