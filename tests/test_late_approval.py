"""Late approval (plan 6.5 step 4), expiry and reconciliation."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from delve.db.models import Approval, Conversation, Message, Run
from delve.runs.approvals import ApprovalError, ApprovalService
from delve.runs.late import AUTO_DECIDED_BY, late_prompt
from delve.runs.steps import command_hash

from .fake_hermes import load_json
from .harness import Harness, sse_events

pytestmark = pytest.mark.pg

CMD = "rm -r /tmp/x"  # the command in the d (timeout), b and c recordings


def rid(name: str) -> str:
    return next(e for e in sse_events(name) if e["event"] == "approval.request")["request_id"]  # type: ignore[no-any-return]


def run_id(name: str) -> str:
    return load_json(f"{name}.create.json")["run_id"]  # type: ignore[no-any-return]


def svc(h: Harness) -> ApprovalService:
    return ApprovalService(h.db, h.hermes, h.hub, h.mgr)


async def get(h: Harness, request_id: str) -> Approval:
    async with h.db() as s:
        a = await s.get(Approval, request_id)
    assert a is not None
    return a


async def seed_expired(
    h: Harness, *, command: str = CMD, expires_in: timedelta = timedelta(minutes=20)
) -> str:
    """A conversation whose run ended while its approval was unanswered."""
    now = datetime.now(UTC)
    request_id = uuid.uuid4().hex
    async with h.db() as s:
        s.add(Conversation(id="c_late"))
        await s.flush()
        s.add_all(
            [
                Run(run_id="run_old", conversation_id="c_late", idempotency_key=uuid.uuid4(),
                    status="completed", input="q"),
                Approval(request_id=request_id, run_id="run_old", conversation_id="c_late",
                         command=command, command_sha256=command_hash(command),
                         action_class="delete_kill", status="expired_in_hermes",
                         created_at=now - timedelta(minutes=10),
                         hermes_deadline=now - timedelta(minutes=5),
                         expires_at=now + expires_in),
            ]
        )  # fmt: skip
        await s.commit()
    return request_id


def approval_statuses(h: Harness, request_id: str) -> list[str]:
    return [
        e.data["status"]
        for e in h.events()
        if e.type == "approval.updated" and e.data["request_id"] == request_id
    ]


async def test_late_once_runs_followup_and_auto_approves(h: Harness) -> None:
    h.fake.enqueue("protocol/d")  # Hermes times the approval out
    conv_id, _ = await h.send(CMD)
    await h.mgr.start()
    await h.settle()
    orig = rid("protocol/d")
    assert (await get(h, orig)).status == "expired_in_hermes"
    h.events()

    new, follow = rid("protocol/c"), run_id("protocol/c")
    h.fake.enqueue(
        "protocol/c",
        pause_before_seq=2,  # Hermes waits for the (automatic) answer
        approvals={new: load_json(f"protocol/approval-once-{follow}.json")},
    )
    out = await svc(h).respond(orig, "once", "pixel")
    assert out["status"] == "late_approved" and out["decided_by"] == "pixel"
    await h.settle()

    a = await get(h, orig)
    assert (a.status, a.choice, a.decided_by, a.followup_run_id) == (
        "executed", "once", "pixel", follow,
    )  # fmt: skip
    b = await get(h, new)
    assert (b.status, b.choice, b.decided_by) == ("approved_once", "once", AUTO_DECIDED_BY)
    posts = h.fake.calls("POST", f"/v1/runs/{follow}/approval")
    assert [p.body for p in posts] == [{"request_id": new, "choice": "once"}]
    assert h.fake.calls("POST", "/v1/runs")[1].body["input"] == late_prompt(CMD)

    async with h.db() as s:
        run = await s.get(Run, follow)
        msgs = list(
            (
                await s.execute(
                    select(Message).where(Message.conversation_id == conv_id).order_by(Message.id)
                )
            ).scalars()
        )
    assert run is not None and run.origin == "late_approval" and run.status == "completed"
    assert [m.role for m in msgs] == ["user", "assistant", "system", "assistant"]
    assert CMD in msgs[2].content and msgs[2].run_id == follow
    assert approval_statuses(h, orig) == ["late_approved", "followup_started", "executed"]


async def test_followup_with_another_command_is_not_auto_approved(h: Harness) -> None:
    orig = await seed_expired(h)
    h.fake.enqueue("protocol/appr")  # asks for `rm -r /tmp/delve-fx-dir`, answered deny
    await svc(h).respond(orig, "session", "pixel")
    await h.mgr.start()
    await h.settle()
    a = await get(h, orig)
    assert (a.status, a.choice) == ("superseded", "session")
    other = await get(h, rid("protocol/appr"))
    assert other.status == "denied" and other.decided_by is None  # answered elsewhere
    assert h.fake.calls("POST", f"/v1/runs/{run_id('protocol/appr')}/approval") == []


async def test_followup_without_approval_supersedes(h: Harness) -> None:
    orig = await seed_expired(h)
    h.fake.enqueue("protocol/e1")
    await svc(h).respond(orig, "once", "pixel")
    await h.mgr.start()
    await h.settle()
    assert (await get(h, orig)).status == "superseded"


async def test_late_deny_needs_no_run(h: Harness) -> None:
    orig = await seed_expired(h)
    out = await svc(h).respond(orig, "deny", "pixel")
    assert out["status"] == "denied" and out["decided_by"] == "pixel"
    async with h.db() as s:
        assert len(list((await s.execute(select(Run))).scalars())) == 1  # only run_old
    assert h.fake.requests == []
    with pytest.raises(ApprovalError) as exc:
        await svc(h).respond(orig, "once", "pixel")
    assert exc.value.code == "approval_already_decided"


async def test_expiry(h: Harness) -> None:
    orig = await seed_expired(h, expires_in=timedelta(seconds=-1))
    with pytest.raises(ApprovalError) as exc:
        await svc(h).respond(orig, "once", "pixel")
    assert exc.value.code == "approval_expired"
    assert (await get(h, orig)).status == "expired"
    with pytest.raises(ApprovalError) as exc:
        await svc(h).respond(orig, "once", "pixel")
    assert exc.value.code == "approval_expired"


async def test_expiry_sweep(h: Harness) -> None:
    due = await seed_expired(h, expires_in=timedelta(seconds=-1))
    async with h.db() as s:
        s.add(
            Approval(request_id="later", run_id="run_old", conversation_id="c_late",
                     command="ls", command_sha256=command_hash("ls"), action_class="other",
                     status="expired_in_hermes", hermes_deadline=datetime.now(UTC),
                     expires_at=datetime.now(UTC) + timedelta(hours=1))
        )  # fmt: skip
        await s.commit()
    await h.mgr._expire_tick()
    assert (await get(h, due)).status == "expired"
    assert (await get(h, "later")).status == "expired_in_hermes"
    assert approval_statuses(h, due) == ["expired"]


async def test_gone_while_answering_becomes_late(h: Harness) -> None:
    orig, first = rid("protocol/d"), run_id("protocol/d")
    h.fake.enqueue(
        "protocol/d",
        pause_before_seq=2,
        approvals={orig: load_json(f"protocol/approval-once-{first}.json")},  # not_active
    )
    new, follow = rid("protocol/c"), run_id("protocol/c")
    h.fake.enqueue(
        "protocol/c",
        pause_before_seq=2,
        approvals={new: load_json(f"protocol/approval-once-{follow}.json")},
    )
    await h.send(CMD)
    await h.mgr.start()
    async with asyncio.timeout(5):
        while (a := await get_or_none(h, orig)) is None or a.status != "pending":  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    out = await svc(h).respond(orig, "once", "pixel")
    assert out["status"] == "late_approved"
    h.fake.runs[first].release.set()  # Hermes finishes the first run (timeout)
    await h.settle()
    assert (await get(h, orig)).status == "executed"
    assert (await get(h, new)).decided_by == AUTO_DECIDED_BY


async def get_or_none(h: Harness, request_id: str) -> Approval | None:
    async with h.db() as s:
        return await s.get(Approval, request_id)


async def test_failed_followup_gives_the_approval_back(h: Harness) -> None:
    orig = await seed_expired(h)
    h.fake.fail_next.append(
        (400, {"error": {"message": "bad", "type": "x", "param": None, "code": "invalid_request"}})
    )
    await svc(h).respond(orig, "once", "pixel")
    await h.mgr.start()
    await h.settle()
    a = await get(h, orig)
    assert (a.status, a.followup_run_id, a.choice) == ("expired_in_hermes", None, None)


async def test_stopping_a_queued_followup_cancels_it(h: Harness) -> None:
    orig = await seed_expired(h)
    out = await svc(h).respond(orig, "once", "pixel")
    qid = (await get(h, orig)).followup_run_id
    assert out["status"] == "late_approved" and qid and qid.startswith("q_")
    assert await h.mgr.stop(qid) == "cancelled"
    assert (await get(h, orig)).status == "cancelled"
    assert h.fake.requests == []


async def test_reconcile_after_gateway_restart(h: Harness) -> None:
    async with h.db() as s:
        s.add(Conversation(id="c_r"))
        await s.flush()
        s.add_all(
            [
                Run(run_id="run_dead", conversation_id="c_r", idempotency_key=uuid.uuid4(),
                    status="failed", input="q", error="run_lost"),
                Approval(request_id="stale", run_id="run_dead", conversation_id="c_r",
                         command="ls", command_sha256=command_hash("ls"), action_class="other",
                         status="pending", hermes_deadline=datetime.now(UTC),
                         expires_at=datetime.now(UTC) + timedelta(hours=1)),
            ]
        )  # fmt: skip
        await s.commit()
    await h.mgr._health_tick()  # first good poll: silent
    assert h.mgr.hermes_ok is True and h.mgr.hermes_version == "0.22.2"
    h.fake.health_down = True
    await h.mgr._health_tick()
    h.fake.health_down = False
    await h.mgr._health_tick()
    evs = h.events()
    assert [e.data["hermes"] for e in evs if e.type == "health.status"] == ["down", "ok"]
    assert (await get(h, "stale")).status == "expired_in_hermes"
