"""Run queue and lifecycle against the fixture-replaying fake Hermes and a live database."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import func, select

from delve.db.models import (
    Conversation,
    Message,
    Run,
    RunEvent,
    SystemState,
)
from delve.events import Envelope
from delve.runs.manager import NotFound

from .fake_hermes import load_json
from .harness import ROUTE, Harness, final_output, sse_events

pytestmark = pytest.mark.pg


def types(events: list[Envelope]) -> list[str]:
    return [e.type for e in events if e.type != "message.delta"]


async def test_simple_run(h: Harness) -> None:
    h.fake.enqueue("protocol/a")
    conv_id, qid = await h.send("load?")
    assert qid.startswith("q_")
    await h.mgr.start()
    await h.settle()

    run_id = load_json("protocol/a.create.json")["run_id"]
    run = await h.run(run_id)
    assert run.status == "completed" and run.error is None
    assert run.usage and run.usage["output_tokens"] == 58
    assert run.perf is not None and run.perf["sse_reconnects"] == 0
    assert run.perf["first_delta_s"] >= 0  # no llama client: no context fields
    assert set(run.perf) == {"sse_reconnects", "first_delta_s"}
    user, assistant = await h.messages(conv_id)
    assert user.role == "user" and user.run_id == run_id and user.content == "load?"
    assert assistant.role == "assistant" and assistant.status == "final"
    assert assistant.content == final_output("protocol/a")  # N27 (a): the output, no "\n\n"
    assert assistant.interim is None
    async with h.db() as s:
        stored = await s.scalar(
            select(func.count()).select_from(RunEvent).where(RunEvent.run_id == run_id)
        )
        conv = await s.get(Conversation, conv_id)
    assert stored == len(sse_events("protocol/a")) - 1  # reasoning.available is dropped
    assert conv is not None and conv.last_run_id == run_id

    body = h.fake.calls("POST", "/v1/runs")[0].body
    assert body["session_id"] == conv_id and body["input"] == "load?"
    assert "provider" not in body and "model" not in body

    evs = h.events()
    assert types(evs) == [
        "conversation.created",
        "message.created",
        "run.queued",
        "run.started",
        "message.created",
        "tool.started",
        "tool.completed",
        "message.updated",
        "run.finished",
        "conversation.updated",  # totals (and, with a llama client, the context)
    ]
    totals = evs[-1].data["totals"]
    assert totals["input_tokens"] == 16290 and totals["total_tokens"] == 16348
    started = next(e for e in evs if e.type == "run.started")
    assert started.data["queued_id"] == qid and started.data["run_id"] == run_id
    streamed = "".join(e.data["delta"] for e in evs if e.type == "message.delta")
    assert streamed == "\n\n" + final_output("protocol/a")
    assert [e.seq for e in evs] == sorted(e.seq for e in evs)


async def test_interim_kept_apart(h: Harness) -> None:
    h.fake.enqueue("runs/t1")
    conv_id, _ = await h.send("rm -rf /tmp/dl1")
    await h.mgr.start()
    await h.settle()
    _, assistant = await h.messages(conv_id)
    assert assistant.content == final_output("runs/t1")
    assert assistant.interim == "I'll help you run that command."
    interim_events = [e for e in h.events() if e.type == "message.interim"]
    assert [e.data["text"] for e in interim_events] == ["I'll help you run that command."]


async def test_fifo_and_positions(h: Harness) -> None:
    h.fake.enqueue("protocol/e1")
    h.fake.enqueue("protocol/e2")
    c1, q1 = await h.send("one")
    c2, q2 = await h.send("two")
    queued = [e.data for e in h.events() if e.type == "run.queued"]
    assert [(d["run_id"], d["position"]) for d in queued] == [(q1, 0), (q2, 1)]

    await h.mgr.start()
    await h.settle()
    paths = [(r.method, r.path) for r in h.fake.requests]
    first = load_json("protocol/e1.create.json")["run_id"]
    # the second run is submitted only after the first one's stream ended
    assert paths.index(("POST", "/v1/runs")) < paths.index(("GET", f"/v1/runs/{first}/events"))
    second_post = [i for i, p in enumerate(paths) if p == ("POST", "/v1/runs")][1]
    assert second_post > paths.index(("GET", f"/v1/runs/{first}/events"))
    assert [b.body["session_id"] for b in h.fake.calls("POST", "/v1/runs")] == [c1, c2]


async def test_idempotent_submit(h: Harness) -> None:
    cid = uuid.uuid4()
    a = await h.mgr.submit(text="x", client_msg_id=cid)
    b = await h.mgr.submit(text="x", client_msg_id=cid)
    assert a.created and not b.created
    assert a.run_id == b.run_id and a.message["id"] == b.message["id"]
    async with h.db() as s:
        assert await s.scalar(select(func.count()).select_from(Run)) == 1


async def test_submit_validation(h: Harness) -> None:
    from delve.runs.manager import InvalidRequest

    with pytest.raises(InvalidRequest):
        await h.mgr.submit(text="   ", client_msg_id=uuid.uuid4())
    with pytest.raises(NotFound):
        await h.mgr.submit(text="x", client_msg_id=uuid.uuid4(), conversation_id="c_nope")


async def test_stop_queued(h: Harness) -> None:
    _, qid = await h.send()
    assert await h.mgr.stop(qid) == "cancelled"
    assert await h.mgr.stop(qid) == "cancelled"  # idempotent
    await h.mgr.start()
    await h.settle()
    assert h.fake.calls("POST", "/v1/runs") == []
    assert (await h.run(qid)).status == "cancelled"
    with pytest.raises(NotFound):
        await h.mgr.stop("run_nope")


async def test_stop_active(h: Harness) -> None:
    h.fake.enqueue("protocol/f", pause_before_seq=1)
    conv_id, qid = await h.send("sleep 60")
    await h.mgr.start()
    async with asyncio.timeout(5):
        while not (h.mgr.active_run_id or "").startswith("run_"):  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    assert await h.mgr.stop(qid) == "stopping"  # the queue id still works after the rename
    await h.settle()
    run_id = load_json("protocol/f.create.json")["run_id"]
    assert h.fake.runs[run_id].stopped
    run = await h.run(run_id)
    assert run.status == "cancelled" and run.error == "interrupted_by_user"
    _, assistant = await h.messages(conv_id)
    assert assistant.status == "cancelled"
    statuses = [e.data["status"] for e in h.events() if e.type == "run.updated"]
    assert statuses == ["stopping"]


@pytest.mark.parametrize("honour", [True, False])
async def test_reconnect_resumes_without_duplicates(h: Harness, honour: bool) -> None:
    run_id = load_json("protocol/a.create.json")["run_id"]
    h.fake.enqueue(
        "protocol/a",
        cut_after_seq=10,
        cut_once=True,
        honour_last_event_id=honour,
        status={"object": "hermes.run", "run_id": run_id, "status": "running"},
    )
    conv_id, _ = await h.send()
    await h.mgr.start()
    await h.settle()
    run = await h.run(run_id)
    assert run.status == "completed" and run.perf and run.perf["sse_reconnects"] == 1
    gets = h.fake.calls("GET", f"/v1/runs/{run_id}/events")
    assert len(gets) == 2 and gets[1].headers["last-event-id"] == "10"
    evs = h.events()
    streamed = "".join(e.data["delta"] for e in evs if e.type == "message.delta")
    assert streamed == "\n\n" + final_output("protocol/a")  # nothing repeated
    assert [e.data["status"] for e in evs if e.type == "run.updated"] == [
        "reconnecting",
        "running",
    ]
    async with h.db() as s:
        stored = await s.scalar(
            select(func.count()).select_from(RunEvent).where(RunEvent.run_id == run_id)
        )
    assert stored == len(sse_events("protocol/a")) - 1
    _, assistant = await h.messages(conv_id)
    assert assistant.content == final_output("protocol/a")


async def test_cut_stream_settled_from_status(h: Harness) -> None:
    h.fake.enqueue("protocol/a", cut_after_seq=5, status="protocol/a.final.json")
    conv_id, _ = await h.send()
    await h.mgr.start()
    await h.settle()
    run = await h.run(load_json("protocol/a.create.json")["run_id"])
    assert run.status == "completed" and run.usage and run.usage["output_tokens"] == 58
    _, assistant = await h.messages(conv_id)
    assert assistant.content == final_output("protocol/a") and assistant.status == "final"


async def test_run_lost_after_gateway_restart(h: Harness) -> None:
    h.fake.enqueue("protocol/a", cut_after_seq=5)  # no status -> 404 like after a restart
    conv_id, _ = await h.send()
    await h.mgr.start()
    await h.settle()
    run = await h.run(load_json("protocol/a.create.json")["run_id"])
    assert run.status == "failed" and run.error == "run_lost"
    _, assistant = await h.messages(conv_id)
    assert assistant.status == "failed" and assistant.content  # the partial text is kept


async def test_thinking_run(h: Harness) -> None:
    h.fake.enqueue("protocol/think")
    conv_id, _ = await h.send("17*23?", thinking=True)
    session = load_json("protocol/think.session.json")
    session["session"]["id"] = conv_id
    h.fake.sessions[conv_id] = session
    h.fake.messages[conv_id] = load_json("protocol/think.messages.json")
    await h.mgr.start()
    await h.settle()

    body = h.fake.calls("POST", "/v1/runs")[0].body
    assert body["provider"] == ROUTE.provider and body["model"] == ROUTE.model
    run_id = load_json("protocol/think.create.json")["run_id"]
    run = await h.run(run_id)
    assert run.thinking and run.perf
    assert (run.perf["sse_reconnects"], run.perf["route_applied"]) == (0, True)
    _, assistant = await h.messages(conv_id)
    reasoning = load_json("protocol/think.messages.json")["data"][1]["reasoning"]
    assert assistant.content == "391"
    assert assistant.reasoning == reasoning and assistant.hermes_message_id == "200"
    async with h.db() as s:
        state = await s.get(SystemState, "thinking_route")
        conv = await s.get(Conversation, conv_id)
    assert state is not None and state.value["ok"] is True
    assert conv is not None and conv.title == session["session"]["title"]

    evs = h.events()
    final = [e.data for e in evs if e.type == "reasoning.final"]
    assert final == [
        {"run_id": run_id, "conversation_id": conv_id, "message_id": assistant.id,
         "text": reasoning}
    ]  # fmt: skip
    titled = [e.data for e in evs if e.type == "conversation.updated" and e.data["title"]]
    assert titled and titled[-1]["title"] == conv.title
    assert types(evs).index("reasoning.final") > types(evs).index("run.finished")


async def test_thinking_route_regression_is_reported(h: Harness) -> None:
    h.fake.enqueue("protocol/a")  # runtime.route_source == "global"
    await h.send(thinking=True)
    await h.mgr.start()
    await h.settle()
    run = await h.run(load_json("protocol/a.create.json")["run_id"])
    assert run.status == "completed" and run.perf and run.perf["route_applied"] is False
    async with h.db() as s:
        state = await s.get(SystemState, "thinking_route")
    assert state is not None and state.value["ok"] is False
    errors = [e.data for e in h.events() if e.type == "error"]
    assert [e["code"] for e in errors] == ["thinking_route_not_applied"]


async def test_thinking_follows_conversation_at_start(h: Harness) -> None:
    h.fake.enqueue("protocol/e1")
    conv_id, _ = await h.send()  # created without thinking
    await h.mgr.set_conversation(conv_id, thinking=True)  # switched before the run starts
    await h.mgr.start()
    await h.settle()
    assert h.fake.calls("POST", "/v1/runs")[0].body["provider"] == ROUTE.provider


async def test_submission_rejected(h: Harness) -> None:
    h.fake.fail_next.append(
        (400, {"error": {"message": "bad", "type": "x", "param": None, "code": "invalid_request"}})
    )
    conv_id, qid = await h.send()
    await h.mgr.start()
    await h.settle()
    run = await h.run(qid)
    assert run.status == "failed" and run.error == "hermes_400_invalid_request"
    assert [m.role for m in await h.messages(conv_id)] == ["user"]


async def test_submission_retried_with_same_key(h: Harness) -> None:
    h.fake.enqueue("protocol/e1")
    h.fake.fail_next.append((503, {"error": {"code": "unavailable", "message": "busy"}}))
    await h.send()
    await h.mgr.start()
    await h.settle()
    posts = h.fake.calls("POST", "/v1/runs")
    assert len(posts) == 2
    assert posts[0].headers["idempotency-key"] == posts[1].headers["idempotency-key"]
    assert (await h.run(load_json("protocol/e1.create.json")["run_id"])).status == "completed"


async def test_approval_status_transitions(h: Harness) -> None:
    h.fake.enqueue("protocol/b")
    await h.send()
    await h.mgr.start()
    await h.settle()
    statuses = [e.data["status"] for e in h.events() if e.type == "run.updated"]
    assert statuses == ["waiting_for_approval", "running"]


async def test_recovery_resumes_running_run(h: Harness) -> None:
    run_id = load_json("protocol/a.create.json")["run_id"]
    events = [e for e in sse_events("protocol/a") if e["event"] != "reasoning.available"]
    async with h.db() as s:
        s.add(Conversation(id="c_rec"))
        await s.flush()
        s.add_all(
            [
                Message(conversation_id="c_rec", run_id=run_id, role="user", content="q"),
                Run(run_id=run_id, conversation_id="c_rec", idempotency_key=uuid.uuid4(),
                    status="running", input="q"),
                Message(conversation_id="c_rec", run_id=run_id, role="assistant",
                        status="streaming", content="stale"),
                *[RunEvent(run_id=run_id, seq=e["seq"], event=e["event"], payload=e)
                  for e in events[:6]],
            ]
        )  # fmt: skip
        await s.commit()
    from .fake_hermes import Scenario

    h.fake.runs[run_id] = Scenario(run_id=run_id, events="protocol/a.events.sse")
    await h.mgr.start()
    await h.settle()
    assert (await h.run(run_id)).status == "completed"
    get = h.fake.calls("GET", f"/v1/runs/{run_id}/events")[0]
    assert get.headers["last-event-id"] == str(events[5]["seq"])
    _, assistant = await h.messages("c_rec")
    assert assistant.content == final_output("protocol/a") and assistant.status == "final"
    assert h.fake.calls("POST", "/v1/runs") == []


async def test_recovery_resubmits_starting_run(h: Harness) -> None:
    key = uuid.uuid4()
    async with h.db() as s:
        s.add(Conversation(id="c_rec2"))
        await s.flush()
        s.add_all(
            [
                Message(conversation_id="c_rec2", run_id="q_1", role="user", content="q"),
                Run(run_id="q_1", conversation_id="c_rec2", idempotency_key=key,
                    status="starting", input="q"),
            ]
        )  # fmt: skip
        await s.commit()
    h.fake.enqueue("protocol/e1")
    await h.mgr.start()
    await h.settle()
    post = h.fake.calls("POST", "/v1/runs")[0]
    assert post.headers["idempotency-key"] == str(key)
    run_id = load_json("protocol/e1.create.json")["run_id"]
    assert (await h.run(run_id)).status == "completed"
    assert [m.run_id for m in await h.messages("c_rec2")] == [run_id, run_id]


def test_failed_run_reason_from_output() -> None:
    """N73: Hermes 0.22.2 ends a run cut at the output limit with `run.failed`, no `error`,
    the explanation in `output` (recorded 2026-10-27)."""
    import json
    from pathlib import Path

    from delve.hermes.models import RunStatus
    from delve.runs.manager import _error_text, _status_error

    p = Path(__file__).resolve().parent / "fixtures" / "protocol" / "failed-partial.event.json"
    data = json.loads(p.read_text())
    assert data["partial"] is True and "error" not in data
    assert _error_text(data) == data["output"][:500]
    assert _error_text({**data, "event": "run.cancelled"}) is None
    assert _error_text({**data, "error": "boom"}) == "boom"
    assert _error_text({"event": "run.completed", "output": "x"}) is None
    st = RunStatus(run_id="r", status="failed", output=data["output"])
    assert _status_error(st) == data["output"]
    assert _status_error(RunStatus(run_id="r", status="cancelled", output="x")) is None
    assert _status_error(RunStatus(run_id="r", status="failed", turn_exit_reason="t")) == "t"
