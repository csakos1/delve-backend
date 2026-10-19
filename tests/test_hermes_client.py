from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from delve.hermes.client import (
    EXPECTED_TOOLSETS,
    HermesClient,
    HermesError,
    HermesUnavailable,
    StreamEnded,
    ThinkingRoute,
    build_run_body,
    capability_problems,
    new_reasoning,
)
from delve.hermes.models import Health, SessionMessage
from delve.hermes.sse import parse_events

from .fake_hermes import API_KEY, FakeHermes, load_json, load_text

ROUTE = ThinkingRoute(provider="custom:llama-qwen38-think", model="/home/<operator>/m.gguf")


@pytest.fixture
def fake() -> FakeHermes:
    return FakeHermes()


@pytest.fixture
async def client(fake: FakeHermes) -> AsyncIterator[HermesClient]:
    c = HermesClient(
        "http://hermes.test", API_KEY, ROUTE, transport=httpx.ASGITransport(app=fake.app)
    )
    async with c:
        yield c


# --- body / capability helpers (pure) ------------------------------------------


def test_run_body_without_thinking_has_no_route() -> None:
    body = build_run_body(session_id="c_1", input="hi", thinking=False, route=ROUTE)
    assert body == {"input": "hi", "session_id": "c_1"}


def test_run_body_thinking_adds_provider_and_model() -> None:
    body = build_run_body(
        session_id="c_1", input="hi", thinking=True, route=ROUTE, instructions="ctx"
    )
    assert body == {
        "input": "hi",
        "session_id": "c_1",
        "instructions": "ctx",
        "provider": "custom:llama-qwen38-think",
        "model": "/home/<operator>/m.gguf",
    }


def test_run_body_content_parts_and_empty() -> None:
    parts = [{"type": "text", "text": "mi ez?"}, {"type": "image_url", "image_url": {"url": "x"}}]
    body = build_run_body(session_id="c", input=parts, thinking=False, route=ROUTE)
    assert body["input"] == parts and body["input"] is not parts
    for empty in ("", "   ", []):
        with pytest.raises(ValueError, match="empty"):
            build_run_body(session_id="c", input=empty, thinking=False, route=ROUTE)


def test_capability_problems() -> None:
    from .fake_hermes import SYNTHETIC_CAPABILITIES as caps

    ok = Health(status="ok", platform="hermes-agent", version="0.22.2")
    assert capability_problems(caps, ok, "0.22.2") == []
    nested = {"features": {k: {"supported": True} for k in caps if caps[k] is True}}
    assert capability_problems(nested, ok, "0.22.2") == []
    broken = {**caps, "run_stop": False}
    assert capability_problems(broken, ok, "0.22.2") == ["capability run_stop missing"]
    old = Health(status="ok", version="0.21.0")
    assert capability_problems(caps, old, "0.22.2") == ["version '0.21.0' != expected '0.22.2'"]


# --- endpoints against the fake ------------------------------------------------


async def test_health_sends_no_auth(client: HermesClient, fake: FakeHermes) -> None:
    h = await client.health()
    assert (h.status, h.version) == ("ok", "0.22.2")
    assert "authorization" not in fake.requests[-1].headers


async def test_capabilities_and_toolsets(client: HermesClient) -> None:
    caps = await client.capabilities()
    assert capability_problems(caps, await client.health(), "0.22.2") == []
    enabled = [t.name for t in await client.toolsets() if t.enabled]
    assert enabled == list(EXPECTED_TOOLSETS)


async def test_wrong_key_is_hermes_error(fake: FakeHermes) -> None:
    transport = httpx.ASGITransport(app=fake.app)
    async with HermesClient("http://h", "wrong", ROUTE, transport=transport) as c:
        with pytest.raises(HermesError) as ei:
            await c.capabilities()
    assert (ei.value.status, ei.value.code) == (401, "invalid_api_key")


@pytest.mark.parametrize("thinking", [False, True])
async def test_create_run_body_and_headers(
    client: HermesClient, fake: FakeHermes, thinking: bool
) -> None:
    fake.enqueue("protocol/a")
    key = uuid.uuid4()
    created = await client.create_run(
        session_id="c_x", input="uptime?", thinking=thinking, idempotency_key=key
    )
    assert created.run_id == "run_1fa3ea593870452d9b784ca355e42119"
    assert created.replayed is False
    req = fake.calls("POST", "/v1/runs")[-1]
    assert req.headers["idempotency-key"] == str(key)
    assert req.headers["authorization"] == f"Bearer {API_KEY}"
    assert ("provider" in req.body) is thinking and ("model" in req.body) is thinking
    if thinking:
        assert req.body["provider"] == "custom:llama-qwen38-think"


async def test_idempotent_replay_and_conflict(client: HermesClient, fake: FakeHermes) -> None:
    fake.enqueue("protocol/a")
    key = uuid.uuid4()
    first = await client.create_run(session_id="c", input="q", thinking=False, idempotency_key=key)
    again = await client.create_run(session_id="c", input="q", thinking=False, idempotency_key=key)
    assert again.run_id == first.run_id and again.replayed is True
    with pytest.raises(HermesError) as ei:
        await client.create_run(session_id="c", input="other", thinking=False, idempotency_key=key)
    assert (ei.value.status, ei.value.code) == (409, "idempotency_key_conflict")


async def _start(client: HermesClient, fake: FakeHermes, name: str, **kw: Any) -> str:
    fake.enqueue(name, **kw)
    created = await client.create_run(
        session_id="c", input="q", thinking=False, idempotency_key=uuid.uuid4()
    )
    return created.run_id


async def test_stream_full_run(client: HermesClient, fake: FakeHermes) -> None:
    run_id = await _start(client, fake, "protocol/b")
    events = [e async for e in client.stream_events(run_id)]
    names = [e.event for e in events]
    assert names[:3] == ["tool.started", "approval.request", "approval.responded"]
    assert names[-1] == "run.completed"
    assert "reasoning.available" not in names
    req = fake.calls("GET", f"/v1/runs/{run_id}/events")[-1]
    assert req.headers["accept"] == "text/event-stream"
    assert "last-event-id" not in req.headers


async def test_stream_cancelled_run(client: HermesClient, fake: FakeHermes) -> None:
    run_id = await _start(client, fake, "protocol/f")
    events = [e async for e in client.stream_events(run_id)]
    assert [e.event for e in events] == ["tool.started", "tool.completed", "run.cancelled"]
    assert '"exit_code": 130' in events[1].data["preview"]


@pytest.mark.parametrize("honour", [True, False])
async def test_stream_disconnect_and_resume(
    client: HermesClient, fake: FakeHermes, honour: bool
) -> None:
    run_id = await _start(client, fake, "protocol/a", cut_after_seq=10)
    got = []
    with pytest.raises(StreamEnded) as ei:
        async for e in client.stream_events(run_id):
            got.append(e)
    assert ei.value.last_seq == 10 and got[-1].seq == 10

    sc = fake.runs[run_id]
    sc.cut_after_seq, sc.honour_last_event_id = None, honour
    rest = [e async for e in client.stream_events(run_id, after_seq=ei.value.last_seq)]
    assert fake.calls("GET", f"/v1/runs/{run_id}/events")[-1].headers["last-event-id"] == "10"
    assert rest[0].seq == 11 and rest[-1].event == "run.completed"
    ref = [e.seq for e in parse_events(load_text("protocol/a.events.sse"))]
    assert [e.seq for e in got + rest] == ref  # no gap, no duplicate


async def test_stream_unknown_run(client: HermesClient) -> None:
    with pytest.raises(HermesError) as ei:
        _ = [e async for e in client.stream_events("run_nope")]
    assert (ei.value.status, ei.value.code) == (404, "run_not_found")


async def test_get_run_waiting_for_approval(client: HermesClient, fake: FakeHermes) -> None:
    run_id = await _start(client, fake, "protocol/d", status="protocol/d.waiting.json")
    st = await client.get_run(run_id)
    assert st.status == "waiting_for_approval" and not st.terminal
    assert st.last_event == "approval.request"
    assert st.approval is not None
    assert st.approval.command == "rm -r /tmp/x"
    assert st.approval.request_id == "2563013e4c314aa09171a77ad27b29bc"
    assert "always" in st.approval.choices

    fake.runs[run_id].status = "protocol/d.final.json"
    final = await client.get_run(run_id)
    assert final.terminal and final.usage is not None and final.usage.output_tokens == 116


async def test_approval_whitelist_never_sends_always(
    client: HermesClient, fake: FakeHermes
) -> None:
    before = len(fake.requests)
    with pytest.raises(ValueError, match="always"):
        await client.respond_approval("run_x", "req", "always")
    assert len(fake.requests) == before


async def test_approval_ok_and_not_pending(client: HermesClient, fake: FakeHermes) -> None:
    run_c = await _start(client, fake, "protocol/c")
    ok = load_json("protocol/approval-once-run_2e98aaff26d64dcaae6e430b519fa7c0.json")
    fake.runs[run_c].approvals[ok["request_id"]] = ok
    resp = await client.respond_approval(run_c, ok["request_id"], "once")
    assert (resp.choice, resp.resolved) == ("once", 1)
    assert fake.requests[-1].body == {"request_id": ok["request_id"], "choice": "once"}

    run_b = await _start(client, fake, "protocol/b")
    err = load_json("protocol/approval-deny-run_f7e77ef8b421445ba5e6e5f7c66768cc.json")
    fake.runs[run_b].approvals["41987fd3adf448fba544e8934b7da49e"] = err
    with pytest.raises(HermesError) as ei:
        await client.respond_approval(run_b, "41987fd3adf448fba544e8934b7da49e", "deny")
    assert ei.value.code == "approval_not_pending"


async def test_stop_run(client: HermesClient, fake: FakeHermes) -> None:
    run_id = await _start(client, fake, "protocol/f", status="protocol/f.stopping.json")
    assert (await client.stop_run(run_id) or {}).get("status") == "stopping"
    st = await client.get_run(run_id)
    assert st.status == "cancelled" and st.turn_exit_reason == "interrupted_by_user"


async def test_session_messages(client: HermesClient, fake: FakeHermes) -> None:
    fake.messages["proto-sess-1"] = load_json("protocol/e.messages.json")
    msgs = await client.get_session_messages("proto-sess-1")
    assert [m.role for m in msgs] == ["user", "assistant", "user", "assistant"]
    assert all(m.thinking_text is None for m in msgs)
    req = fake.requests[-1]
    assert req.query == {"inline_images": "false", "limit": "500"}


def _msg(mid: int, role: str, content: str = "", reasoning: str | None = None) -> SessionMessage:
    return SessionMessage(
        id=mid, role=role, content=content, reasoning=reasoning, reasoning_content=reasoning
    )


def test_new_reasoning_dedup_and_tool_runs() -> None:
    # synthetic: a tool-using thinking run (two assistant messages, each with reasoning)
    msgs = [
        _msg(1, "user", "q"),
        _msg(2, "assistant", "", "The user asks... I should run uptime."),
        _msg(3, "tool", '{"output": "up"}'),
        _msg(4, "assistant", "Fent van.", "Tool output says up."),
        _msg(5, "assistant", "x", "   "),
    ]
    got = new_reasoning(msgs, set())
    assert [mid for mid, _ in got] == ["2", "4"]
    assert new_reasoning(msgs, {"2"})[0][0] == "4"
    assert new_reasoning(msgs, {"2", "4"}) == []


async def test_unreachable_is_hermes_unavailable() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with HermesClient("http://h", "k", ROUTE, transport=httpx.MockTransport(boom)) as c:
        with pytest.raises(HermesUnavailable):
            await c.health()
        with pytest.raises(HermesUnavailable):
            _ = [e async for e in c.stream_events("run_x")]


async def test_non_json_error_body() -> None:
    def bad(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="<html>bad gateway</html>")

    async with HermesClient("http://h", "k", ROUTE, transport=httpx.MockTransport(bad)) as c:
        with pytest.raises(HermesError) as ei:
            await c.get_run("run_x")
    assert ei.value.status == 502 and ei.value.code is None
