"""REST and WebSocket API through the real app (TestClient), fake Hermes, live database."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.testclient import WebSocketTestSession
from starlette.websockets import WebSocketDisconnect

from delve.app import create_app
from delve.auth import create_device
from delve.config import Settings

from .fake_hermes import FakeHermes, StreamingASGITransport, load_json
from .harness import FAST, final_output

pytestmark = pytest.mark.pg


@pytest.fixture
async def token(db: async_sessionmaker[AsyncSession]) -> str:
    async with db() as s:
        _, tok = await create_device(s, "pixel")
        await s.commit()
    return tok


@pytest.fixture
def fake() -> FakeHermes:
    return FakeHermes()


@pytest.fixture
def client(
    env: pytest.MonkeyPatch,
    migrated_url: str,
    fake: FakeHermes,
    db: async_sessionmaker[AsyncSession],
) -> Iterator[TestClient]:
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("HERMES_API_KEY", "fake-key")
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(fake.app),
        timing=FAST,
    )
    with TestClient(app) as c:
        yield c


def auth(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


def hello(ws: WebSocketTestSession, tok: str, **extra: Any) -> dict[str, Any]:
    ws.send_json({"type": "hello", "data": {"device_token": tok, **extra}})
    frame: dict[str, Any] = ws.receive_json()
    assert frame["type"] == "hello.ok", frame
    return frame


def until(ws: WebSocketTestSession, pred: Any, limit: int = 2000) -> list[dict[str, Any]]:
    seen = []
    for _ in range(limit):
        frame = ws.receive_json()
        if frame["type"] == "keepalive":
            continue
        seen.append(frame)
        if pred(frame):
            return seen
    raise AssertionError("condition not met")


def chat(ws: WebSocketTestSession, text: str, fid: str = "1", **data: Any) -> list[dict[str, Any]]:
    ws.send_json(
        {"type": "chat.send", "id": fid,
         "data": {"text": text, "client_msg_id": str(uuid.uuid4()), **data}}
    )  # fmt: skip
    return until(ws, lambda f: f["type"] == "run.finished")


# --- REST ------------------------------------------------------------------------------


def test_rest_requires_token(client: TestClient, token: str) -> None:
    for headers in ({}, {"Authorization": "Bearer dlv_wrong"}, {"Authorization": token}):
        r = client.get("/api/v1/conversations", headers=headers)
        assert r.status_code == 401
        assert r.headers["www-authenticate"] == "Bearer"
    assert client.get("/api/v1/conversations", headers=auth(token)).status_code == 200
    # health and meta stay open for probes
    assert client.get("/api/v1/meta").status_code == 200


def test_rest_conversations_and_messages(client: TestClient, token: str, fake: FakeHermes) -> None:
    fake.enqueue("protocol/e1")
    fake.enqueue("protocol/e2")
    with client.websocket_connect("/api/v1/ws") as ws:
        hello(ws, token)
        first = chat(ws, "one")
        second = chat(ws, "two", fid="2")
    c1 = next(f for f in first if f["type"] == "ack")["data"]["conversation_id"]
    c2 = next(f for f in second if f["type"] == "ack")["data"]["conversation_id"]

    r = client.get("/api/v1/conversations?limit=1", headers=auth(token))
    page = r.json()
    assert [c["id"] for c in page["data"]] == [c2]  # most recently updated first
    assert page["next_cursor"]
    r = client.get(f"/api/v1/conversations?limit=1&cursor={page['next_cursor']}",
                   headers=auth(token))  # fmt: skip
    assert [c["id"] for c in r.json()["data"]] == [c1] and r.json()["next_cursor"] is None
    assert client.get("/api/v1/conversations?cursor=%%%", headers=auth(token)).status_code == 400

    msgs = client.get(f"/api/v1/conversations/{c1}/messages", headers=auth(token)).json()
    assert [m["role"] for m in msgs["data"]] == ["user", "assistant"] and not msgs["has_more"]
    assert msgs["data"][1]["content"] == final_output("protocol/e1")
    older = client.get(
        f"/api/v1/conversations/{c1}/messages?before={msgs['data'][1]['id']}&limit=5",
        headers=auth(token),
    ).json()
    assert [m["role"] for m in older["data"]] == ["user"]

    r = client.patch(f"/api/v1/conversations/{c1}", json={"thinking": True, "archived": True},
                     headers=auth(token))  # fmt: skip
    assert r.status_code == 200 and r.json()["thinking"] and r.json()["archived"]
    listed = client.get("/api/v1/conversations", headers=auth(token)).json()["data"]
    assert [c["id"] for c in listed] == [c2]
    archived = client.get("/api/v1/conversations?archived=true", headers=auth(token)).json()
    assert [c["id"] for c in archived["data"]] == [c1]

    assert client.patch(f"/api/v1/conversations/{c1}", json={"title": "x"},
                        headers=auth(token)).status_code == 422  # fmt: skip
    assert client.patch("/api/v1/conversations/c_nope", json={"thinking": True},
                        headers=auth(token)).status_code == 404  # fmt: skip
    assert client.get("/api/v1/conversations/c_nope/messages",
                      headers=auth(token)).status_code == 404  # fmt: skip
    assert client.get(f"/api/v1/conversations/{c2}", headers=auth(token)).json()["id"] == c2


# --- WebSocket ---------------------------------------------------------------------------


def test_ws_rejects_bad_hello(client: TestClient, token: str) -> None:
    with client.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "hello", "data": {"device_token": "dlv_nope"}})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
    assert exc.value.code == 4401
    with client.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "chat.send", "data": {}})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
    assert exc.value.code == 4400


def test_ws_chat_flow(client: TestClient, token: str, fake: FakeHermes) -> None:
    fake.enqueue("protocol/a")
    with client.websocket_connect("/api/v1/ws") as ws:
        ok = hello(ws, token)
        assert ok["data"]["resync_required"] is False and ok["data"]["epoch"]
        frames = chat(ws, "load?")
    ack = next(f for f in frames if f["type"] == "ack")
    assert ack["id"] == "1" and ack["data"]["run_id"].startswith("q_")
    assert ack["data"]["replayed"] is False
    events = [f for f in frames if "seq" in f]
    kinds = [f["type"] for f in events if f["type"] != "message.delta"]
    assert kinds == [
        "conversation.created", "message.created", "run.queued", "run.started",
        "message.created", "tool.started", "tool.completed", "message.updated", "run.finished",
    ]  # fmt: skip
    assert [f["seq"] for f in events] == list(range(events[0]["seq"], events[-1]["seq"] + 1))
    final = next(f for f in events if f["type"] == "message.updated")["data"]["message"]
    assert final["content"] == final_output("protocol/a") and final["status"] == "final"
    assert events[-1]["data"]["status"] == "completed"


def test_ws_resume_and_resync(client: TestClient, token: str, fake: FakeHermes) -> None:
    fake.enqueue("protocol/e1")
    with client.websocket_connect("/api/v1/ws") as ws:
        ok = hello(ws, token)
        frames = chat(ws, "hi")
    epoch = ok["data"]["epoch"]
    seqs = [f["seq"] for f in frames if "seq" in f]
    resume_from = seqs[2]

    with client.websocket_connect("/api/v1/ws") as ws:
        ok2 = hello(ws, token, last_seq=resume_from, epoch=epoch)
        assert ok2["data"]["resync_required"] is False
        replay = until(ws, lambda f: f.get("seq") == seqs[-1])
    assert [f["seq"] for f in replay] == seqs[3:]

    with client.websocket_connect("/api/v1/ws") as ws:
        ok3 = hello(ws, token, last_seq=resume_from, epoch="old-epoch")
        assert ok3["data"]["resync_required"] is True
        ws.send_json({"type": "ping", "id": "p"})
        assert ws.receive_json() == {"type": "pong", "id": "p", "data": {}}


def test_ws_commands_and_errors(client: TestClient, token: str, fake: FakeHermes) -> None:
    with client.websocket_connect("/api/v1/ws") as ws:
        hello(ws, token)

        def call(ftype: str, data: Any, fid: str = "x") -> dict[str, Any]:
            ws.send_json({"type": ftype, "id": fid, "data": data})
            frames = until(ws, lambda f: f.get("id") == fid)
            return frames[-1]

        assert call("nope", {})["data"]["code"] == "unknown_type"
        assert call("chat.send", {"text": "x", "client_msg_id": "bad"})["data"]["code"] == (
            "invalid_request"
        )
        assert (
            call(
                "chat.send",
                {"text": "x", "client_msg_id": str(uuid.uuid4()), "attachment_ids": ["a"]},
            )["data"]["code"]
            == "not_implemented"
        )
        assert (
            call("chat.send", {"text": " ", "client_msg_id": str(uuid.uuid4())})["data"]["code"]
            == "invalid_request"
        )
        assert call("run.stop", {"run_id": "run_nope"})["data"]["code"] == "not_found"
        assert (
            call("conversation.set", {"conversation_id": "c_x", "thinking": "yes"})["data"]["code"]
            == "invalid_request"
        )
        ws.send_text("not json")
        assert until(ws, lambda f: f["type"] == "error")[-1]["data"]["code"] == "invalid_json"

        cid = str(uuid.uuid4())
        fake.enqueue("protocol/e1")
        sent = call("chat.send", {"text": "hi", "client_msg_id": cid, "thinking": True}, "s")
        conv_id = sent["data"]["conversation_id"]
        until(ws, lambda f: f["type"] == "run.finished")
        replay = call("chat.send", {"text": "hi", "client_msg_id": cid}, "r")
        # the replay names the run as it is now: renamed from q_ to Hermes' id
        assert sent["data"]["run_id"].startswith("q_")
        assert replay["data"]["replayed"] is True
        assert replay["data"]["run_id"] == load_json("protocol/e1.create.json")["run_id"]

        conv = call("conversation.set", {"conversation_id": conv_id, "thinking": False}, "c")
        assert conv["type"] == "ack" and conv["data"]["conversation"]["thinking"] is False

        synced = call("sync", {"conversation_id": conv_id, "after_message_id": 0}, "y")
        assert [m["role"] for m in synced["data"]["messages"]] == ["user", "assistant"]
        assert synced["data"]["conversation"]["id"] == conv_id
        assert call("sync", {"conversation_id": "c_nope"}, "z")["data"]["code"] == "not_found"

    body = fake.calls("POST", "/v1/runs")[0].body
    assert body["provider"] == "custom:llama-qwen38-think"  # thinking=True on a new chat


def test_approval_over_ws_and_rest(client: TestClient, token: str, fake: FakeHermes) -> None:
    created = load_json("protocol/c.create.json")
    run_id = created["run_id"]
    rid = "9a498aa517494a869dbf623f95b62d44"
    fake.enqueue(
        "protocol/c",
        pause_before_seq=2,
        approvals={rid: load_json(f"protocol/approval-once-{run_id}.json")},
    )
    with client.websocket_connect("/api/v1/ws") as ws:
        hello(ws, token)
        cmid = str(uuid.uuid4())
        ws.send_json(
            {
                "type": "chat.send",
                "id": "1",
                "data": {"text": "rm -r /tmp/x", "client_msg_id": cmid},
            }
        )
        frames = until(ws, lambda f: f["type"] == "approval.request")
        req = frames[-1]["data"]
        assert req["request_id"] == rid and req["choices"] == ["once", "session", "deny"]
        assert req["action_class"] == "delete_kill" and req["command"] == "rm -r /tmp/x"

        # REST inbox sees it while it is pending
        inbox = client.get("/api/v1/approvals", headers=auth(token)).json()["data"]
        assert [a["request_id"] for a in inbox] == [rid]

        ws.send_json({"type": "approval.respond", "id": "a",
                      "data": {"request_id": rid, "choice": "always"}})  # fmt: skip
        err = until(ws, lambda f: f.get("id") == "a")[-1]
        assert err["type"] == "error" and err["data"]["code"] == "invalid_choice"

        ws.send_json({"type": "approval.respond", "id": "b",
                      "data": {"request_id": rid, "choice": "once"}})  # fmt: skip
        ack = until(ws, lambda f: f.get("id") == "b")[-1]
        assert ack["type"] == "ack" and ack["data"]["approval"]["status"] == "approved_once"
        assert ack["data"]["approval"]["decided_by"] == "pixel"
        rest = until(ws, lambda f: f["type"] == "run.finished")
        done = next(f for f in rest if f["type"] == "tool.completed")["data"]["step"]
        assert done["status"] == "ok"

    conv_id = req["conversation_id"]
    msgs = client.get(f"/api/v1/conversations/{conv_id}/messages", headers=auth(token)).json()
    user, assistant = msgs["data"]
    assert "tool_steps" not in user
    (step,) = assistant["tool_steps"]
    assert step["approval"]["status"] == "approved_once" and step["approval"]["choice"] == "once"
    assert step["preview"] == "rm -r /tmp/x"

    assert client.get("/api/v1/approvals", headers=auth(token)).json()["data"] == []
    decided = client.get("/api/v1/approvals?status=approved_once", headers=auth(token)).json()
    assert [a["request_id"] for a in decided["data"]] == [rid]
    r = client.post(f"/api/v1/approvals/{rid}", json={"choice": "deny"}, headers=auth(token))
    assert r.status_code == 409 and r.json()["detail"]["code"] == "approval_already_decided"
    r = client.post(f"/api/v1/approvals/{rid}", json={"choice": "always"}, headers=auth(token))
    assert r.status_code == 400
    r = client.post("/api/v1/approvals/nope", json={"choice": "once"}, headers=auth(token))
    assert r.status_code == 404
    assert client.post(f"/api/v1/approvals/{rid}", json={"choice": "once"}).status_code == 401
    assert len(fake.calls("POST", f"/v1/runs/{run_id}/approval")) == 1
