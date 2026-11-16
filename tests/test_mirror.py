"""Notification mirroring (DA2): the queue between the phone and the desktop agent, the
two tokens' roles, and the routes through the real app."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.websockets import WebSocketDisconnect

from delve.auth import ROLE_DESK, authenticate, authenticate_app, create_device
from delve.mirror import DESK_FRESH_S, MirrorService

from .test_api import auth

DEVICE = uuid.UUID(int=7)


def post(key: str = "0|com.example|1|null|10001", **fields: Any) -> dict[str, Any]:
    return {
        "op": "post",
        "key": key,
        "app": "Example",
        "title": "Title",
        "text": "Text",
        "category": "msg",
        "hidden": False,
        **fields,
    }


def remove(key: str = "0|com.example|1|null|10001") -> dict[str, Any]:
    return {"op": "remove", "key": key}


# --- the queue (no database) -------------------------------------------------------------


async def test_show_then_close_reach_the_agent_in_order() -> None:
    m = MirrorService()
    assert m.accept(DEVICE, [post(), remove()]) == 2
    commands = await m.next(0)
    # the show that nobody fetched is gone; the close still goes out (an earlier post of
    # the key may be on the desktop)
    assert [c["verb"] for c in commands] == ["notify.close"]
    assert await m.next(0) == []


async def test_a_newer_post_of_a_key_replaces_the_waiting_one() -> None:
    m = MirrorService()
    m.accept(DEVICE, [post(text="one"), post(key="other", text="x"), post(text="two")])
    commands = await m.next(0)
    assert [(c["verb"], c["body"]) for c in commands] == [
        ("notify.show", "x"),
        ("notify.show", "two"),
    ]
    first = m.notification_id(DEVICE, "0|com.example|1|null|10001")
    assert commands[1]["id"] == first and commands[0]["id"] != first
    assert len(first) == 16 and int(first, 16) >= 0
    # another device's same key is another notification; another process, another id
    assert m.notification_id(uuid.UUID(int=8), "0|com.example|1|null|10001") != first
    assert MirrorService().notification_id(DEVICE, "0|com.example|1|null|10001") != first


async def test_hidden_content_goes_without_its_text() -> None:
    m = MirrorService()
    m.accept(DEVICE, [post(title="", text="Sensitive notification content hidden", hidden=True)])
    (command,) = await m.next(0)
    assert command == {
        "verb": "notify.show",
        "id": m.notification_id(DEVICE, "0|com.example|1|null|10001"),
        "app": "Example",
        "title": "",
        "body": "",
        "category": "msg",
        "hidden": True,
    }


async def test_a_waiting_poll_gets_what_arrives() -> None:
    m = MirrorService()
    waiting = asyncio.create_task(m.next(5))
    await asyncio.sleep(0.01)
    assert not waiting.done() and m.connected()
    m.accept(DEVICE, [post()])
    commands = await asyncio.wait_for(waiting, 1)
    assert [c["verb"] for c in commands] == ["notify.show"]


async def test_an_older_poll_ends_when_a_newer_one_arrives() -> None:
    m = MirrorService()
    old = asyncio.create_task(m.next(5))
    await asyncio.sleep(0.01)
    new = asyncio.create_task(m.next(5))
    assert await asyncio.wait_for(old, 1) == []  # nothing for the connection left behind
    m.accept(DEVICE, [post()])
    assert len(await asyncio.wait_for(new, 1)) == 1


async def test_commands_expire_and_the_queue_is_bounded() -> None:
    m = MirrorService(ttl_s=0.02, queue_max=3)
    m.accept(DEVICE, [post(key=f"k{i}") for i in range(5)])
    assert len(m._queue) == 3
    await asyncio.sleep(0.03)
    assert await m.next(0) == []


async def test_status_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    m = MirrorService()
    assert m.status() == {"desk": {"connected": False, "last_seen_s": None}}
    assert await m.next(0) == []
    assert m.status() == {"desk": {"connected": True, "last_seen_s": 0}}
    # two rounds without a poll: the agent is away
    monkeypatch.setattr(m, "_desk_seen", (m._desk_seen or 0.0) - DESK_FRESH_S - 1)
    assert m.status()["desk"]["connected"] is False

    waiting = asyncio.create_task(m.next(5))
    await asyncio.sleep(0.01)
    m.close()
    assert await asyncio.wait_for(waiting, 1) == []


async def test_a_stopping_server_ends_the_waiting_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("delve.mirror.STOP_CHECK_S", 0.02)
    m = MirrorService()
    stop = False
    m.stopping = lambda: stop
    waiting = asyncio.create_task(m.next(5))
    await asyncio.sleep(0.06)
    assert not waiting.done()  # several slices on, still waiting
    stop = True
    assert await asyncio.wait_for(waiting, 1) == []


async def test_nothing_of_a_notification_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    m = MirrorService()
    with caplog.at_level(logging.DEBUG, logger="delve"):
        m.accept(DEVICE, [post(title="SECRET-TITLE", text="SECRET-TEXT", app="SECRET-APP")])
        await m.next(0)
    dumped = " ".join(f"{r.getMessage()} {r.__dict__}" for r in caplog.records)
    assert "SECRET" not in dumped and "com.example" not in dumped
    assert any(r.getMessage() == "mirror events" for r in caplog.records)


# --- roles (database) --------------------------------------------------------------------


@pytest.fixture
async def desk_token(db: async_sessionmaker[AsyncSession]) -> str:
    async with db() as s:
        _, tok = await create_device(s, "desk", role=ROLE_DESK)
        await s.commit()
    return tok


@pytest.mark.pg
async def test_roles(db: async_sessionmaker[AsyncSession], token: str, desk_token: str) -> None:
    async with db() as s:
        app_identity = await authenticate(s, token)
        desk_identity = await authenticate(s, desk_token)
        assert app_identity is not None and app_identity.role == "app"
        assert desk_identity is not None and desk_identity.role == "desk"
        assert await authenticate_app(s, token) is not None
        assert await authenticate_app(s, desk_token) is None
        with pytest.raises(ValueError, match="role"):
            await create_device(s, "x", role="admin")


# --- the routes --------------------------------------------------------------------------


@pytest.mark.pg
def test_the_agents_token_opens_only_its_route(
    client: TestClient, token: str, desk_token: str
) -> None:
    for path in ("/api/v1/conversations", "/api/v1/devices/me", "/api/v1/mirror", "/api/v1/status"):
        assert client.get(path, headers=auth(desk_token)).status_code == 401, path
    r = client.post("/api/v1/mirror/phone", headers=auth(desk_token), json={"events": [post()]})
    assert r.status_code == 401
    with client.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "hello", "data": {"device_token": desk_token}})
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
        assert closed.value.code == 4401

    # and the app's token does not open the agent's
    for headers in ({}, auth(token), auth("dlv_wrong")):
        r = client.get("/api/v1/desk/next?wait=0", headers=headers)
        assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
    assert client.get("/api/v1/desk/next?wait=0", headers=auth(desk_token)).json() == {
        "commands": []
    }


@pytest.mark.pg
def test_phone_to_desk_round_trip(client: TestClient, token: str, desk_token: str) -> None:
    assert client.get("/api/v1/mirror", headers=auth(token)).json() == {
        "desk": {"connected": False, "last_seen_s": None}
    }
    r = client.post(
        "/api/v1/mirror/phone",
        headers=auth(token),
        json={"events": [post(text="hello"), post(key="b", app="Mail", category="email")]},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"accepted": 2, "desk": {"connected": False, "last_seen_s": None}}

    commands = client.get("/api/v1/desk/next?wait=0", headers=auth(desk_token)).json()["commands"]
    assert [(c["verb"], c["app"], c["body"], c["category"]) for c in commands] == [
        ("notify.show", "Example", "hello", "msg"),
        ("notify.show", "Mail", "Text", "email"),
    ]
    assert all(set(c) == {"verb", "id", "app", "title", "body", "category", "hidden"}
               for c in commands)  # fmt: skip
    assert client.get("/api/v1/mirror", headers=auth(token)).json()["desk"]["connected"] is True

    r = client.post("/api/v1/mirror/phone", headers=auth(token), json={"events": [remove()]})
    assert r.json()["accepted"] == 1 and r.json()["desk"]["connected"] is True
    assert client.get("/api/v1/desk/next?wait=0", headers=auth(desk_token)).json() == {
        "commands": [{"verb": "notify.close", "id": commands[0]["id"]}]
    }


@pytest.mark.pg
def test_phone_events_are_validated(client: TestClient, token: str) -> None:
    def send(events: Any) -> int:
        r = client.post("/api/v1/mirror/phone", headers=auth(token), json={"events": events})
        return r.status_code

    assert send([]) == 422
    assert send([post()] * 33) == 422
    assert send([post(app=None)]) == 422  # a post names its app
    assert send([post(app="  ")]) == 422
    assert send([post(key="")]) == 422
    assert send([post(key="k" * 513)]) == 422
    assert send([post(text="x" * 2001)]) == 422
    assert send([post(category="Msg Received")]) == 422
    assert send([post(hidden="yes")]) == 422
    assert send([{**post(), "extra": 1}]) == 422
    assert send([{"op": "update", "key": "k"}]) == 422
    assert send([remove()]) == 200
    assert send([post(title=None, text=None, category=None)]) == 200
    assert client.get("/api/v1/desk/next?wait=31", headers=auth(token)).status_code == 401
