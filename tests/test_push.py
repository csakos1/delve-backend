"""Push notifications (push.py): the FCM client against a fake Google, presence, and the
service end to end with recorded Hermes runs and a live database."""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.app import create_app
from delve.auth import create_device, set_fcm_token
from delve.config import Settings
from delve.db.models import Device, PushLog
from delve.push import (
    FcmClient,
    Presence,
    PushConfigError,
    PushService,
    ServiceAccount,
    build_push,
    clip,
    notify_settings,
    signed_assertion,
)
from delve.runs.approvals import ApprovalService

from .fake_hermes import FakeHermes, StreamingASGITransport, load_json
from .harness import FAST, Harness, final_output, sse_events

TOKEN_URI = "https://oauth2.example.test/token"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def sa_dict(**over: Any) -> dict[str, Any]:
    pem = _KEY.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return {
        "type": "service_account",
        "project_id": "delve-test",
        "private_key_id": "kid1",
        "private_key": pem,
        "client_email": "push@delve-test.iam.example.test",
        "token_uri": TOKEN_URI,
        **over,
    }


def _unb64(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


@dataclass
class FakeGoogle:
    """The OAuth token endpoint and FCM `messages:send`."""

    sends: list[dict[str, Any]] = field(default_factory=list)
    tokens_issued: int = 0
    # queued answers for the next sends: (status, FcmError code or None)
    answers: list[tuple[int, str | None]] = field(default_factory=list)
    auth_headers: list[str] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            form = dict(x.split("=", 1) for x in request.content.decode().split("&"))
            assert form["grant_type"] == "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
            self.tokens_issued += 1
            return httpx.Response(
                200, json={"access_token": f"at{self.tokens_issued}", "expires_in": 3599}
            )
        assert request.url.path == "/v1/projects/delve-test/messages:send"
        self.auth_headers.append(request.headers["Authorization"])
        if self.answers:
            status, code = self.answers.pop(0)
            if status != 200:
                details = [{"@type": "x", "errorCode": code}] if code else []
                return httpx.Response(
                    status, json={"error": {"code": status, "status": "X", "details": details}}
                )
        self.sends.append(json.loads(request.content)["message"])
        return httpx.Response(200, json={"name": "projects/delve-test/messages/1"})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def kinds(self) -> list[str]:
        return [m["data"]["kind"] for m in self.sends]

    def approvals(self) -> list[dict[str, Any]]:
        return [m for m in self.sends if m["data"]["kind"].startswith("approval")]


def client_for(google: FakeGoogle, **kw: Any) -> FcmClient:
    return FcmClient(
        ServiceAccount.from_dict(sa_dict()), transport=google.transport(), backoff_s=0.001, **kw
    )


# --- units -------------------------------------------------------------------------------


def test_notify_settings_and_clip() -> None:
    assert notify_settings(None) == {
        "approvals": True,
        "run_done": True,
        "water": False,
        "brief": False,
    }
    got = notify_settings({"run_done": False, "water": "yes", "other": True})
    assert got == {"approvals": True, "run_done": False, "water": False, "brief": False}
    assert clip("  a \n b  ", 80) == "a b"
    assert clip("x" * 100, 80) == "x" * 79 + "…"
    assert clip(None, 10) == ""


def test_service_account_errors(tmp_path: Path) -> None:
    with pytest.raises(PushConfigError, match="cannot read"):
        ServiceAccount.from_file(tmp_path / "missing.json")
    with pytest.raises(PushConfigError, match="not a service account"):
        ServiceAccount.from_dict(sa_dict(type="authorized_user"))
    with pytest.raises(PushConfigError, match="invalid service account"):
        ServiceAccount.from_dict(sa_dict(private_key="nope"))
    with pytest.raises(PushConfigError, match="project id"):
        FcmClient(ServiceAccount.from_dict(sa_dict(project_id="")))
    sa = ServiceAccount.from_dict(sa_dict())
    assert "BEGIN" not in repr(sa)  # the key never shows up in logs


def test_signed_assertion_verifies() -> None:
    sa = ServiceAccount.from_dict(sa_dict())
    jwt = signed_assertion(sa, 1_000_000)
    head, claims, sig = jwt.split(".")
    assert json.loads(_unb64(head)) == {"alg": "RS256", "typ": "JWT", "kid": "kid1"}
    assert json.loads(_unb64(claims)) == {
        "iss": "push@delve-test.iam.example.test",
        "scope": "https://www.googleapis.com/auth/firebase.messaging",
        "aud": TOKEN_URI,
        "iat": 1_000_000,
        "exp": 1_003_600,
    }
    _KEY.public_key().verify(
        _unb64(sig), f"{head}.{claims}".encode(), padding.PKCS1v15(), hashes.SHA256()
    )


async def test_fcm_send_body_and_token_cache() -> None:
    g = FakeGoogle()
    fcm = client_for(g)
    r1 = await fcm.send("t1", {"kind": "approval"}, high=True, ttl_s=90, collapse_key="a1")
    r2 = await fcm.send("t2", {"kind": "run_done"}, high=False, ttl_s=10**9)
    await fcm.aclose()
    assert r1.ok and r2.ok
    assert g.tokens_issued == 1 and g.auth_headers == ["Bearer at1", "Bearer at1"]
    assert g.sends[0] == {
        "token": "t1",
        "data": {"kind": "approval"},
        "android": {"priority": "HIGH", "ttl": "90s", "collapse_key": "a1"},
    }
    assert g.sends[1]["android"] == {"priority": "NORMAL", "ttl": f"{28 * 24 * 3600}s"}


async def test_fcm_errors() -> None:
    g = FakeGoogle()
    fcm = client_for(g)
    g.answers = [(404, "UNREGISTERED")]
    r = await fcm.send("dead", {}, high=True, ttl_s=1)
    assert (r.ok, r.unregistered) == (False, True) and r.error == "HTTP 404 UNREGISTERED"

    g.answers = [(401, None)]  # an expired access token: refresh once, then send
    assert (await fcm.send("t", {}, high=True, ttl_s=1)).ok
    assert g.tokens_issued == 2 and g.auth_headers[-1] == "Bearer at2"

    g.answers = [(503, None), (429, "QUOTA_EXCEEDED")]  # retried with backoff
    assert (await fcm.send("t", {}, high=True, ttl_s=1)).ok

    g.answers = [(500, "INTERNAL")] * 3
    r = await fcm.send("t", {}, high=True, ttl_s=1)
    assert not r.ok and not r.unregistered and r.error == "HTTP 500 INTERNAL"

    g.answers = [(400, "INVALID_ARGUMENT"), (200, None)]  # not retried
    r = await fcm.send("t", {}, high=True, ttl_s=1)
    assert not r.ok and not r.unregistered and len(g.answers) == 1
    await fcm.aclose()


def test_presence() -> None:
    now = [100.0]
    p = Presence(stale_s=60, clock=lambda: now[0])
    dev, other = uuid.uuid4(), uuid.uuid4()
    conn = p.enter(dev)
    assert p.live(dev) and not p.live(other)
    now[0] += 61
    assert not p.live(dev)  # a socket that went quiet (the phone lost its network)
    p.touch(conn)
    assert p.live(dev)
    p.leave(conn)
    assert not p.live(dev)


def test_build_push(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    sm: Any = None
    off = build_push(sm, Presence(), service_account_file=None, project_id=None, run_done_min_s=1)
    assert not off.enabled
    bad = tmp_path / "bad.json"
    bad.write_text("{}")
    broken = build_push(sm, Presence(), service_account_file=bad, project_id=None,
                        run_done_min_s=1)  # fmt: skip
    assert not broken.enabled and "push disabled" in caplog.text
    good = tmp_path / "fcm.json"
    good.write_text(json.dumps(sa_dict()))
    on = build_push(sm, Presence(), service_account_file=good, project_id="other",
                    run_done_min_s=1)  # fmt: skip
    assert on.enabled and on.fcm is not None and on.fcm.project_id == "other"


# --- the service with recorded runs -------------------------------------------------------

pg = pytest.mark.pg


@dataclass
class Rig:
    h: Harness
    google: FakeGoogle
    push: PushService
    devices: dict[str, uuid.UUID]

    async def drain(self) -> None:
        await self.h.settle()
        async with asyncio.timeout(5):
            await self.push.drain()

    async def device(self, name: str) -> Device:
        async with self.h.db() as s:
            row = await s.get(Device, self.devices[name])
            assert row is not None
            return row

    async def log(self) -> list[tuple[str, str | None, bool]]:
        async with self.h.db() as s:
            rows = (await s.execute(select(PushLog).order_by(PushLog.id))).scalars()
            return [(r.kind, r.ref_id, r.ok) for r in rows]


async def _device(db: async_sessionmaker[AsyncSession], name: str, fcm: str | None,
                  **notify: bool) -> uuid.UUID:  # fmt: skip
    async with db() as s:
        dev, _ = await create_device(s, name)
        if fcm is not None:
            await set_fcm_token(s, dev.id, fcm)
        if notify:
            dev.notify = {**notify_settings(None), **notify}
        await s.commit()
        return dev.id


@pytest.fixture
async def rig(h: Harness) -> AsyncIterator[Rig]:
    google = FakeGoogle()
    push = PushService(h.db, client_for(google), Presence(), run_done_min_s=0,
                       approval_delay_s=0.05)  # fmt: skip
    h.hub.add_listener(push.listener)
    devices = {
        "pixel": await _device(h.db, "pixel", "fcm-pixel"),
        "quiet": await _device(h.db, "quiet", "fcm-quiet", approvals=False, run_done=False),
        "no-fcm": await _device(h.db, "no-fcm", None),
    }
    await push.start()
    try:
        yield Rig(h, google, push, devices)
    finally:
        await push.close()


def _rid(name: str) -> str:
    return next(e for e in sse_events(name) if e["event"] == "approval.request")["request_id"]  # type: ignore[no-any-return]


@pg
async def test_approval_push_then_resolved(rig: Rig) -> None:
    h = rig.h
    rid = _rid("protocol/c")
    run_id = load_json("protocol/c.create.json")["run_id"]
    h.fake.enqueue(
        "protocol/c",
        pause_before_seq=2,
        approvals={rid: load_json(f"protocol/approval-once-{run_id}.json")},
    )
    conv_id, _ = await h.send("rm -r /tmp/x")
    await h.mgr.start()
    async with asyncio.timeout(5):
        while not rig.google.sends:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
    (msg,) = rig.google.sends  # only "pixel": "quiet" opted out, "no-fcm" has no token
    assert msg["token"] == "fcm-pixel"
    assert msg["android"]["priority"] == "HIGH" and msg["android"]["collapse_key"] == rid
    data = msg["data"]
    assert all(isinstance(v, str) for v in data.values())
    assert {
        k: data[k]
        for k in (
            "kind",
            "request_id",
            "conversation_id",
            "command",
            "status",
            "action_class",
            "title",
        )
    } == {
        "kind": "approval",
        "request_id": rid,
        "conversation_id": conv_id,
        "command": "rm -r /tmp/x",
        "status": "pending",
        "action_class": "delete_kill",
        "title": "rm -r /tmp/x",  # no Hermes title yet: the preview
    }
    assert data["hermes_deadline"] and data["expires_at"]

    await ApprovalService(h.db, h.hermes, h.hub).respond(rid, "once", "pixel")
    await rig.drain()
    assert [m["data"]["kind"] for m in rig.google.approvals()] == [
        "approval",
        "approval_resolved",
    ]
    assert rig.google.approvals()[1]["data"] == {
        "kind": "approval_resolved",
        "request_id": rid,
        "status": "approved_once",
    }
    assert ("approval", rid, True) in await rig.log()


@pg
async def test_approval_answered_within_the_delay_is_not_pushed(rig: Rig) -> None:
    rig.h.fake.enqueue("protocol/b")  # Hermes got a deny from elsewhere right away
    await rig.h.send("rm -r /tmp/x")
    await rig.h.mgr.start()
    await rig.drain()
    assert rig.google.approvals() == []
    assert [k for k, _, _ in await rig.log()] == ["run_done"]


@pg
async def test_expired_in_hermes_is_pushed_again(rig: Rig) -> None:
    rig.h.fake.enqueue("protocol/d")  # Hermes gives up waiting: still approvable here
    await rig.h.send()
    await rig.h.mgr.start()
    await rig.drain()
    statuses = [m["data"]["status"] for m in rig.google.approvals()]
    # the first push may already see the expired state if Hermes gave up within the delay
    assert statuses[-1] == "expired_in_hermes" and set(statuses) <= {
        "pending",
        "expired_in_hermes",
    }
    assert [m["data"]["kind"] for m in rig.google.approvals()] == ["approval"] * len(statuses)


@pg
async def test_run_done(rig: Rig) -> None:
    h = rig.h
    h.fake.enqueue("protocol/e1")
    conv_id, _ = await h.send("17*23?")
    await h.mgr.start()
    await rig.drain()
    (msg,) = rig.google.sends
    assert msg["token"] == "fcm-pixel" and msg["android"]["priority"] == "NORMAL"
    run_id = load_json("protocol/e1.create.json")["run_id"]
    assert msg["data"] == {
        "kind": "run_done",
        "conversation_id": conv_id,
        "run_id": run_id,
        "title": "17*23?",
        "status": "completed",
        "preview": clip(final_output("protocol/e1"), 120),
    }


@pg
async def test_run_done_skips_live_devices_and_short_runs(rig: Rig) -> None:
    h = rig.h
    conn = rig.push.presence.enter(rig.devices["pixel"])
    h.fake.enqueue("protocol/e1")
    await h.send()
    await h.mgr.start()
    await rig.drain()
    assert rig.google.sends == []  # the app is open: it shows the answer itself
    rig.push.presence.leave(conn)

    rig.push._run_done_min_s = 3600
    h.fake.enqueue("protocol/e1")
    await h.send()
    await rig.drain()
    assert rig.google.sends == []


@pg
async def test_unregistered_token_is_forgotten(rig: Rig) -> None:
    rig.google.answers = [(404, "UNREGISTERED")]
    rig.h.fake.enqueue("protocol/e1")
    await rig.h.send()
    await rig.h.mgr.start()
    await rig.drain()
    assert (await rig.device("pixel")).fcm_token is None
    run_id = load_json("protocol/e1.create.json")["run_id"]
    assert await rig.log() == [("run_done", run_id, False)]


@pg
async def test_revoked_device_gets_nothing(rig: Rig) -> None:
    async with rig.h.db() as s:
        await s.execute(update(Device).where(Device.name == "pixel").values(revoked=True))
        await s.commit()
    rig.h.fake.enqueue("protocol/e1")
    await rig.h.send()
    await rig.h.mgr.start()
    await rig.drain()
    assert rig.google.sends == []


@pg
async def test_preview_follows_the_conversation(h: Harness) -> None:
    h.fake.enqueue("protocol/e1")
    conv_id, _ = await h.send("  first\n question  ")
    created = next(e.data for e in h.events() if e.type == "conversation.created")
    assert created["preview"] == "first question"
    await h.mgr.start()
    await h.settle()
    last = [e.data for e in h.events() if e.type == "conversation.updated"][-1]
    assert last["id"] == conv_id
    assert last["preview"] == " ".join(final_output("protocol/e1").split())[:200]


# --- API -----------------------------------------------------------------------------------


@pytest.fixture
def push_client(
    env: pytest.MonkeyPatch,
    migrated_url: str,
    fake: FakeHermes,
    db: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> Iterator[tuple[TestClient, FakeGoogle]]:
    key = tmp_path / "fcm.json"
    key.write_text(json.dumps(sa_dict()))
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("FCM_SERVICE_ACCOUNT_FILE", str(key))
    google = FakeGoogle()
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(fake.app),
        timing=FAST,
        push_transport=google.transport(),
    )
    with TestClient(app) as c:
        yield c, google


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pg
def test_devices_me_notify(client: TestClient, token: str) -> None:
    me = client.get("/api/v1/devices/me", headers=_auth(token)).json()
    assert me["push"] is False  # no service account in the test environment
    assert me["notify"] == {"approvals": True, "run_done": True, "water": False, "brief": False}

    r = client.patch(
        "/api/v1/devices/me", headers=_auth(token), json={"notify": {"run_done": False}}
    )
    assert r.status_code == 200
    assert r.json()["notify"] == {
        "approvals": True,
        "run_done": False,
        "water": False,
        "brief": False,
    }
    r = client.patch("/api/v1/devices/me", headers=_auth(token), json={"notify": {"water": True}})
    assert r.json()["notify"]["run_done"] is False and r.json()["notify"]["water"] is True
    assert client.get("/api/v1/devices/me", headers=_auth(token)).json() == r.json()

    for bad in ({"notify": {"other": True}}, {"name": "x"}, {"notify": {"water": "yes"}}):
        r = client.patch("/api/v1/devices/me", headers=_auth(token), json=bad)
        assert r.status_code == 422, bad
    assert client.patch("/api/v1/devices/me", json={}).status_code == 401
    r = client.patch("/api/v1/devices/me", headers=_auth(token), json={})
    assert r.status_code == 200 and r.json()["notify"]["water"] is True


@pg
def test_app_wiring_and_presence(push_client: tuple[TestClient, FakeGoogle], token: str) -> None:
    c, _ = push_client
    assert c.get("/api/v1/devices/me", headers=_auth(token)).json()["push"] is True
    app: Any = c.app
    assert app.state.push.listener in app.state.hub._listeners
    device_id = uuid.UUID(c.get("/api/v1/devices/me", headers=_auth(token)).json()["id"])
    presence: Presence = app.state.presence
    with c.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "hello", "data": {"device_token": token}})
        assert ws.receive_json()["type"] == "hello.ok"
        assert presence.live(device_id)
        ws.send_json({"type": "ping", "id": 1, "data": {}})
        assert ws.receive_json()["type"] == "pong"
    for _ in range(200):  # the server notices the close
        if not presence.live(device_id):
            break
        time.sleep(0.01)
    assert not presence.live(device_id)
