"""Attachments (B6): uploads, binding in chat.send, the Hermes run input, cleanup, REST."""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.app import create_app
from delve.attachments import (
    MAX_IMAGE_BYTES,
    NOTE_EARLIER_IMAGES,
    ONLY_ATTACHMENTS_TEXT,
    AttachmentError,
    AttachmentStore,
    attachments_preview,
    disk_name,
    display_name,
    normalize_mime,
    now,
    sniff_image,
)
from delve.config import Settings
from delve.contract import shapes
from delve.db.models import Attachment, Conversation

from .fake_hermes import FakeHermes, StreamingASGITransport
from .fake_llama import FakeLlama
from .harness import FAST, Harness, make_harness

JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 60
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 60
WEBP = b"RIFF\x00\x00\x00\x00WEBPVP8 " + b"\x00" * 40
TOO_BIG = JPEG + b"\0" * MAX_IMAGE_BYTES


def files_under(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file()]


@pytest.fixture
def store(db: async_sessionmaker[AsyncSession], tmp_path: Path) -> AttachmentStore:
    return AttachmentStore(db, images_dir=tmp_path / "img", inbox_dir=tmp_path / "inbox")


@pytest.fixture
async def ha(
    db: async_sessionmaker[AsyncSession], store: AttachmentStore
) -> AsyncIterator[Harness]:
    async with make_harness(db, attachments=store) as h:
        await h.mgr.start()
        yield h


def run_bodies(fake: FakeHermes) -> list[dict[str, Any]]:
    return [r.body for r in fake.calls("POST", "/v1/runs") if r.path == "/v1/runs"]


async def image(store: AttachmentStore, data: bytes = JPEG, name: str = "IMG_1.jpg") -> uuid.UUID:
    return uuid.UUID(
        (await store.save(kind="image", mime="image/jpeg", filename=name, data=data))["id"]
    )


async def file(
    store: AttachmentStore, data: bytes = b"hello", name: str = "report.txt"
) -> uuid.UUID:
    return uuid.UUID(
        (await store.save(kind="file", mime="text/plain", filename=name, data=data))["id"]
    )


# --- pure helpers ------------------------------------------------------------------


def test_sniff_image() -> None:
    assert sniff_image("image/jpeg", JPEG[:16])
    assert sniff_image("image/png", PNG[:16])
    assert sniff_image("image/webp", WEBP[:16])
    assert not sniff_image("image/png", JPEG[:16])
    assert not sniff_image("image/gif", b"GIF89a")


def test_names_and_types() -> None:
    assert display_name("../../etc/passwd", fallback="f") == "passwd"
    assert display_name("C:\\Users\\a\\jelentés.pdf", fallback="f") == "jelentés.pdf"
    assert display_name("bad\x00\x1bname.txt", fallback="f") == "badname.txt"
    assert display_name("...", fallback="file") == "file"
    assert display_name(None, fallback="image.jpg") == "image.jpg"
    long = display_name("a" * 300 + ".pdf", fallback="f")
    assert len(long) == 120 and long.endswith(".pdf")
    assert disk_name("my report (v2).pdf") == "my_report_v2_.pdf"
    assert disk_name("$(rm -rf).txt") == "rm_-rf_.txt"
    assert disk_name("árvíztűrő tükör.txt") == "árvíztűrő_tükör.txt"
    assert normalize_mime("Text/Plain; charset=utf-8") == "text/plain"
    assert normalize_mime("not a mime") == "application/octet-stream"
    assert normalize_mime(None) == "application/octet-stream"


# --- the store ---------------------------------------------------------------------


async def test_save_image_and_file(store: AttachmentStore, tmp_path: Path) -> None:
    img = await store.save(kind="image", mime="image/jpeg", filename="IMG_1.jpg", data=JPEG)
    assert img["kind"] == "image" and img["mime"] == "image/jpeg"
    assert img["message_id"] is None and img["available"] is True
    assert img["size_bytes"] == len(JPEG)
    assert (tmp_path / "img" / f"{uuid.UUID(img['id']).hex}.jpg").read_bytes() == JPEG
    f = await store.save(kind="file", mime="text/plain", filename="my notes.txt", data=b"x")
    assert f["filename"] == "my notes.txt"
    assert (tmp_path / "inbox" / uuid.UUID(f["id"]).hex / "my_notes.txt").read_bytes() == b"x"
    assert set(img) == set(shapes()["attachment"])


@pytest.mark.parametrize(
    ("kw", "code"),
    [
        ({"kind": "image", "mime": "image/gif", "data": b"GIF89a"}, "unsupported_image"),
        ({"kind": "image", "mime": "image/png", "data": JPEG}, "unsupported_image"),
        ({"kind": "image", "mime": "image/jpeg", "data": TOO_BIG}, "too_large"),
        ({"kind": "file", "mime": "text/plain", "data": b""}, "empty"),
        ({"kind": "audio", "mime": "audio/ogg", "data": b"x"}, "invalid_kind"),
    ],
)  # fmt: skip
async def test_save_refuses(
    store: AttachmentStore, tmp_path: Path, kw: dict[str, Any], code: str
) -> None:
    with pytest.raises(AttachmentError) as err:
        await store.save(filename="x", **kw)
    assert err.value.code == code
    assert files_under(tmp_path) == []


async def test_copy_makes_a_new_unbound_upload(store: AttachmentStore) -> None:
    a = await image(store)
    b = await store.copy(a)
    assert b["id"] != str(a) and b["filename"] == "IMG_1.jpg" and b["message_id"] is None
    with pytest.raises(AttachmentError) as err:
        await store.copy(uuid.uuid4())
    assert err.value.code == "attachment_not_found"


def test_preview_of_attachments() -> None:
    def a(kind: str, name: str) -> Attachment:
        return Attachment(kind=kind, filename=name)

    assert attachments_preview([a("image", "x.jpg")]) == "Image"
    assert attachments_preview([a("image", "x"), a("image", "y"), a("file", "r.pdf")]) == (
        "2 images, r.pdf"
    )
    assert attachments_preview([]) is None


# --- chat.send binding and the run input --------------------------------------------


async def test_send_with_an_image_and_a_file(ha: Harness, store: AttachmentStore) -> None:
    img, doc = await image(store), await file(store)
    ha.fake.enqueue("protocol/e1")
    r = await ha.mgr.submit(
        text="Mi van a képen?", client_msg_id=uuid.uuid4(), attachment_ids=[img, doc]
    )
    assert [a["id"] for a in r.message["attachments"]] == [str(img), str(doc)]
    created = next(
        e
        for e in ha.events()
        if e.type == "message.created" and e.data["message"]["role"] == "user"
    )
    assert len(created.data["message"]["attachments"]) == 2
    await ha.settle()
    body = run_bodies(ha.fake)[0]
    content = body["input"][0]["content"]
    assert body["input"][0]["role"] == "user"
    assert content[0] == {"type": "text", "text": "Mi van a képen?"}
    url = content[1]["image_url"]["url"]
    assert base64.b64decode(url.split(",", 1)[1]) == JPEG
    assert url.startswith("data:image/jpeg;base64,")
    path = store.inbox_dir / doc.hex / "report.txt"
    assert body["instructions"] == (
        f"The user attached a file: {path} (text/plain, 5 bytes). "
        "Read it with your tools when the question needs it."
    )
    async with ha.db() as s:
        rows = list((await s.execute(select(Attachment))).scalars())
    assert {a.conversation_id for a in rows} == {r.conversation["id"]}
    assert {a.message_id for a in rows} == {r.message["id"]}


async def test_later_runs_get_the_note_and_plain_runs_stay_plain(
    ha: Harness, store: AttachmentStore
) -> None:
    ha.fake.enqueue("protocol/e1")
    ha.fake.enqueue("protocol/e2")
    ha.fake.enqueue("protocol/a")
    conv, _ = await ha.send("first", attachment_ids=[await image(store)])
    await ha.settle()
    await ha.send("what time was it?", conversation_id=conv)
    await ha.settle()
    await ha.send("unrelated")
    await ha.settle()
    first, second, third = run_bodies(ha.fake)
    assert "instructions" not in first
    assert second["input"] == "what time was it?"
    assert second["instructions"] == NOTE_EARLIER_IMAGES
    assert third["input"] == "unrelated" and "instructions" not in third


async def test_attachments_only(ha: Harness, store: AttachmentStore) -> None:
    ha.fake.enqueue("protocol/e1")
    ha.fake.enqueue("protocol/e2")
    r = await ha.mgr.submit(text="", client_msg_id=uuid.uuid4(), attachment_ids=[await file(store)])
    assert r.conversation["preview"] == "report.txt"
    r2 = await ha.mgr.submit(
        text=" ", client_msg_id=uuid.uuid4(), attachment_ids=[await image(store)]
    )
    assert r2.conversation["preview"] == "Image"
    await ha.settle()
    file_only, image_only = run_bodies(ha.fake)
    assert file_only["input"] == ONLY_ATTACHMENTS_TEXT
    assert [p["type"] for p in image_only["input"][0]["content"]] == ["image_url"]


async def test_a_missing_file_becomes_a_note(ha: Harness, store: AttachmentStore) -> None:
    img = await image(store, name="lost.jpg")
    (store.images_dir / f"{img.hex}.jpg").unlink()
    ha.fake.enqueue("protocol/e1")
    await ha.send("look", attachment_ids=[img])
    await ha.settle()
    body = run_bodies(ha.fake)[0]
    assert body["input"] == "look"
    assert body["instructions"] == "An image the user attached (lost.jpg) is no longer available."


async def test_binding_rules(ha: Harness, store: AttachmentStore) -> None:
    ha.fake.enqueue("protocol/e1")
    used = await image(store)
    await ha.send("one", attachment_ids=[used])

    async def code(ids: list[uuid.UUID]) -> str:
        with pytest.raises(AttachmentError) as err:
            await ha.send("x", attachment_ids=ids)
        return err.value.code

    assert await code([used]) == "attachment_used"
    assert await code([uuid.uuid4()]) == "attachment_not_found"
    ids = [await image(store) for _ in range(5)]
    assert await code(ids) == "too_many_attachments"
    assert await code([ids[0], ids[0]]) == "duplicate_attachment"
    async with ha.db() as s:
        await s.execute(update(Attachment).where(Attachment.id == ids[1]).values(deleted_at=now()))
        await s.commit()
    assert await code([ids[1]]) == "attachment_gone"
    # a refused send leaves nothing behind: no conversation, nothing bound
    async with ha.db() as s:
        assert len(list((await s.execute(select(Conversation))).scalars())) == 1
        free = (await s.execute(select(Attachment).where(Attachment.message_id.is_(None)))).all()
    assert len(free) == 5
    await ha.settle()


async def test_replayed_send_does_not_bind_again(ha: Harness, store: AttachmentStore) -> None:
    ha.fake.enqueue("protocol/e1")
    cid, img = uuid.uuid4(), await image(store)
    first = await ha.mgr.submit(text="x", client_msg_id=cid, attachment_ids=[img])
    again = await ha.mgr.submit(text="x", client_msg_id=cid, attachment_ids=[img])
    assert not again.created and again.message["id"] == first.message["id"]
    await ha.settle()


# --- cleanup -------------------------------------------------------------------------


async def test_cleanup(ha: Harness, store: AttachmentStore) -> None:
    ha.fake.enqueue("protocol/e1")
    stale, kept = await image(store), await image(store)
    sent_img, sent_file = await image(store), await file(store)
    await ha.send("x", attachment_ids=[sent_img, sent_file])
    await ha.settle()
    async with ha.db() as s:
        old = now() - timedelta(days=15)
        await s.execute(
            update(Attachment).where(Attachment.id.in_([stale, sent_img, sent_file]))
            .values(created_at=old)
        )  # fmt: skip
        await s.commit()
    assert await store.cleanup() == (1, 2)
    assert await store.cleanup() == (0, 0)
    async with ha.db() as s:
        rows = {a.id: a for a in (await s.execute(select(Attachment))).scalars()}
    assert stale not in rows and rows[kept].deleted_at is None
    assert rows[sent_img].deleted_at is not None and rows[sent_file].deleted_at is not None
    assert not (store.images_dir / f"{stale.hex}.jpg").exists()
    assert not (store.images_dir / f"{sent_img.hex}.jpg").exists()
    assert not (store.inbox_dir / sent_file.hex).exists()
    assert (store.images_dir / f"{kept.hex}.jpg").exists()
    # an unbound upload goes after an hour
    assert await store.cleanup(at=now() + timedelta(minutes=61)) == (1, 0)


# --- REST and WebSocket ----------------------------------------------------------------


@pytest.fixture
def api(
    env: pytest.MonkeyPatch,
    migrated_url: str,
    fake: FakeHermes,
    llama: FakeLlama,
    db: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> Iterator[TestClient]:
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("DELVE_ATTACHMENTS_DIR", str(tmp_path / "img"))
    env.setenv("DELVE_INBOX_DIR", str(tmp_path / "inbox"))
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(fake.app),
        llama_transport=StreamingASGITransport(llama.app),
        timing=FAST,
    )
    with TestClient(app) as c:
        yield c


def test_rest_upload_download_copy(api: TestClient, token: str) -> None:
    auth = {"Authorization": f"Bearer {token}"}
    r = api.post(
        "/api/v1/attachments?kind=image&filename=IMG_1.jpg",
        content=JPEG,
        headers={**auth, "Content-Type": "image/jpeg"},
    )
    assert r.status_code == 201, r.text
    att = r.json()
    got = api.get(f"/api/v1/attachments/{att['id']}", headers=auth)
    assert got.status_code == 200 and got.content == JPEG
    assert got.headers["content-type"] == "image/jpeg"
    assert got.headers["content-disposition"] == "inline; filename*=UTF-8''IMG_1.jpg"
    copied = api.post(f"/api/v1/attachments/{att['id']}/copy", headers=auth)
    assert copied.status_code == 201 and copied.json()["id"] != att["id"]
    f = api.post(
        "/api/v1/attachments?kind=file&filename=jelentés.pdf",
        content=b"%PDF-1.7",
        headers={**auth, "Content-Type": "application/pdf"},
    )
    assert f.status_code == 201 and f.json()["filename"] == "jelentés.pdf"
    got = api.get(f"/api/v1/attachments/{f.json()['id']}", headers=auth)
    assert got.headers["content-disposition"] == "attachment; filename*=UTF-8''jelent%C3%A9s.pdf"


def test_rest_errors(api: TestClient, token: str) -> None:
    auth = {"Authorization": f"Bearer {token}"}

    def post(kind: str, data: bytes, mime: str, **h: str) -> Any:
        return api.post(
            f"/api/v1/attachments?kind={kind}",
            content=data,
            headers={**auth, "Content-Type": mime, **h},
        )

    assert api.post("/api/v1/attachments?kind=image", content=JPEG).status_code == 401
    assert post("audio", b"x", "audio/ogg").status_code == 422
    too_big = post("image", TOO_BIG, "image/jpeg")
    assert too_big.status_code == 413 and too_big.json()["detail"]["code"] == "too_large"
    bad = post("image", b"GIF89a....", "image/gif")
    assert bad.status_code == 415 and bad.json()["detail"]["code"] == "unsupported_image"
    missing = api.get(f"/api/v1/attachments/{uuid.uuid4()}", headers=auth)
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "attachment_not_found"
    assert api.get("/api/v1/attachments/not-a-uuid", headers=auth).status_code == 422


def test_ws_send_and_history(api: TestClient, token: str, fake: FakeHermes) -> None:
    auth = {"Authorization": f"Bearer {token}"}
    att = api.post(
        "/api/v1/attachments?kind=image&filename=a.jpg",
        content=JPEG,
        headers={**auth, "Content-Type": "image/jpeg"},
    ).json()
    fake.enqueue("protocol/e1")
    with api.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "hello", "data": {"device_token": token}})
        assert ws.receive_json()["type"] == "hello.ok"
        ws.send_json(
            {
                "type": "chat.send",
                "id": "s",
                "data": {"text": "", "client_msg_id": str(uuid.uuid4()),
                         "attachment_ids": [att["id"]]},
            }
        )  # fmt: skip
        while True:
            frame = ws.receive_json()
            if frame.get("id") == "s":
                break
        assert frame["type"] == "ack", frame
        conv = frame["data"]["conversation_id"]
        ws.send_json(
            {"type": "chat.send", "id": "t",
             "data": {"text": "x", "client_msg_id": str(uuid.uuid4()),
                      "attachment_ids": [att["id"]]}}
        )  # fmt: skip
        while True:
            frame = ws.receive_json()
            if frame.get("id") == "t":
                break
        assert frame["type"] == "error" and frame["data"]["code"] == "attachment_used"
    msgs = api.get(f"/api/v1/conversations/{conv}/messages", headers=auth).json()["data"]
    user = next(m for m in msgs if m["role"] == "user")
    assert [a["id"] for a in user["attachments"]] == [att["id"]]
    assert user["attachments"][0]["message_id"] == user["id"]
    assistant = [m for m in msgs if m["role"] == "assistant"]
    assert all(m["attachments"] == [] for m in assistant)
    assert json.dumps(user["attachments"][0])  # plain JSON
