"""The personal profile (P1): the files, what a run is told, a chat keeping the profile it
started with, and the routes through the real app. The person in here is invented."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.attachments import NOTE_EARLIER_IMAGES, AttachmentStore
from delve.auth import ROLE_DESK, create_device
from delve.db.models import Conversation
from delve.persona import (
    BLOCK_HEAD,
    BLOCK_RULE,
    GENERAL_HEAD,
    TOPICS_HEAD,
    TOPICS_MAX,
    Persona,
    PersonaError,
    PersonaStore,
    Section,
    Topic,
    build_block,
    general_text,
    parse_profile,
    parse_topic,
    persona_out,
    render_profile,
    render_topic,
    revision,
)

from .fake_hermes import FakeHermes
from .harness import Harness, make_harness
from .test_api import auth, chat, hello
from .test_attachments import JPEG, image, run_bodies

PROFILE = """\
# Profile - Minta Bence

<!-- a note for the editor,
     never for the model -->

## Basics
- Name: Minta Bence. Lives in Győr.
- Dog: Zsömle, a vizsla.

## Style [general]
- Writes Hungarian, often without accents. Answer in Hungarian, briefly.

## Words
- "a torony" = the desktop; "a kicsi" = the laptop.
"""

RUNNING = """\
---
title: Tájfutás
summary: results, training plan, the club
general: no
---
- Club: Rába TK.
- Hungária Kupa 2026: 23rd.
"""

BOOKS = """\
---
title: Books
summary: what he reads
general: yes
---
- Scandinavian crime.
"""


def seed(root: Path) -> PersonaStore:
    (root / "topics").mkdir(parents=True)
    (root / "profile.md").write_text(PROFILE, encoding="utf-8")
    (root / "topics" / "tajfutas.md").write_text(RUNNING, encoding="utf-8")
    (root / "topics" / "books.md").write_text(BOOKS, encoding="utf-8")
    return PersonaStore(root)


# --- the files (no database) ---------------------------------------------------------------


def test_profile_sections_comments_and_the_general_mark() -> None:
    sections = parse_profile(PROFILE)
    assert [(s.title, s.general) for s in sections] == [
        ("Basics", False),
        ("Style", True),
        ("Words", False),
    ]
    assert sections[0].text == "- Name: Minta Bence. Lives in Győr.\n- Dog: Zsömle, a vizsla."
    assert all("editor" not in s.text for s in sections)
    # what is written reads back the same
    assert parse_profile(render_profile(sections)) == sections
    # text before the first heading is kept, under a name
    loose = parse_profile("He sails.\n\n## Boats\n- one\n\n## Empty\n\n")
    assert [(s.title, s.text) for s in loose] == [("About", "He sails."), ("Boats", "- one")]
    assert parse_profile("") == ()


def test_topic_front_matter() -> None:
    t = parse_topic("tajfutas", RUNNING)
    assert (t.title, t.summary, t.general) == (
        "Tájfutás",
        "results, training plan, the club",
        False,
    )
    assert t.text == "- Club: Rába TK.\n- Hungária Kupa 2026: 23rd."
    assert parse_topic("tajfutas", render_topic(t)) == t
    assert parse_topic("books", BOOKS).general is True
    bare = parse_topic("notes", "just text\n")
    assert (bare.title, bare.summary, bare.general, bare.text) == ("notes", "", False, "just text")
    # a front matter that never closes is text
    assert parse_topic("x", "---\ntitle: T\nbody").text == "---\ntitle: T\nbody"


def test_what_a_run_is_told(tmp_path: Path) -> None:
    store = seed(tmp_path)
    persona = store.load()
    block = build_block(persona, tmp_path)
    assert block.startswith(f"{BLOCK_HEAD}\n### Basics\n- Name: Minta Bence.")
    assert "### Style\n- Writes Hungarian" in block and "[general]" not in block
    core, _, index = block.partition("\n\n" + TOPICS_HEAD + "\n")
    assert core.endswith(BLOCK_RULE)
    assert index.splitlines() == [
        f"- {tmp_path}/topics/books.md: Books - what he reads",
        f"- {tmp_path}/topics/tajfutas.md: Tájfutás - results, training plan, the club",
    ]
    # the topic files' content is not in the prompt
    assert "23rd" not in block and "Scandinavian" not in block
    assert build_block(Persona(), tmp_path) == ""
    only_topics = build_block(Persona(topics=persona.topics), tmp_path)
    assert only_topics.startswith(TOPICS_HEAD) and BLOCK_HEAD not in only_topics


def test_only_the_marked_part_is_for_the_general_chats(tmp_path: Path) -> None:
    persona = seed(tmp_path).load()
    text = general_text(persona)
    assert text == "\n".join(
        [
            GENERAL_HEAD,
            "### Style",
            "- Writes Hungarian, often without accents. Answer in Hungarian, briefly.",
            "### Books",
            "- Scandinavian crime.",
            BLOCK_RULE,
        ]
    )
    for private in ("Bence", "Győr", "Zsömle", "torony", "Rába", str(tmp_path)):
        assert private not in text
    assert general_text(Persona(sections=(Section("Basics", "x"),))) == ""


def test_the_shape_for_the_app(tmp_path: Path) -> None:
    out = persona_out(seed(tmp_path).load(), tmp_path)
    assert out["available"] is True
    assert out["revision"] == revision(out["block"]) and len(out["revision"]) == 12
    assert out["block_chars"] == len(out["block"])
    assert [s["title"] for s in out["sections"]] == ["Basics", "Style", "Words"]
    assert out["topics"][0] == {
        "name": "books",
        "title": "Books",
        "summary": "what he reads",
        "text": "- Scandinavian crime.",
        "general": True,
    }
    empty = persona_out(Persona(), tmp_path)
    assert (empty["available"], empty["revision"], empty["block"]) == (False, None, "")


async def test_no_directory_is_no_profile(tmp_path: Path) -> None:
    store = PersonaStore(tmp_path / "missing")
    assert await store.read() == Persona()
    assert await store.block() == ""
    with pytest.raises(PersonaError) as e:
        await store.save_profile([Section("Basics", "x")])
    assert e.value.code == "persona_no_store"
    assert await store.delete_topic("books") is False


async def test_the_store_ignores_what_is_not_a_topic(tmp_path: Path) -> None:
    store = seed(tmp_path)
    topics = tmp_path / "topics"
    (topics / "Notes.md").write_text("capital letter")
    (topics / "notes.txt").write_text("not markdown")
    (topics / ".hidden.md").write_text("dot file")
    (topics / "dir.md").mkdir()
    (tmp_path / "elsewhere.md").write_text("outside")
    (topics / "link.md").symlink_to(tmp_path / "elsewhere.md")
    assert [t.name for t in (await store.read()).topics] == ["books", "tajfutas"]
    # an empty directory is a profile without anything
    empty = PersonaStore(tmp_path / "empty")
    (tmp_path / "empty").mkdir()
    assert await empty.read() == Persona(available=True)


async def test_saving_and_removing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    root = tmp_path / "p"
    root.mkdir()
    store = PersonaStore(root)
    with caplog.at_level(logging.DEBUG, logger="delve"):
        await store.save_profile(
            [
                Section("  Basics ", "- Dog:\tZsömle\r\n\n\n\n- Town: Győr  \n", True),
                Section("Empty", "   "),
                Section("Words", "x\x00y"),
            ]
        )
        await store.save_topic(Topic("gepek", "Machines\nhere", "what\tthey are", "48 GB\n"))
    persona = await store.read()
    assert persona.sections == (
        Section("Basics", "- Dog:\tZsömle\n\n- Town: Győr", True),
        Section("Words", "xy"),
    )
    assert persona.topics == (Topic("gepek", "Machines here", "what they are", "48 GB"),)
    assert not list(root.rglob("*.part"))
    for record in caplog.records:
        line = record.getMessage() + repr(getattr(record, "__dict__", {}))
        assert "Zsömle" not in line and "48 GB" not in line
    with pytest.raises(PersonaError) as e:
        await store.save_topic(Topic("Bad Name", "t", "", "x"))
    assert e.value.code == "invalid_topic"
    assert await store.delete_topic("gepek") is True
    assert await store.delete_topic("gepek") is False
    assert await store.delete_topic("../profile") is False
    assert (root / "profile.md").exists()
    # an emptied profile is no profile
    await store.save_profile([])
    assert await store.block() == ""


async def test_at_most_so_many_topics(tmp_path: Path) -> None:
    store = PersonaStore(tmp_path)
    for i in range(TOPICS_MAX):
        await store.save_topic(Topic(f"t{i:02d}", "t", "", "x"))
    with pytest.raises(PersonaError) as e:
        await store.save_topic(Topic("one-more", "t", "", "x"))
    assert e.value.code == "too_many_topics"
    await store.save_topic(Topic("t00", "again", "", "y"))  # replacing one is fine
    assert len((await store.read()).topics) == TOPICS_MAX


# --- a run's instructions (database) ---------------------------------------------------------


@pytest.fixture
async def hp(
    db: async_sessionmaker[AsyncSession], tmp_path: Path
) -> AsyncIterator[tuple[Harness, PersonaStore]]:
    store = seed(tmp_path / "persona")
    async with make_harness(db, persona=store.block) as h:
        await h.mgr.start()
        yield h, store


async def _pinned(h: Harness, conv_id: str) -> str | None:
    async with h.db() as s:
        conv = await s.get(Conversation, conv_id)
    assert conv is not None
    return conv.persona


async def test_a_chat_keeps_the_profile_it_started_with(
    hp: tuple[Harness, PersonaStore],
) -> None:
    h, store = hp
    for _ in range(4):
        h.fake.enqueue("protocol/a")
    first_block = await store.block()
    conv, _ = await h.send("szia")
    await h.settle()
    assert run_bodies(h.fake)[0]["instructions"] == first_block
    assert await _pinned(h, conv) == first_block

    await store.save_profile([Section("Basics", "- Name: Minta Bence. Lives in Sopron.")])
    new_block = await store.block()
    assert new_block != first_block
    await h.send("es most?", conversation_id=conv)
    await h.settle()
    assert run_bodies(h.fake)[1]["instructions"] == first_block  # the running chat: unchanged

    other, _ = await h.send("uj chat")
    await h.settle()
    assert run_bodies(h.fake)[2]["instructions"] == new_block  # a new chat: the new one
    assert await _pinned(h, other) == new_block

    # a chat that started without a profile stays without, whatever comes later
    async with h.db() as s:
        row = await s.get(Conversation, conv)
        assert row is not None
        row.persona = ""
        await s.commit()
    await h.send("regi chat", conversation_id=conv)
    await h.settle()
    assert "instructions" not in run_bodies(h.fake)[3]


async def test_without_a_profile_nothing_changes(
    db: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    store = PersonaStore(tmp_path / "missing")
    async with make_harness(db, persona=store.block) as h:
        await h.mgr.start()
        h.fake.enqueue("protocol/a")
        conv, _ = await h.send("szia")
        await h.settle()
        assert "instructions" not in run_bodies(h.fake)[0]
        assert await _pinned(h, conv) == ""
    async with make_harness(db) as h:  # a manager that was given no profile at all
        await h.mgr.start()
        h.fake.enqueue("protocol/a")
        conv, _ = await h.send("szia")
        await h.settle()
        assert "instructions" not in run_bodies(h.fake)[0]
        assert await _pinned(h, conv) is None


async def test_a_profile_that_cannot_be_read_is_asked_for_again(
    db: async_sessionmaker[AsyncSession],
) -> None:
    answers: list[str | Exception] = [OSError("disk"), "## About\n- x"]

    async def flaky() -> str:
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    async with make_harness(db, persona=flaky) as h:
        await h.mgr.start()
        h.fake.enqueue("protocol/a")
        h.fake.enqueue("protocol/a")
        conv, _ = await h.send("egy")
        await h.settle()
        assert "instructions" not in run_bodies(h.fake)[0]
        assert await _pinned(h, conv) is None  # undecided, not "none"
        await h.send("ketto", conversation_id=conv)
        await h.settle()
        assert run_bodies(h.fake)[1]["instructions"] == "## About\n- x"


async def test_the_profile_comes_before_the_notes_of_attachments(
    db: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    files = AttachmentStore(db, images_dir=tmp_path / "img", inbox_dir=tmp_path / "inbox")
    store = seed(tmp_path / "persona")
    async with make_harness(db, attachments=files, persona=store.block) as h:
        await h.mgr.start()
        h.fake.enqueue("protocol/e1")
        h.fake.enqueue("protocol/e2")
        conv, _ = await h.send("first", attachment_ids=[await image(files, JPEG)])
        await h.settle()
        await h.send("and then?", conversation_id=conv)
        await h.settle()
        first, second = run_bodies(h.fake)
        block = await store.block()
        assert first["instructions"] == block
        assert second["instructions"] == f"{block}\n\n{NOTE_EARLIER_IMAGES}"


# --- the routes ------------------------------------------------------------------------------


@pytest.fixture
def pdir(env: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = tmp_path / "persona"
    root.mkdir()
    env.setenv("DELVE_PERSONA_DIR", str(root))
    return root


def test_routes_read_and_edit(pdir: Path, client: TestClient, token: str) -> None:
    h = auth(token)
    assert client.get("/api/v1/persona").status_code == 401
    empty = client.get("/api/v1/persona", headers=h).json()
    assert empty["available"] is True and empty["sections"] == [] and empty["revision"] is None

    body = {
        "sections": [
            {"title": "Basics", "text": "- Name: Minta Bence."},
            {"title": "Style", "text": "- Answer briefly.", "general": True},
        ]
    }
    out = client.put("/api/v1/persona/profile", headers=h, json=body).json()
    assert [s["title"] for s in out["sections"]] == ["Basics", "Style"]
    assert out["general_text"].splitlines()[1:3] == ["### Style", "- Answer briefly."]
    assert "Minta Bence" in out["block"] and "Minta Bence" not in out["general_text"]
    assert (pdir / "profile.md").read_text(encoding="utf-8").startswith("# Profile\n\n## Basics\n")

    topic = {"title": "Machines", "summary": "the parts", "text": "- 48 GB RAM"}
    out = client.put("/api/v1/persona/topics/gepek", headers=h, json=topic).json()
    assert out["topics"] == [{"name": "gepek", **topic, "general": False}]
    assert f"- {pdir}/topics/gepek.md: Machines - the parts" in out["block"]
    assert client.get("/api/v1/persona", headers=h).json() == out

    assert client.delete("/api/v1/persona/topics/gepek", headers=h).json()["topics"] == []
    gone = client.delete("/api/v1/persona/topics/gepek", headers=h)
    assert gone.status_code == 404 and gone.json()["detail"]["code"] == "topic_not_found"


def test_routes_refuse(pdir: Path, client: TestClient, token: str) -> None:
    h = auth(token)
    put = client.put
    for bad in (
        {"sections": [{"title": "", "text": "x"}]},
        {"sections": [{"title": "T", "text": "x" * 4001}]},
        {"sections": [{"title": "T", "text": "x", "general": "yes"}]},
        {"sections": [{"title": "T", "text": "x", "extra": 1}]},
        {"sections": [{"title": "T", "text": "x"}] * 13},
        {},
    ):
        assert put("/api/v1/persona/profile", headers=h, json=bad).status_code == 422, bad
    topic = {"title": "T", "text": "x"}
    for name in ("Bad", "a_b", "-a", "a" * 41):
        r = put(f"/api/v1/persona/topics/{name}", headers=h, json=topic)
        assert r.status_code == 422, name
    assert put("/api/v1/persona/topics/ok", headers=h, json={"text": "x"}).status_code == 422
    assert not (pdir / "profile.md").exists() and not (pdir / "topics").exists()


async def test_the_desk_token_opens_nothing_here(
    pdir: Path, client: TestClient, db: async_sessionmaker[AsyncSession]
) -> None:
    async with db() as s:
        _, desk = await create_device(s, "desk", role=ROLE_DESK)
        await s.commit()
    assert client.get("/api/v1/persona", headers=auth(desk)).status_code == 401
    body = {"sections": [{"title": "T", "text": "x"}]}
    assert client.put("/api/v1/persona/profile", headers=auth(desk), json=body).status_code == 401


def test_routes_without_a_store(client: TestClient, token: str) -> None:
    h = auth(token)
    out = client.get("/api/v1/persona", headers=h).json()
    assert out["available"] is False and out["block"] == ""
    body = {"sections": [{"title": "T", "text": "x"}]}
    r = client.put("/api/v1/persona/profile", headers=h, json=body)
    assert r.status_code == 503 and r.json()["detail"]["code"] == "persona_no_store"


def test_a_chat_through_the_app_gets_the_profile(
    pdir: Path, client: TestClient, token: str, fake: FakeHermes
) -> None:
    body = {"sections": [{"title": "Basics", "text": "- Name: Minta Bence."}]}
    block = client.put("/api/v1/persona/profile", headers=auth(token), json=body).json()["block"]
    fake.enqueue("protocol/a")
    with client.websocket_connect("/api/v1/ws") as ws:
        hello(ws, token)
        chat(ws, "szia")
    assert run_bodies(fake)[0]["instructions"] == block
