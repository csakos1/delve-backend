"""News (N1): the collector's batches in, the feed list out, duplicates and release days,
the Dashboard block, the lists, one story, read / saved / feedback, topics, feeds,
settings, import / export, retention, and the REST routes."""

from __future__ import annotations

import asyncio
import io
import json
import os
import stat
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve import cli
from delve.db.models import NewsBatch, NewsItem, NewsSource
from delve.events import EventHub
from delve.llama import LlamaClient
from delve.news import (
    InvalidBatch,
    NewsError,
    NewsService,
    check_settings,
    check_source,
    check_topic,
    fold_title,
    parse_batch,
    release_title,
    source_id_for,
)
from delve.news_model import (
    Candidate,
    ModelAnswerError,
    TopicRef,
    Why,
    content_of,
    group_body,
    parse_groups,
    parse_scores,
    parse_whys,
    score_body,
    system_prompt,
    why_body,
)

from .fake_hermes import StreamingASGITransport
from .fake_llama import FakeLlama

pg = pytest.mark.pg
NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)

CONFIG: dict[str, Any] = {
    "v": 1,
    "topics": [
        {"id": "orienteering", "name": "Orienteering", "description": "Races and maps."},
        {"id": "tech", "name": "Tech", "description": "Big launches."},
        {"id": "off", "name": "Off", "enabled": False},
    ],
    "sources": [
        {"id": "o-news", "topic": "orienteering", "url": "https://o.example/rss"},
        {"id": "t-news", "topic": "tech", "url": "https://t.example/feed", "every_min": 120},
        {
            "id": "tool",
            "topic": "tech",
            "url": "https://github.com/acme/tool/releases.atom",
            "kind": "release",
            "window_h": 168,
        },
        {"id": "hidden", "topic": "tech", "url": "https://h.example/rss", "enabled": False},
        {"id": "in-off", "topic": "off", "url": "https://x.example/rss"},
    ],
    "settings": {"setup": "An invented reader's laptop.", "digest_at": "06:30"},
}


def key(n: int) -> str:
    return f"{n:032x}"


def iso(t: datetime) -> str:
    return t.isoformat().replace("+00:00", "Z")


def item(n: int, source: str = "o-news", title: str | None = None, **kw: Any) -> dict[str, Any]:
    return {
        "key": key(n),
        "source": source,
        "url": kw.pop("url", f"https://{source}.example/{n}"),
        "title": title or f"Story number {n} about something",
        "publisher": kw.pop("publisher", "Pub"),
        "published": kw.pop("published", iso(NOW - timedelta(minutes=n))),
        "summary": kw.pop("summary", "Short."),
        "gn": kw.pop("gn", False),
        "kind": kw.pop("kind", "feed"),
        **kw,
    }


def report(sid: str, status: str = "ok", **kw: Any) -> dict[str, Any]:
    return {
        "id": sid,
        "status": status,
        "http": kw.pop("http", 200),
        "items": 3,
        "new": 1,
        "error_code": None,
        "error": None,
        "ms": 10,
        **kw,
    }


def batch(
    items: list[dict[str, Any]], reports: list[dict[str, Any]] | None = None, at: datetime = NOW
) -> dict[str, Any]:
    return {"v": 1, "at": iso(at), "run_s": 1.0, "sources": reports or [], "items": items}


# --- pure parts ---------------------------------------------------------------------------


def test_fold_and_release_titles() -> None:
    assert fold_title("Árvíztűrő  TÜKÖRFÚRÓ-gép: 2.0!") == "arvizturo tukorfuro gep 2 0"
    one = [{"tag": "b2"}]
    assert release_title("tool", one) == "tool: b2"
    assert release_title("tool", [{"tag": "b3"}, {"tag": "b2"}, {"tag": "b1"}]) == (
        "tool: 3 releases (b1 - b3)"
    )
    assert source_id_for("https://www.reddit.com/r/x/top/.rss?t=day") == "reddit.com-r"
    assert source_id_for("https://telex.hu/rss") == "telex.hu-rss"


def test_parse_batch() -> None:
    raw = batch(
        [
            item(1),
            item(2, url="ftp://x"),
            item(3, kind="page"),
            {"key": "no"},
            "x",
            item(4, title="  "),
            item(5, gn="yes"),
        ],
        [
            report("o-news"),
            {"id": "x", "status": "weird"},
            report("t-news", "error", error_code="http_404", error="HTTP 404"),
        ],
    )
    b = parse_batch(json.dumps(raw).encode())
    assert [i.key for i in b.items] == [key(1)]
    assert b.skipped == 6
    assert [r["id"] for r in b.reports] == ["o-news", "t-news"]
    assert b.items[0].published == NOW - timedelta(minutes=1)
    for bad in (b"[]", b"{}", b'{"v": 1, "at": "x", "sources": [], "items": []}', b"nope"):
        with pytest.raises(InvalidBatch):
            parse_batch(bad)


def test_checks() -> None:
    assert check_topic({"id": "local_ai", "name": "Local AI"}) == {
        "id": "local_ai",
        "name": "Local AI",
    }
    assert check_source({"url": "https://a.example/rss", "topic": "tech"})["id"] == (
        "a.example-rss"
    )
    for bad in ({"id": "Local", "name": "x"}, {"id": "a", "name": ""}, {"id": "a", "x": 1}):
        with pytest.raises(NewsError):
            check_topic(bad)
    for bad in (
        {"url": "ftp://a.example/", "topic": "t"},
        {"url": "https://u:p@a.example/rss", "topic": "t"},
        {"url": "https://a.example/rss", "topic": "t", "every_min": 5},
        {"url": "https://a.example/rss", "topic": "t", "kind": "page"},
        {"url": "https://a.example/rss"},
    ):
        with pytest.raises(NewsError):
            check_source(bad)
    with pytest.raises(NewsError):
        check_source({"url": "https://b.example/"}, partial=True)
    with pytest.raises(NewsError):
        check_settings({"digest_at": "7:00"})
    assert check_settings({"push_max": 0, "push": False}) == {"push_max": 0, "push": False}


# --- the service ----------------------------------------------------------------------------


class Env:
    def __init__(self, db: async_sessionmaker[AsyncSession], tmp: Path) -> None:
        self.db = db
        self.hub = EventHub(validate=True)
        self.news_dir = tmp / "news"
        self.news_dir.mkdir()
        self.batch_dir = tmp / "batches"
        self.batch_dir.mkdir()
        self.now = NOW
        self.svc = NewsService(
            db,
            self.hub,
            news_dir=self.news_dir,
            batch_dir=self.batch_dir,
            poll_s=0,
            clock=lambda: self.now,
        )
        self.n = 0

    def drop(self, data: dict[str, Any] | bytes, name: str | None = None) -> str:
        self.n += 1
        name = name or f"batch-20261117T12{self.n:04d}Z-1.json"
        raw = data if isinstance(data, bytes) else json.dumps(data).encode()
        (self.batch_dir / name).write_bytes(raw)
        return name

    def sources_file(self) -> dict[str, Any]:
        return json.loads((self.news_dir / "sources.json").read_text())  # type: ignore[no-any-return]

    def events(self, type_: str) -> list[dict[str, Any]]:
        return [e.data for e in self.hub.since(0) or [] if e.type == type_]

    async def rows(self) -> list[NewsItem]:
        async with self.db() as s:
            return list((await s.execute(select(NewsItem).order_by(NewsItem.id))).scalars())


@pytest.fixture
async def env_(db: async_sessionmaker[AsyncSession], tmp_path: Path) -> AsyncIterator[Env]:
    e = Env(db, tmp_path)
    await e.svc.import_config(CONFIG)
    yield e
    await e.svc.close()


@pg
async def test_import_writes_the_feed_list(env_: Env) -> None:
    data = env_.sources_file()
    assert [s["id"] for s in data["sources"]] == ["o-news", "t-news", "tool"]  # not hidden/off
    assert data["sources"][1] == {
        "id": "t-news",
        "url": "https://t.example/feed",
        "kind": "feed",
        "window_h": 48,
        "every_min": 120,
    }
    assert stat.S_IMODE(os.stat(env_.news_dir / "sources.json").st_mode) == 0o640
    assert not [p for p in os.listdir(env_.news_dir) if p.endswith(".part")]
    assert (await env_.svc.settings()) == {
        "setup": "An invented reader's laptop.",
        "digest_at": "06:30",
        "push": True,
        "push_max": 2,
    }
    exported = await env_.svc.export_config()
    assert [t["id"] for t in exported["topics"]] == ["orienteering", "tech", "off"]
    # importing the export again changes nothing; --replace drops what is not in the file
    assert await env_.svc.import_config(exported) == {"topics": 3, "sources": 5, "settings": 4}
    small = {"v": 1, "topics": [CONFIG["topics"][1]], "sources": [CONFIG["sources"][1]]}
    await env_.svc.import_config(small, replace=True)
    assert [s["id"] for s in await env_.svc.sources()] == ["t-news"]
    assert [s["id"] for s in env_.sources_file()["sources"]] == ["t-news"]


@pg
async def test_import_refuses_before_writing(env_: Env) -> None:
    before = await env_.svc.export_config()
    bad = [
        {"v": 1, "sources": [{"topic": "nope", "url": "https://n.example/rss"}]},
        {"v": 1, "sources": [{"id": "a", "topic": "tech", "url": "https://o.example/rss"}]},
        {"v": 1, "topics": [{"id": "x", "name": "X"}, {"id": "x", "name": "Y"}]},
        {"v": 2},
        {"v": 1, "extra": 1},
    ]
    for data in bad:
        with pytest.raises(NewsError):
            await env_.svc.import_config(data)
    assert await env_.svc.export_config() == before


@pg
async def test_a_batch_comes_in_once(env_: Env) -> None:
    name = env_.drop(
        batch(
            [item(1), item(2, "t-news")],
            [report("o-news"), report("t-news", "not_modified", http=304)],
        )
    )
    assert await env_.svc.ingest() == 2
    assert await env_.svc.ingest() == 0  # the same file is not read again
    rows = await env_.rows()
    assert [(r.key, r.source_id, r.topic_id) for r in rows] == [
        (key(1), "o-news", "orienteering"),
        (key(2), "t-news", "tech"),
    ]
    assert rows[0].seen_at == NOW and rows[0].excerpt == "Short."
    assert env_.events("news.updated") == [{"new": 2, "updated_at": NOW.isoformat()}]
    async with env_.db() as s:
        b = await s.get(NewsBatch, name)
        o, t = await s.get(NewsSource, "o-news"), await s.get(NewsSource, "t-news")
    assert b is not None and (b.items, b.new, b.error) == (2, 2, None)
    assert o is not None and (o.last_status, o.last_items, o.last_new, o.last_ok_at) == (
        "ok",
        3,
        1,
        NOW,
    )
    assert t is not None and (t.last_status, t.last_http, t.last_items) == (
        "not_modified",
        304,
        None,
    )


@pg
async def test_failures_and_refused_batches(env_: Env) -> None:
    for i in range(3):
        env_.drop(
            batch(
                [],
                [report("o-news", "error", http=403, error_code="http_403", error="HTTP 403")],
                at=NOW + timedelta(hours=i),
            )
        )
    env_.drop(b"{broken")
    env_.drop(b"{}", name="not-a-batch.json")  # other names are not read
    await env_.svc.ingest()
    async with env_.db() as s:
        o = await s.get(NewsSource, "o-news")
        refused = await s.execute(select(NewsBatch).where(NewsBatch.error.is_not(None)))
        assert len(refused.scalars().all()) == 1
    assert o is not None and (o.errors_in_row, o.last_error_code) == (3, "http_403")
    assert (await env_.svc.digest())["failing"] == 1
    env_.drop(batch([], [report("o-news")], at=NOW + timedelta(hours=5)))
    env_.drop(batch([], [report("o-news", "error")], at=NOW - timedelta(hours=5)))  # read late
    await env_.svc.ingest()
    async with env_.db() as s:
        o = await s.get(NewsSource, "o-news")
    assert o is not None and (o.errors_in_row, o.last_error_code, o.last_status) == (0, None, "ok")


@pg
async def test_items_of_removed_or_hidden_feeds_are_dropped(env_: Env) -> None:
    env_.drop(batch([item(1, "hidden"), item(2, "gone"), item(3)]))
    assert await env_.svc.ingest() == 1
    assert [r.key for r in await env_.rows()] == [key(3)]


@pg
async def test_the_same_title_is_one_story(env_: Env) -> None:
    title = "Új térkép készült a Mátrában"
    env_.drop(
        batch(
            [
                item(1, title=title),
                item(2, "t-news", title="ÚJ TÉRKÉP készült a mátrában!"),
                item(3, title="Short"),
                item(4, "t-news", title="short"),
            ]
        )
    )
    assert await env_.svc.ingest() == 3  # 1, 3 and 4 lead (a short title never matches)
    rows = await env_.rows()
    assert [r.dup_of for r in rows] == [None, rows[0].id, None, None]
    data, _ = await env_.svc.items()
    assert [(d["id"], d["sources"]) for d in data] == [
        (rows[3].id, 1),
        (rows[2].id, 1),
        (rows[0].id, 2),
    ]
    story = await env_.svc.item(rows[1].id)  # the duplicate opens its story
    assert story["id"] == rows[0].id and story["text"] is None
    assert [o["url"] for o in story["others"]] == ["https://t-news.example/2"]
    # three days later the same title is a new story
    env_.now = NOW + timedelta(days=4)
    env_.drop(batch([item(9, "t-news", title=title)], at=env_.now))
    assert await env_.svc.ingest() == 1


@pg
async def test_a_day_of_releases_is_one_row(env_: Env) -> None:
    def rel(n: int, tag: str, minutes: int) -> dict[str, Any]:
        return item(
            n,
            "tool",
            title=tag,
            kind="release",
            publisher="Release notes from tool",
            url=f"https://github.com/acme/tool/releases/tag/{tag}",
            published=iso(NOW - timedelta(minutes=minutes)),
        )

    env_.drop(batch([rel(1, "b10", 30), rel(2, "b11", 20)]))
    env_.drop(batch([rel(3, "b12", 10), rel(1, "b10", 30)], at=NOW + timedelta(minutes=5)))
    assert await env_.svc.ingest() == 1
    (row,) = await env_.rows()
    assert row.title == "tool: 3 releases (b10 - b12)"
    assert row.url.endswith("/b12") and row.publisher == "tool"
    assert [r["tag"] for r in row.releases or []] == ["b12", "b11", "b10"]
    assert row.seen_at == NOW + timedelta(minutes=5)
    # the next day is another row
    env_.drop(batch([rel(4, "b13", -24 * 60)], at=NOW + timedelta(days=1)))
    assert await env_.svc.ingest() == 1
    assert (await env_.rows())[-1].title == "tool: b13"


@pg
async def test_the_dashboard_block(env_: Env) -> None:
    assert (await env_.svc.digest()) == {
        "top": [],
        "new": 0,
        "updated_at": None,
        "state": "empty",
        "sources": 4,
        "failing": 0,
        "weekly": None,
    }
    env_.drop(batch([item(n) for n in range(1, 7)]))
    await env_.svc.ingest()
    rows = await env_.rows()
    async with env_.db() as s:  # N1b will score; here by hand
        await s.execute(update(NewsItem).where(NewsItem.id == rows[4].id).values(score=9))
        await s.execute(update(NewsItem).where(NewsItem.id == rows[5].id).values(score=5))
        await s.commit()
    await env_.svc.mark(rows[0].id, {"read": True})
    d = await env_.svc.digest()
    # best score first, then the newest; the read one is out
    assert [i["id"] for i in d["top"]] == [rows[4].id, rows[5].id, rows[1].id, rows[2].id]
    assert (d["new"], d["state"], d["updated_at"]) == (5, "ok", NOW.isoformat())
    env_.now = NOW + timedelta(hours=25)
    assert (await env_.svc.digest())["top"] == []


@pg
async def test_lists_one_story_and_marks(env_: Env) -> None:
    env_.drop(batch([item(n, "o-news" if n % 2 else "t-news") for n in range(1, 6)]))
    await env_.svc.ingest()
    rows = await env_.rows()
    page, nxt = await env_.svc.items(limit=2)
    assert [d["id"] for d in page] == [rows[4].id, rows[3].id] and nxt == rows[3].id
    page, nxt = await env_.svc.items(limit=2, before=nxt)
    assert [d["id"] for d in page] == [rows[2].id, rows[1].id]
    tech, _ = await env_.svc.items(topic="tech")
    assert {d["topic"] for d in tech} == {"tech"} and len(tech) == 2
    out = await env_.svc.mark(rows[0].id, {"saved": True, "feedback": "more", "read": True})
    assert (out["saved"], out["feedback"], out["read"]) == (True, "more", True)
    saved, _ = await env_.svc.items(saved=True)
    assert [d["id"] for d in saved] == [rows[0].id]
    out = await env_.svc.mark(rows[0].id, {"feedback": None, "read": False})
    assert (out["saved"], out["feedback"], out["read"]) == (True, None, False)
    for bad in ({}, {"feedback": "love"}, {"read": 1}, {"x": True}):
        with pytest.raises(NewsError):
            await env_.svc.mark(rows[0].id, bad)
    with pytest.raises(NewsError) as e:
        await env_.svc.item(10_000)
    assert e.value.status == 404
    # hiding a feed hides its stories (a saved one stays in Saved)
    await env_.svc.update_source("o-news", {"enabled": False})
    assert {d["source"] for d in (await env_.svc.items())[0]} == {"t-news"}
    assert len((await env_.svc.items(saved=True))[0]) == 1
    assert "o-news" not in [s["id"] for s in env_.sources_file()["sources"]]


@pg
async def test_topics_and_sources(env_: Env) -> None:
    topics = await env_.svc.topics()
    assert [(t["id"], t["sources"]) for t in topics] == [
        ("orienteering", 1),
        ("tech", 3),
        ("off", 1),
    ]
    t = await env_.svc.create_topic({"id": "sailing", "name": "Sailing", "position": 9})
    assert t == {
        "id": "sailing",
        "name": "Sailing",
        "description": "",
        "position": 9,
        "enabled": True,
        "sources": 0,
        "items_24h": 0,
    }
    with pytest.raises(NewsError) as e:
        await env_.svc.create_topic({"id": "sailing", "name": "Again"})
    assert e.value.status == 409
    src = await env_.svc.create_source({"url": "https://s.example/rss", "topic": "sailing"})
    assert (src["id"], src["origin"], src["kind"], src["window_h"]) == (
        "s.example-rss",
        "app",
        "feed",
        48,
    )
    with pytest.raises(NewsError):
        await env_.svc.create_source(
            {"url": "https://s.example/rss", "topic": "tech", "id": "other"}
        )
    with pytest.raises(NewsError):
        await env_.svc.create_source({"url": "https://z.example/rss", "topic": "nope"})
    assert "s.example-rss" in [s["id"] for s in env_.sources_file()["sources"]]
    env_.drop(batch([item(1, "s.example-rss")]))
    await env_.svc.ingest()
    # moving a feed moves its stories; deleting a topic deletes its feeds, the stories stay
    await env_.svc.update_source("s.example-rss", {"topic": "tech", "every_min": 240})
    assert (await env_.rows())[0].topic_id == "tech"
    await env_.svc.update_source("s.example-rss", {"topic": "sailing"})
    await env_.svc.update_topic("sailing", {"name": "Sailboats", "enabled": False})
    assert "s.example-rss" not in [s["id"] for s in env_.sources_file()["sources"]]
    assert (await env_.svc.items())[0] == []  # a disabled topic's stories are not listed
    await env_.svc.delete_topic("sailing")
    assert "s.example-rss" not in [s["id"] for s in await env_.svc.sources()]
    row = (await env_.rows())[0]
    assert (row.topic_id, row.source_id) == (None, None)
    for call in (
        env_.svc.delete_topic("sailing"),
        env_.svc.delete_source("nope"),
        env_.svc.update_topic("nope", {"name": "x"}),
    ):
        with pytest.raises(NewsError):
            await call


@pg
async def test_settings_and_retention(env_: Env) -> None:
    out = await env_.svc.update_settings({"push": False, "push_max": 1})
    assert out == {
        "setup": "An invented reader's laptop.",
        "digest_at": "06:30",
        "push": False,
        "push_max": 1,
    }
    with pytest.raises(NewsError):
        await env_.svc.update_settings({"theme": "dark"})
    env_.drop(batch([item(1), item(2)]))
    await env_.svc.ingest()
    rows = await env_.rows()
    await env_.svc.mark(rows[1].id, {"saved": True})
    env_.now = NOW + timedelta(days=31)
    assert await env_.svc.cleanup() == 1
    assert [r.id for r in await env_.rows()] == [rows[1].id]


@pg
async def test_the_feed_list_without_its_directory(
    db: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    svc = NewsService(db, EventHub(), news_dir=tmp_path / "missing", batch_dir=tmp_path)
    assert await svc.write_sources() is False
    assert await svc.ingest() == 0  # no batches there either


@pg
async def test_the_loop(env_: Env) -> None:
    env_.drop(batch([item(1)]))
    svc = NewsService(
        env_.db,
        env_.hub,
        news_dir=env_.news_dir,
        batch_dir=env_.batch_dir,
        poll_s=0.05,
        clock=lambda: env_.now,
    )
    (env_.news_dir / "sources.json").unlink()
    await svc.start()
    try:
        for _ in range(100):
            if await env_.rows() and (env_.news_dir / "sources.json").exists():
                break
            await asyncio.sleep(0.05)
    finally:
        await svc.close()
    assert [r.key for r in await env_.rows()] == [key(1)]
    assert (env_.news_dir / "sources.json").exists()


# --- the CLI --------------------------------------------------------------------------------


def test_cli_import_and_export(
    migrated_url: str,
    db: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DELVE_DATABASE_URL", migrated_url)
    monkeypatch.setenv("DELVE_NEWS_DIR", str(tmp_path))
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(CONFIG)))
    assert cli.main(["news", "import"]) == 0
    assert "3 topics, 5 sources, 2 settings imported; sources.json written" in (
        capsys.readouterr().err
    )
    assert len(json.loads((tmp_path / "sources.json").read_text())["sources"]) == 3
    assert cli.main(["news", "export"]) == 0
    exported = json.loads(capsys.readouterr().out)
    assert exported["settings"]["digest_at"] == "06:30" and len(exported["sources"]) == 5
    monkeypatch.setattr("sys.stdin", io.StringIO('{"v": 1, "sources": [{"url": "x"}]}'))
    assert cli.main(["news", "import"]) == 2
    monkeypatch.setattr("sys.stdin", io.StringIO("not json"))
    assert cli.main(["news", "import"]) == 2


# --- REST --------------------------------------------------------------------------------------


def auth(tok: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture
def ndirs(env: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    news, batches = tmp_path / "news", tmp_path / "batches"
    news.mkdir()
    batches.mkdir()
    env.setenv("DELVE_NEWS_DIR", str(news))
    env.setenv("DELVE_NEWS_BATCH_DIR", str(batches))
    yield news, batches


def test_routes(ndirs: tuple[Path, Path], client: TestClient, token: str) -> None:
    h = auth(token)
    news_dir, batch_dir = ndirs
    assert client.get("/api/v1/news").status_code == 401
    assert client.get("/api/v1/news", headers=h).json()["state"] == "empty"
    r = client.post("/api/v1/news/topics", headers=h, json={"id": "tech", "name": "Tech"})
    assert r.status_code == 201 and r.json()["id"] == "tech"
    assert client.post("/api/v1/news/topics", headers=h, json={"id": "tech", "name": "x"}).json()[
        "detail"
    ]["code"] == ("news_topic_exists")
    r = client.post(
        "/api/v1/news/sources",
        headers=h,
        json={"url": "https://t.example/feed", "topic": "tech", "id": "t-news"},
    )
    assert r.status_code == 201
    assert json.loads((news_dir / "sources.json").read_text())["sources"][0]["id"] == "t-news"
    bad = client.post("/api/v1/news/sources", headers=h, json={"url": "nope", "topic": "tech"})
    assert (bad.status_code, bad.json()["detail"]["code"]) == (422, "invalid_url")
    # a batch, read by hand (the poll loop is off in the tests)
    (batch_dir / "batch-20261117T120000Z-1.json").write_text(
        json.dumps(batch([item(1, "t-news", published=None)]))
    )
    assert client.portal is not None
    assert client.portal.call(client.app.state.news.ingest) == 1  # type: ignore[attr-defined]
    d = client.get("/api/v1/news", headers=h).json()
    assert d["new"] == 1 and d["top"][0]["title"] == "Story number 1 about something"
    iid = d["top"][0]["id"]
    items = client.get("/api/v1/news/items?topic=tech&limit=10", headers=h).json()
    assert [i["id"] for i in items["data"]] == [iid] and items["next_before"] is None
    assert client.get("/api/v1/news/items?topic=BAD", headers=h).status_code == 422
    story = client.patch(f"/api/v1/news/items/{iid}", headers=h, json={"saved": True}).json()
    assert story["saved"] is True and story["others"] == [] and story["text"] is None
    assert client.get(f"/api/v1/news/items/{iid}", headers=h).json()["saved"] is True
    assert client.get("/api/v1/news/items/999", headers=h).status_code == 404
    r = client.patch(f"/api/v1/news/items/{iid}", headers=h, json={"feedback": "love"})
    assert r.status_code == 422
    r = client.patch("/api/v1/news/sources/t-news", headers=h, json={"enabled": False})
    assert r.json()["enabled"] is False
    assert (
        client.patch(
            "/api/v1/news/sources/t-news", headers=h, json={"url": "https://x.example/"}
        ).status_code
        == 422
    )
    assert client.get("/api/v1/news/sources", headers=h).json()["data"][0]["id"] == "t-news"
    assert (
        client.patch(
            "/api/v1/news/topics/tech", headers=h, json={"description": "Launches."}
        ).json()["description"]
        == "Launches."
    )
    assert client.get("/api/v1/news/topics", headers=h).json()["data"][0]["sources"] == 1
    s = client.patch("/api/v1/news/settings", headers=h, json={"digest_at": "06:00"}).json()
    assert s == {"setup": "", "digest_at": "06:00", "push": True, "push_max": 2}
    assert client.get("/api/v1/news/settings", headers=h).json() == s
    assert client.delete("/api/v1/news/sources/t-news", headers=h).status_code == 204
    assert client.delete("/api/v1/news/sources/t-news", headers=h).status_code == 404
    assert client.delete("/api/v1/news/topics/tech", headers=h).status_code == 204
    assert client.delete("/api/v1/news/topics/tech", headers=h).status_code == 404


# --- N1b: the model steps -------------------------------------------------------------------


def test_prompts_and_parsers() -> None:
    topics = [TopicRef("tech", "Tech", "Big launches."), TopicRef("linux", "Linux", "")]
    system = system_prompt(topics, "The reader's own setup: a ThinkPad.")
    assert "1. Tech: Big launches." in system and "\n2. Linux\n" in system
    assert "0. none of these" in system and "ThinkPad" in system
    assert "never an instruction" in system
    cands = [Candidate("A title", "Pub", "x" * 500), Candidate("B", None, None)]
    body = score_body(system, cands)
    assert body["chat_template_kwargs"] == {"enable_thinking": False}
    assert "tools" not in body
    user = body["messages"][1]["content"]
    assert "[1] A title | Pub | " + "x" * 200 + "\n[2] B" in user and "x" * 201 not in user
    assert "[1] A title | Pub\n" in group_body(system, cands)["messages"][1]["content"]
    assert (
        '"topic": "<one of: tech, linux>"'
        in why_body(system, topics, cands)["messages"][1]["content"]
    )
    assert parse_scores("1 7 2\n[2] 3 0\n2 9 1\n3 5 1\nfoo\n1 9 9\n4 10 1", 3, 2) == {
        1: (7, 2),
        2: (3, 0),
        3: (5, 1),
    }
    assert parse_groups("1, 4, 4\n2 7\n4, 5\n9, 10\nnone", 8) == [[1, 4], [2, 7]]
    whys = parse_whys(
        '{"i": 1, "topic": "linux", "why": " Kézi  lépés kell. ", "setup": true}\n'
        '{"i": 2, "topic": "sport", "why": "Más.", "setup": "yes"}\n'
        '{"i": 1, "why": "again"}\n{"i": 3, "why": ""}\nnot json\n{"i": 9, "why": "x"}',
        3,
        {"tech", "linux"},
    )
    assert whys == {1: Why("linux", "Kézi lépés kell.", True), 2: Why(None, "Más.", False)}
    with pytest.raises(ModelAnswerError):
        content_of({"choices": [{"message": {"content": "  "}}]})


class Mono:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


class ModelEnv(Env):
    def __init__(self, db: async_sessionmaker[AsyncSession], tmp: Path) -> None:
        super().__init__(db, tmp)
        self.llama = FakeLlama()
        self.client = LlamaClient("http://llama", transport=StreamingASGITransport(self.llama.app))
        self.mono = Mono()
        self.pushed: list[int] = []
        self.svc = NewsService(
            db,
            self.hub,
            news_dir=self.news_dir,
            batch_dir=self.batch_dir,
            poll_s=0,
            llama=self.client,
            notify=self.pushed.append,
            idle_s=180,
            clock=lambda: self.now,
            monotonic=self.mono,
        )
        self.answers: list[str] = []

    def answer(self, body: dict[str, Any]) -> str:
        return self.answers.pop(0)

    async def step(self) -> bool:
        """One work() call, with llama idle long enough before it."""
        await self.svc._llama_idle()  # sees the counters
        self.mono.t += 200
        return await self.svc.work()

    async def by_title(self) -> dict[str, NewsItem]:
        return {r.title: r for r in await self.rows()}


@pytest.fixture
async def menv(db: async_sessionmaker[AsyncSession], tmp_path: Path) -> AsyncIterator[ModelEnv]:
    e = ModelEnv(db, tmp_path)
    e.llama.reply = e.answer
    await e.svc.import_config(CONFIG)
    yield e
    await e.client.aclose()


@pg
async def test_llama_must_be_idle_for_a_while(menv: ModelEnv) -> None:
    menv.drop(batch([item(1)]))
    await menv.svc.ingest()
    menv.answers = ["1 6 2"]
    assert await menv.svc.work() is False  # just started: llama counts as busy
    menv.mono.t += 100
    assert await menv.svc.work() is False  # not long enough
    menv.llama.add(tokens_predicted_total=50)  # somebody (opencode) used it
    menv.mono.t += 100
    assert await menv.svc.work() is False
    menv.mono.t += 179
    assert await menv.svc.work() is False
    menv.llama.set(task=9, processing=True, tokens=10)
    menv.mono.t += 500
    assert await menv.svc.work() is False  # generating right now
    menv.llama.set(task=9, processing=False, tokens=10)
    assert await menv.svc.work() is False
    menv.mono.t += 180
    assert await menv.svc.work() is True
    assert menv.llama.chats and (await menv.rows())[0].score == 6
    assert await menv.svc.work() is False  # nothing left; our own call did not count


@pg
async def test_scores_topics_and_retries(menv: ModelEnv) -> None:
    menv.drop(batch([item(n, "o-news" if n < 3 else "t-news") for n in range(1, 5)]))
    await menv.svc.ingest()
    # one batch, the newest id first: the prompt's [1] is item 4, [4] is item 1
    menv.answers = ["1 7 1\n3 3 0\n4 8 2"]  # item 3 left out; topic 2 = tech
    assert await menv.step() is True
    body = menv.llama.chats[0]
    assert "1. Orienteering: Races and maps." in body["messages"][0]["content"]
    assert "An invented reader's laptop." in body["messages"][0]["content"]
    rows = await menv.rows()
    assert [(r.score, r.topic_id, r.score_attempts) for r in rows] == [
        (8, "tech", 0),
        (3, "orienteering", 0),
        (None, "tech", 1),
        (7, "orienteering", 0),
    ]
    # item 3 again, twice left out: it gives up with 0
    menv.answers = ["nothing useful", "1 5 1"]
    assert await menv.step() is False  # no line at all: a failed step, backoff
    assert (await menv.rows())[2].score_attempts == 2
    menv.mono.t += 61
    assert await menv.step() is True
    assert (await menv.rows())[2].score == 5
    # llama down: the try counts, the service waits
    menv.drop(batch([item(9)]))
    await menv.svc.ingest()
    menv.llama.chat_status = 503
    assert await menv.step() is False
    assert (await menv.rows())[-1].score_attempts == 1


@pg
async def test_grouping_merges_the_same_event(menv: ModelEnv) -> None:
    titles = [
        "Bridge hit in the night",
        "Éjjel találat érte a hidat",
        "Other bridge hit",
        "A new GPU",
        "Release day",
    ]
    menv.drop(batch([item(n, title=t) for n, t in enumerate(titles, 1)]))
    await menv.svc.ingest()
    menv.answers = ["1 2 2\n2 9 2\n3 6 1\n4 7 1\n5 8 1"]  # [1] is item 5
    assert await menv.step() is True
    menv.answers = []
    assert await menv.step() is False  # 4 new >= 5: fewer than 8, and they have not waited
    menv.now = NOW + timedelta(minutes=21)
    menv.answers = ["1, 2\n3, 9\n", '{"i": 1, "why": "x"}']
    assert await menv.step() is True
    user = menv.llama.chats[-1]["messages"][1]["content"]
    assert "same specific event" in user and "Release day" not in user  # score 2: not asked
    t = await menv.by_title()
    lead, dup = t["Bridge hit in the night"], t["Éjjel találat érte a hidat"]
    assert dup.dup_of == lead.id and lead.score == 8 and t["Other bridge hit"].dup_of is None
    assert all(r.grouped_at is not None for r in t.values() if (r.score or 0) >= 5)
    data, _ = await menv.svc.items()
    assert {d["title"]: d["sources"] for d in data}["Bridge hit in the night"] == 2
    # a later item joins the known story; the better score moves to the lead
    menv.now += timedelta(minutes=30)
    menv.drop(batch([item(7, title="Bridge strike: more details")], at=menv.now))
    await menv.svc.ingest()
    menv.answers = ["1 9 1"]
    assert await menv.step() is True
    menv.now += timedelta(minutes=21)
    # the new one first in the prompt, then the known stories best first: GPU, bridge, other
    menv.answers = ["3, 1"]
    assert await menv.step() is True
    t = await menv.by_title()
    lead = t["Bridge hit in the night"]
    assert t["Bridge strike: more details"].dup_of == lead.id and lead.score == 9
    assert [o["title"] for o in (await menv.svc.item(lead.id))["others"]] == [
        "Éjjel találat érte a hidat",
        "Bridge strike: more details",
    ]


@pg
async def test_whys_setup_and_push(menv: ModelEnv) -> None:
    menv.now = datetime(2026, 11, 17, 8, 0, tzinfo=UTC)  # 09:00 in Budapest
    rel = item(
        1,
        "tool",
        title="b10",
        kind="release",
        publisher="Release notes from tool",
        published=iso(menv.now),
    )
    menv.drop(
        batch([rel, item(2, "t-news", title="A big launch", published=iso(menv.now))], at=menv.now)
    )
    await menv.svc.ingest()
    menv.answers = ["1 9 2\n2 6 2"]  # [1] is the launch
    assert await menv.step() is True
    menv.now += timedelta(minutes=21)
    menv.answers = [
        "none",
        '{"i": 1, "topic": "tech", "why": "Nagy bejelentés.", "setup": false}\n'
        '{"i": 2, "topic": "orienteering", "why": "Új kiadás.", "setup": false}',
    ]
    assert await menv.step() is True  # grouping: none
    assert await menv.step() is True  # why, best score first
    t = await menv.by_title()
    launch, release = t["A big launch"], t["tool: b10"]
    assert (launch.why, launch.setup, launch.topic_id) == ("Nagy bejelentés.", False, "tech")
    assert (release.why, release.setup, release.topic_id) == ("Új kiadás.", True, "orienteering")
    assert menv.pushed == [launch.id]  # score 9 with a why, pushed right after the step
    assert (await menv.svc.item(launch.id))["why"] == "Nagy bejelentés."
    assert await menv.svc.push_due() == 0  # once


@pg
async def test_push_hours_and_daily_limit(menv: ModelEnv) -> None:
    night = datetime(2026, 11, 17, 22, 30, tzinfo=UTC)  # 23:30 in Budapest
    menv.now = night
    menv.drop(
        batch(
            [item(n, title=f"Must read number {n}", published=iso(night)) for n in range(1, 5)],
            at=night,
        )
    )
    await menv.svc.ingest()
    async with menv.db() as s:
        await s.execute(update(NewsItem).values(score=9, why="Fontos."))
        await s.commit()
    assert await menv.svc.push_due() == 0  # night: waits
    menv.now = datetime(2026, 11, 18, 5, 59, tzinfo=UTC)  # 06:59: before the digest at 06:30?
    assert await menv.svc.push_due() == 2  # 06:59 >= 06:30 (the imported digest time)
    assert await menv.svc.push_due() == 0  # two a day
    await menv.svc.update_settings({"push_max": 3})
    assert await menv.svc.push_due() == 1
    await menv.svc.update_settings({"push_max": 5, "push": False})
    assert await menv.svc.push_due() == 0
    await menv.svc.update_settings({"push": True})
    menv.now = night + timedelta(hours=13)  # older than 12 hours: not pushed any more
    assert await menv.svc.push_due() == 0
    assert len(menv.pushed) == 3


@pg
async def test_the_weekly(menv: ModelEnv) -> None:
    monday = datetime(2026, 11, 23, 5, 0, tzinfo=UTC)  # 06:00 in Budapest, before 06:30
    for n, (days, score, read) in enumerate(
        [(1, 8, False), (3, 9, False), (5, 6, True), (6, 4, False), (8, 9, False)], 1
    ):
        at = monday - timedelta(days=days)
        menv.drop(batch([item(n, title=f"Story of the week {n}", published=iso(at))], at=at))
        await menv.svc.ingest()
        async with menv.db() as s:
            row = (await s.execute(select(NewsItem).where(NewsItem.key == key(n)))).scalar_one()
            row.score, row.read_at = score, (monday if read else None)
            await s.commit()
    menv.now = monday
    assert (await menv.svc.digest())["weekly"] is None
    menv.now = monday + timedelta(minutes=31)
    assert (await menv.svc.digest())["weekly"] == {"count": 2}
    data, nxt = await menv.svc.items(weekly=True)
    assert [d["title"] for d in data] == ["Story of the week 2", "Story of the week 1"]
    assert nxt is None
    menv.now = monday + timedelta(days=1)
    assert (await menv.svc.digest())["weekly"] is None


def test_publisher_names() -> None:
    raw = batch(
        [
            item(1, publisher="Scuttlebutt Sailing News: Providing sailing news for sailors"),
            item(2, publisher="Ars: Short"),
        ]
    )
    b = parse_batch(json.dumps(raw).encode())
    assert [i.publisher for i in b.items] == ["Scuttlebutt Sailing News", "Ars: Short"]
