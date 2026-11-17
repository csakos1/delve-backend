"""News (N1): topics, the feeds the agent's collector reads, and the items it found.

The collector is a no-agent cron job of the agent's user (delve-infra `newslib.py`,
D-58): the backend never fetches the web. The two meet in two places:

- `sources.json` in `DELVE_NEWS_DIR` (`/var/lib/delve-news`, 2750 delve:agent) - the
  backend writes the enabled feeds there whenever they change, the collector reads it;
- `batch-*.json` in `DELVE_NEWS_BATCH_DIR` (the agent's `~/.hermes/cards/news`, read-only
  here through the card directory's bind mount) - one per collection: the new items and
  what each feed answered. Every `poll_s` the unread batches are taken in, each once
  (`news_batches`); the collector removes them after 3 days.

An item is stored under the collector's key (a hash of its normalised URL). An item whose
folded title another lead item of the last 72 hours already has is stored as its
duplicate (`dup_of`: another source of the same story); the lists show the lead only, with
the number of sources. The releases of a `release` feed are merged into one row per
source and day ("llama.cpp: 10 releases (b12468 - b12477)"). Scoring, grouping across
languages and the Hungarian summaries are N1b; until then the lists are newest first.

What the operator reads - topics, feeds, the line about his own setup - is personal: it is
imported (`delve-backend news import`) or edited in the app, and is in no repository.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import time
import unicodedata
import urllib.parse
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import NewsBatch, NewsItem, NewsSource, NewsTopic, SystemState
from delve.events import EventHub

log = logging.getLogger("delve.news")

TZ = ZoneInfo("Europe/Budapest")
TOPIC_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,47}$")
KEY_RE = re.compile(r"^[0-9a-f]{32}$")
BATCH_NAME_RE = re.compile(r"^batch-\d{8}T\d{6}Z-\d+\.json$")
HHMM_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
KINDS = ("feed", "release")
FEEDBACK = ("more", "less")
REPORT_STATUSES = ("ok", "not_modified", "skipped", "error")
TITLE_CHARS = 300
EXCERPT_CHARS = 400
PUBLISHER_CHARS = 80
URL_CHARS = 2000
NAME_CHARS = 60
DESCRIPTION_CHARS = 1500
SETUP_CHARS = 800
MAX_TOPICS = 32
MAX_SOURCES = 200
MAX_BATCH_BYTES = 16 * 1024 * 1024
MAX_BATCH_ITEMS = 5000
DUP_WINDOW = timedelta(hours=72)
DUP_MIN_CHARS = 12  # a shorter folded title ("live", "update") never makes a duplicate
MAX_RELEASES = 30  # per source and day
DIGEST_TOP = 4
DIGEST_WINDOW = timedelta(hours=24)
BATCH_KEEP = timedelta(days=10)  # the collector removes its files after 3
CLEANUP_S = 3600.0
FAILING_IN_ROW = 3
SETTINGS_KEY = "news"
DEFAULT_SETTINGS: dict[str, Any] = {"setup": "", "digest_at": "07:00", "push": True,
                                    "push_max": 2}  # fmt: skip


class NewsError(ValueError):
    """A request the news service refuses: `code` for the API, a message for people."""

    def __init__(self, code: str, message: str, status: int = 422) -> None:
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


class InvalidBatch(ValueError):
    pass


# --- pure helpers (used by the contract and the tests too) -------------------------------


def fold_title(title: str) -> str:
    """Lower case, no accents, letters and digits only: the duplicate test's key."""
    s = unicodedata.normalize("NFKD", title.lower())
    s = "".join(c if c.isalnum() else " " for c in s if not unicodedata.combining(c))
    return " ".join(s.split())


def release_label(publisher: str | None, source_id: str) -> str:
    label = re.sub(r"^Release notes from\s+", "", publisher or "", flags=re.I).strip()
    return label or source_id


def release_title(label: str, releases: Sequence[dict[str, Any]]) -> str:
    if len(releases) == 1:
        return f"{label}: {releases[0]['tag']}"
    return f"{label}: {len(releases)} releases ({releases[-1]['tag']} - {releases[0]['tag']})"


def _iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts else None


def _parse_ts(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def _clip(v: Any, n: int) -> str | None:
    if not isinstance(v, str):
        return None
    v = " ".join(v.split())
    return v[:n] if v else None


def check_url(url: Any) -> str:
    """A feed URL as the app or an import may give it: http(s), a host, no credentials.
    (The collector checks the address it resolves to before every request - D-41.)"""
    if not isinstance(url, str) or not 8 <= len(url) <= URL_CHARS:
        raise NewsError("invalid_url", "the URL is missing or too long")
    try:
        p = urllib.parse.urlsplit(url.strip())
    except ValueError as exc:
        raise NewsError("invalid_url", "not a URL") from exc
    if p.scheme not in ("http", "https") or not p.hostname or p.username or p.password:
        raise NewsError("invalid_url", "an http(s) URL with a host and no credentials")
    return url.strip()


def source_id_for(url: str) -> str:
    """An id from the URL when the app adds a feed without one: host + first path part."""
    p = urllib.parse.urlsplit(url)
    host = (p.hostname or "").lower().removeprefix("www.")
    part = next((x for x in p.path.split("/") if x), "")
    raw = f"{host}-{part}" if part else host
    sid = re.sub(r"[^a-z0-9.-]+", "-", raw.lower()).strip("-.")[:48].rstrip("-.")
    return sid or "feed"


# --- serialization ------------------------------------------------------------------------


def item_out(i: NewsItem, sources: int = 1) -> dict[str, Any]:
    return {
        "id": i.id,
        "topic": i.topic_id,
        "source": i.source_id,
        "kind": i.kind,
        "title": i.title,
        "url": i.url,
        "publisher": i.publisher,
        "excerpt": i.excerpt,
        "published_at": _iso(i.published_at),
        "seen_at": _iso(i.seen_at),
        "gn": i.gn,
        "sources": sources,
        "releases": i.releases,
        "score": i.score,
        "why": i.why,
        "setup": i.setup,
        "summary_hu": i.summary_hu,
        "read": i.read_at is not None,
        "saved": i.saved_at is not None,
        "feedback": i.feedback,
    }


def other_out(i: NewsItem) -> dict[str, Any]:
    return {"id": i.id, "publisher": i.publisher, "title": i.title, "url": i.url,
            "published_at": _iso(i.published_at)}  # fmt: skip


def topic_out(t: NewsTopic, counts: dict[str, int] | None = None) -> dict[str, Any]:
    c = counts or {}
    return {
        "id": t.id,
        "name": t.name,
        "description": t.description,
        "position": t.position,
        "enabled": t.enabled,
        "sources": int(c.get("sources") or 0),
        "items_24h": int(c.get("items_24h") or 0),
    }


def source_out(s: NewsSource) -> dict[str, Any]:
    return {
        "id": s.id,
        "topic": s.topic_id,
        "url": s.url,
        "kind": s.kind,
        "window_h": s.window_h,
        "every_min": s.every_min,
        "enabled": s.enabled,
        "origin": s.origin,
        "last_at": _iso(s.last_at),
        "last_ok_at": _iso(s.last_ok_at),
        "last_status": s.last_status,
        "last_http": s.last_http,
        "last_error_code": s.last_error_code,
        "last_error": s.last_error,
        "last_items": s.last_items,
        "last_new": s.last_new,
        "errors_in_row": s.errors_in_row,
    }


def settings_out(value: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(DEFAULT_SETTINGS)
    for k in DEFAULT_SETTINGS:
        if value and k in value:
            out[k] = value[k]
    return out


# --- validation of what comes in -----------------------------------------------------------


def _int(v: Any, name: str, lo: int, hi: int) -> int:
    if not isinstance(v, int) or isinstance(v, bool) or not lo <= v <= hi:
        raise NewsError("invalid_value", f"{name}: a whole number {lo}..{hi}")
    return v


def _bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise NewsError("invalid_value", f"{name}: true or false")
    return v


def _text(v: Any, name: str, n: int, *, empty: bool = False) -> str:
    if not isinstance(v, str) or len(v) > n or (not empty and not v.strip()):
        raise NewsError("invalid_value", f"{name}: text of at most {n} characters")
    return v.strip()


def check_topic(raw: Any, *, partial: bool = False) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise NewsError("invalid_value", "a topic is an object")
    out: dict[str, Any] = {}
    allowed = {"id", "name", "description", "position", "enabled"}
    extra = set(raw) - allowed
    if extra:
        raise NewsError("invalid_value", f"unknown field(s): {', '.join(sorted(extra))}")
    if not partial:
        tid = raw.get("id")
        if not isinstance(tid, str) or not TOPIC_ID_RE.match(tid):
            raise NewsError("invalid_value", "id: a-z, 0-9 and _, starting with a letter")
        out["id"] = tid
    elif "id" in raw:
        raise NewsError("invalid_value", "the id of a topic cannot change")
    if "name" in raw or not partial:
        out["name"] = _text(raw.get("name"), "name", NAME_CHARS)
    if "description" in raw:
        out["description"] = _text(raw["description"], "description", DESCRIPTION_CHARS,
                                   empty=True)  # fmt: skip
    if "position" in raw:
        out["position"] = _int(raw["position"], "position", 0, 1000)
    if "enabled" in raw:
        out["enabled"] = _bool(raw["enabled"], "enabled")
    return out


def check_source(raw: Any, *, partial: bool = False) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise NewsError("invalid_value", "a source is an object")
    allowed = {"id", "topic", "url", "kind", "window_h", "every_min", "enabled"}
    extra = set(raw) - allowed
    if extra:
        raise NewsError("invalid_value", f"unknown field(s): {', '.join(sorted(extra))}")
    out: dict[str, Any] = {}
    if partial and ("id" in raw or "url" in raw or "kind" in raw):
        raise NewsError("invalid_value", "id, url and kind cannot change: add a new source")
    if not partial:
        out["url"] = check_url(raw.get("url"))
        sid = raw.get("id") or source_id_for(out["url"])
        if not isinstance(sid, str) or not SOURCE_ID_RE.match(sid):
            raise NewsError("invalid_value", "id: a-z, 0-9, '.' and '-', at most 48")
        out["id"] = sid
        kind = raw.get("kind", "feed")
        if kind not in KINDS:
            raise NewsError("invalid_value", "kind: feed or release")
        out["kind"] = kind
    if "topic" in raw or not partial:
        topic = raw.get("topic")
        if not isinstance(topic, str) or not TOPIC_ID_RE.match(topic):
            raise NewsError("invalid_value", "topic: the id of a topic")
        out["topic_id"] = topic
    if "window_h" in raw:
        out["window_h"] = _int(raw["window_h"], "window_h", 1, 720)
    if "every_min" in raw:
        out["every_min"] = _int(raw["every_min"], "every_min", 30, 1440)
    if "enabled" in raw:
        out["enabled"] = _bool(raw["enabled"], "enabled")
    return out


def check_settings(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise NewsError("invalid_value", "settings are an object")
    extra = set(raw) - set(DEFAULT_SETTINGS)
    if extra:
        raise NewsError("invalid_value", f"unknown field(s): {', '.join(sorted(extra))}")
    out: dict[str, Any] = {}
    if "setup" in raw:
        out["setup"] = _text(raw["setup"], "setup", SETUP_CHARS, empty=True)
    if "digest_at" in raw:
        if not isinstance(raw["digest_at"], str) or not HHMM_RE.match(raw["digest_at"]):
            raise NewsError("invalid_value", "digest_at: HH:MM")
        out["digest_at"] = raw["digest_at"]
    if "push" in raw:
        out["push"] = _bool(raw["push"], "push")
    if "push_max" in raw:
        out["push_max"] = _int(raw["push_max"], "push_max", 0, 5)
    return out


def check_mark(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or not raw:
        raise NewsError("invalid_value", "read, saved and / or feedback")
    extra = set(raw) - {"read", "saved", "feedback"}
    if extra:
        raise NewsError("invalid_value", f"unknown field(s): {', '.join(sorted(extra))}")
    out: dict[str, Any] = {}
    for k in ("read", "saved"):
        if k in raw:
            out[k] = _bool(raw[k], k)
    if "feedback" in raw:
        if raw["feedback"] is not None and raw["feedback"] not in FEEDBACK:
            raise NewsError("invalid_value", "feedback: more, less or null")
        out["feedback"] = raw["feedback"]
    return out


@dataclass
class BatchItem:
    key: str
    source: str
    url: str
    title: str
    publisher: str | None
    published: datetime | None
    excerpt: str | None
    gn: bool
    kind: str


@dataclass
class Batch:
    at: datetime
    reports: list[dict[str, Any]] = field(default_factory=list)
    items: list[BatchItem] = field(default_factory=list)
    skipped: int = 0  # items that did not pass the checks


def parse_batch(raw: bytes) -> Batch:
    """A collector batch file, checked; bad items are dropped and counted, a bad file is
    InvalidBatch."""
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise InvalidBatch("not JSON") from exc
    if not isinstance(data, dict) or data.get("v") != 1:
        raise InvalidBatch("not a v1 batch")
    at = _parse_ts(data.get("at"))
    reports_raw, items_raw = data.get("sources"), data.get("items")
    if at is None or not isinstance(reports_raw, list) or not isinstance(items_raw, list):
        raise InvalidBatch("at, sources and items are required")
    if len(items_raw) > MAX_BATCH_ITEMS or len(reports_raw) > MAX_SOURCES * 2:
        raise InvalidBatch("too many entries")
    b = Batch(at=at)
    for r in reports_raw:
        if (
            not isinstance(r, dict)
            or not isinstance(r.get("id"), str)
            or r.get("status") not in REPORT_STATUSES
        ):
            continue
        http = r.get("http")
        b.reports.append({
            "id": r["id"],
            "status": r["status"],
            "http": http if isinstance(http, int) and not isinstance(http, bool) else None,
            "items": r["items"] if isinstance(r.get("items"), int) else 0,
            "new": r["new"] if isinstance(r.get("new"), int) else 0,
            "error_code": _clip(r.get("error_code"), 40),
            "error": _clip(r.get("error"), 200),
        })  # fmt: skip
    for it in items_raw:
        try:
            b.items.append(_batch_item(it))
        except (ValueError, TypeError, KeyError):
            b.skipped += 1
    return b


def _batch_item(it: Any) -> BatchItem:
    if not isinstance(it, dict):
        raise ValueError("not an object")
    key, source, url, title = it["key"], it["source"], it["url"], it["title"]
    if not isinstance(key, str) or not KEY_RE.match(key):
        raise ValueError("key")
    if not isinstance(source, str) or not SOURCE_ID_RE.match(source):
        raise ValueError("source")
    if (
        not isinstance(url, str)
        or len(url) > URL_CHARS
        or not url.startswith(("https://", "http://"))
    ):
        raise ValueError("url")
    clean = _clip(title, TITLE_CHARS)
    if clean is None:
        raise ValueError("title")
    kind = it.get("kind", "feed")
    if kind not in KINDS:
        raise ValueError("kind")
    gn = it.get("gn", False)
    if not isinstance(gn, bool):
        raise ValueError("gn")
    published = it.get("published")
    return BatchItem(
        key=key,
        source=source,
        url=url,
        title=clean,
        publisher=_clip(it.get("publisher"), PUBLISHER_CHARS),
        published=_parse_ts(published) if published is not None else None,
        excerpt=_clip(it.get("summary"), EXCERPT_CHARS),
        gn=gn,
        kind=kind,
    )


# --- the service ----------------------------------------------------------------------------


class NewsService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hub: EventHub,
        *,
        news_dir: Path,
        batch_dir: Path,
        poll_s: float = 30.0,
        retention_days: int = 30,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._sm = sessionmaker
        self._hub = hub
        self._news_dir = news_dir
        self._batch_dir = batch_dir
        self._poll_s = poll_s
        self._keep = timedelta(days=retention_days)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()  # one ingest at a time
        self._no_dir_logged = False

    # --- lifecycle ------------------------------------------------------------------------

    async def start(self) -> None:
        """The poll loop; `poll_s` 0 runs none (the tests, the CLI)."""
        if self._task is None and self._poll_s > 0:
            self._task = asyncio.create_task(self._loop(), name="news")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        next_cleanup = 0.0
        with contextlib.suppress(Exception):
            await self.write_sources()
        while True:
            try:
                await self.ingest()
                if time.monotonic() >= next_cleanup:
                    await self.cleanup()
                    await self.write_sources()  # mends a file removed by hand
                    next_cleanup = time.monotonic() + CLEANUP_S
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("news loop failed")
            await asyncio.sleep(self._poll_s)

    # --- the feed list for the collector ---------------------------------------------------

    async def write_sources(self) -> bool:
        """`sources.json` for the collector: the enabled feeds of the enabled topics. False
        when the directory is not there (the collector then has nothing to do)."""
        async with self._sm() as s:
            rows = (
                await s.execute(
                    select(NewsSource)
                    .join(NewsTopic, NewsTopic.id == NewsSource.topic_id)
                    .where(NewsSource.enabled, NewsTopic.enabled)
                    .order_by(NewsSource.id)
                )
            ).scalars()
            sources = [{"id": r.id, "url": r.url, "kind": r.kind, "window_h": r.window_h,
                        "every_min": r.every_min} for r in rows]  # fmt: skip
        data = {"v": 1, "written_at": self._clock().isoformat(), "sources": sources}
        path = self._news_dir / "sources.json"
        tmp = self._news_dir / f".sources.{os.getpid()}.part"
        try:
            await asyncio.to_thread(_write_file, tmp, path, json.dumps(data, indent=1) + "\n")
        except FileNotFoundError:
            if not self._no_dir_logged:
                log.warning("news directory missing", extra={"dir": str(self._news_dir)})
                self._no_dir_logged = True
            return False
        except OSError as exc:
            log.warning("news sources not written", extra={"error": type(exc).__name__})
            return False
        self._no_dir_logged = False
        log.info("news sources written", extra={"sources": len(sources)})
        return True

    # --- ingest ---------------------------------------------------------------------------

    async def ingest(self) -> int:
        """Take in the batch files not read yet; the number of new lead items."""
        async with self._lock:
            names = await asyncio.to_thread(_batch_names, self._batch_dir)
            if not names:
                return 0
            async with self._sm() as s:
                known = set(
                    (
                        await s.execute(select(NewsBatch.name).where(NewsBatch.name.in_(names)))
                    ).scalars()
                )
            total = 0
            for name in names:
                if name in known:
                    continue
                total += await self._ingest_file(name)
        if total:
            self._hub.publish("news.updated", {"new": total, "updated_at": _iso(self._clock())})
        return total

    async def _ingest_file(self, name: str) -> int:
        path = self._batch_dir / name
        try:
            raw = await asyncio.to_thread(_read_capped, path, MAX_BATCH_BYTES)
            batch = parse_batch(raw)
        except (OSError, InvalidBatch) as exc:
            async with self._sm() as s:
                await s.execute(
                    insert(NewsBatch)
                    .values(name=name, error=str(exc)[:200])
                    .on_conflict_do_nothing(index_elements=["name"])
                )
                await s.commit()
            log.warning("news batch refused", extra={"batch": name, "error": str(exc)[:200]})
            return 0
        now = self._clock()
        async with self._sm() as s:
            sources = {r.id: r for r in (await s.execute(select(NewsSource))).scalars()}
            for rep in batch.reports:
                src = sources.get(rep["id"])
                if src is not None:
                    _apply_report(src, rep, batch.at, now)
            new = 0
            for it in batch.items:
                src = sources.get(it.source)
                if src is None or not src.enabled:
                    continue  # removed or hidden since the collector read the list
                if it.kind == "release":
                    new += await self._apply_release(s, it, src, batch.at)
                else:
                    new += await self._apply_item(s, it, src, batch.at)
            s.add(NewsBatch(name=name, at=batch.at, items=len(batch.items), new=new))
            await s.commit()
        log.info(
            "news batch read",
            extra={"batch": name, "items": len(batch.items), "new": new,
                   "skipped": batch.skipped, "sources": len(batch.reports)},
        )  # fmt: skip
        return new

    async def _apply_item(
        self, s: AsyncSession, it: BatchItem, src: NewsSource, seen: datetime
    ) -> int:
        norm = fold_title(it.title)
        lead = None
        if len(norm) >= DUP_MIN_CHARS:
            lead = (
                await s.execute(
                    select(NewsItem.id)
                    .where(
                        NewsItem.norm_title == norm,
                        NewsItem.dup_of.is_(None),
                        NewsItem.seen_at >= seen - DUP_WINDOW,
                    )
                    .order_by(NewsItem.id)
                    .limit(1)
                )
            ).scalar_one_or_none()
        row_id = (
            await s.execute(
                insert(NewsItem)
                .values(
                    key=it.key,
                    source_id=src.id,
                    topic_id=src.topic_id,
                    kind="feed",
                    title=it.title,
                    url=it.url,
                    publisher=it.publisher,
                    excerpt=it.excerpt,
                    published_at=it.published,
                    seen_at=seen,
                    gn=it.gn,
                    norm_title=norm,
                    dup_of=lead,
                )
                .on_conflict_do_nothing(index_elements=["key"])
                .returning(NewsItem.id)
            )
        ).scalar_one_or_none()
        return 1 if row_id is not None and lead is None else 0

    async def _apply_release(
        self, s: AsyncSession, it: BatchItem, src: NewsSource, seen: datetime
    ) -> int:
        when = it.published or seen
        key = f"rel:{src.id}:{when.astimezone(TZ).date().isoformat()}"
        entry = {"tag": it.title, "url": it.url, "published_at": _iso(it.published)}
        label = release_label(it.publisher, src.id)
        row = (
            await s.execute(select(NewsItem).where(NewsItem.key == key).with_for_update())
        ).scalar_one_or_none()
        if row is None:
            releases = [entry]
            s.add(
                NewsItem(
                    key=key, source_id=src.id, topic_id=src.topic_id, kind="release",
                    title=release_title(label, releases), url=it.url, publisher=label,
                    excerpt=it.excerpt, published_at=it.published, seen_at=seen, gn=False,
                    norm_title=fold_title(key), releases=releases,
                )
            )  # fmt: skip
            await s.flush()
            return 1
        known = list(row.releases or [])
        if any(r.get("tag") == entry["tag"] for r in known):
            return 0
        known.append(entry)
        known.sort(key=lambda r: r.get("published_at") or "", reverse=True)
        known = known[:MAX_RELEASES]
        row.releases = known
        row.title = release_title(row.publisher or label, known)
        row.url = known[0]["url"]
        row.published_at = _parse_ts(known[0].get("published_at")) or row.published_at
        row.seen_at = seen
        await s.flush()
        return 0

    # --- reads ----------------------------------------------------------------------------

    def _visible(self) -> Any:
        """Lead items of enabled topics and sources (an item whose source is gone stays)."""
        return and_(
            NewsItem.dup_of.is_(None),
            or_(NewsItem.source_id.is_(None), NewsSource.enabled),
            or_(NewsItem.topic_id.is_(None), NewsTopic.enabled),
        )

    def _base(self) -> Any:
        return (
            select(NewsItem)
            .outerjoin(NewsSource, NewsSource.id == NewsItem.source_id)
            .outerjoin(NewsTopic, NewsTopic.id == NewsItem.topic_id)
        )

    async def _with_counts(self, s: AsyncSession, rows: list[NewsItem]) -> list[dict[str, Any]]:
        if not rows:
            return []
        counts: dict[Any, int] = dict(
            (
                await s.execute(
                    select(NewsItem.dup_of, func.count())
                    .where(NewsItem.dup_of.in_([r.id for r in rows]))
                    .group_by(NewsItem.dup_of)
                )
            ).all()
        )
        return [item_out(r, 1 + int(counts.get(r.id, 0))) for r in rows]

    async def digest(self) -> dict[str, Any]:
        """The Dashboard's News block: the best unread stories of the last day, how many are
        new, when the collector last reported, how many feeds fail."""
        now = self._clock()
        since = now - DIGEST_WINDOW
        async with self._sm() as s:
            recent = and_(self._visible(), NewsItem.seen_at >= since)
            unread = and_(recent, NewsItem.read_at.is_(None))
            top = list(
                (
                    await s.execute(
                        self._base()
                        .where(unread)
                        .order_by(
                            NewsItem.score.desc().nulls_last(),
                            func.coalesce(NewsItem.published_at, NewsItem.seen_at).desc(),
                            NewsItem.id.desc(),
                        )
                        .limit(DIGEST_TOP)
                    )
                ).scalars()
            )
            new = await s.scalar(
                select(func.count())
                .select_from(NewsItem)
                .outerjoin(NewsSource, NewsSource.id == NewsItem.source_id)
                .outerjoin(NewsTopic, NewsTopic.id == NewsItem.topic_id)
                .where(unread)
            )
            last_at = await s.scalar(select(func.max(NewsBatch.at)))
            enabled = await s.scalar(
                select(func.count()).select_from(NewsSource).where(NewsSource.enabled)
            )
            failing = await s.scalar(
                select(func.count())
                .select_from(NewsSource)
                .where(NewsSource.enabled, NewsSource.errors_in_row >= FAILING_IN_ROW)
            )
            items = await self._with_counts(s, top)
        return {
            "top": items,
            "new": int(new or 0),
            "updated_at": _iso(last_at),
            "state": "empty" if last_at is None else "ok",
            "sources": int(enabled or 0),
            "failing": int(failing or 0),
        }

    async def items(
        self,
        *,
        topic: str | None = None,
        saved: bool = False,
        before: int | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int | None]:
        async with self._sm() as s:
            q = self._base()
            if saved:
                q = q.where(NewsItem.saved_at.is_not(None), NewsItem.dup_of.is_(None))
            else:
                q = q.where(self._visible())
            if topic is not None:
                q = q.where(NewsItem.topic_id == topic)
            if before is not None:
                q = q.where(NewsItem.id < before)
            q = q.order_by(NewsItem.id.desc()).limit(limit + 1)
            rows = list((await s.execute(q)).scalars())
            more = len(rows) > limit
            rows = rows[:limit]
            data = await self._with_counts(s, rows)
        return data, (rows[-1].id if more and rows else None)

    async def item(self, item_id: int) -> dict[str, Any]:
        async with self._sm() as s:
            row = await s.get(NewsItem, item_id)
            if row is None:
                raise NewsError("news_item_not_found", "no such item", 404)
            if row.dup_of is not None:  # a duplicate opens as its story
                lead = await s.get(NewsItem, row.dup_of)
                row = lead or row
            others = list(
                (
                    await s.execute(
                        select(NewsItem).where(NewsItem.dup_of == row.id).order_by(NewsItem.id)
                    )
                ).scalars()
            )
            return {
                **item_out(row, 1 + len(others)),
                "others": [other_out(o) for o in others],
                "text": None,
            }

    async def mark(self, item_id: int, raw: Any) -> dict[str, Any]:
        """Read / saved / feedback of a story (a duplicate marks its lead)."""
        changes = check_mark(raw)
        now = self._clock()
        async with self._sm() as s:
            row = await s.get(NewsItem, item_id, with_for_update=True)
            if row is None:
                raise NewsError("news_item_not_found", "no such item", 404)
            if row.dup_of is not None:
                lead = await s.get(NewsItem, row.dup_of, with_for_update=True)
                row = lead or row
            if "read" in changes:
                row.read_at = (row.read_at or now) if changes["read"] else None
            if "saved" in changes:
                row.saved_at = (row.saved_at or now) if changes["saved"] else None
            if "feedback" in changes:
                row.feedback = changes["feedback"]
                row.feedback_at = now if changes["feedback"] is not None else None
            await s.commit()
            lead_id = row.id
        return await self.item(lead_id)

    # --- topics, sources, settings --------------------------------------------------------

    async def topics(self) -> list[dict[str, Any]]:
        since = self._clock() - DIGEST_WINDOW
        async with self._sm() as s:
            rows = list(
                (
                    await s.execute(select(NewsTopic).order_by(NewsTopic.position, NewsTopic.id))
                ).scalars()
            )
            nsrc = dict(
                (
                    await s.execute(
                        select(NewsSource.topic_id, func.count()).group_by(NewsSource.topic_id)
                    )
                ).all()
            )
            nitems = dict(
                (
                    await s.execute(
                        select(NewsItem.topic_id, func.count())
                        .where(NewsItem.dup_of.is_(None), NewsItem.seen_at >= since)
                        .group_by(NewsItem.topic_id)
                    )
                ).all()
            )
        return [topic_out(t, {"sources": nsrc.get(t.id, 0), "items_24h": nitems.get(t.id, 0)})
                for t in rows]  # fmt: skip

    async def sources(self) -> list[dict[str, Any]]:
        async with self._sm() as s:
            rows = (await s.execute(select(NewsSource).order_by(NewsSource.id))).scalars()
            return [source_out(r) for r in rows]

    async def settings(self) -> dict[str, Any]:
        async with self._sm() as s:
            row = await s.get(SystemState, SETTINGS_KEY)
            return settings_out(row.value if row else None)

    async def create_topic(self, raw: Any) -> dict[str, Any]:
        t = check_topic(raw)
        async with self._sm() as s:
            if await s.get(NewsTopic, t["id"]) is not None:
                raise NewsError("news_topic_exists", "a topic with this id exists", 409)
            if (await s.scalar(select(func.count()).select_from(NewsTopic)) or 0) >= MAX_TOPICS:
                raise NewsError("too_many_topics", f"at most {MAX_TOPICS} topics", 409)
            s.add(NewsTopic(**t))
            await s.commit()
        await self.write_sources()
        return await self._topic(t["id"])

    async def update_topic(self, topic_id: str, raw: Any) -> dict[str, Any]:
        t = check_topic(raw, partial=True)
        async with self._sm() as s:
            row = await s.get(NewsTopic, topic_id, with_for_update=True)
            if row is None:
                raise NewsError("news_topic_not_found", "no such topic", 404)
            for k, v in t.items():
                setattr(row, k, v)
            row.updated_at = self._clock()
            await s.commit()
        if "enabled" in t:
            await self.write_sources()
        return await self._topic(topic_id)

    async def delete_topic(self, topic_id: str) -> None:
        async with self._sm() as s:
            res = await s.execute(delete(NewsTopic).where(NewsTopic.id == topic_id))
            await s.commit()
        if not res.rowcount:  # type: ignore[attr-defined]
            raise NewsError("news_topic_not_found", "no such topic", 404)
        await self.write_sources()

    async def _topic(self, topic_id: str) -> dict[str, Any]:
        return next(t for t in await self.topics() if t["id"] == topic_id)

    async def create_source(self, raw: Any, origin: str = "app") -> dict[str, Any]:
        src = check_source(raw)
        async with self._sm() as s:
            if await s.get(NewsTopic, src["topic_id"]) is None:
                raise NewsError("news_topic_not_found", "no such topic", 404)
            if await s.get(NewsSource, src["id"]) is not None:
                raise NewsError("news_source_exists", "a source with this id exists", 409)
            if await s.scalar(select(NewsSource.id).where(NewsSource.url == src["url"])):
                raise NewsError("news_source_exists", "this feed is a source already", 409)
            if (await s.scalar(select(func.count()).select_from(NewsSource)) or 0) >= MAX_SOURCES:
                raise NewsError("too_many_sources", f"at most {MAX_SOURCES} sources", 409)
            s.add(NewsSource(**src, origin=origin))
            await s.commit()
            out = source_out(await s.get(NewsSource, src["id"]))  # type: ignore[arg-type]
        await self.write_sources()
        return out

    async def update_source(self, source_id: str, raw: Any) -> dict[str, Any]:
        src = check_source(raw, partial=True)
        async with self._sm() as s:
            row = await s.get(NewsSource, source_id, with_for_update=True)
            if row is None:
                raise NewsError("news_source_not_found", "no such source", 404)
            if "topic_id" in src and await s.get(NewsTopic, src["topic_id"]) is None:
                raise NewsError("news_topic_not_found", "no such topic", 404)
            for k, v in src.items():
                setattr(row, k, v)
            row.updated_at = self._clock()
            if "topic_id" in src:  # the items follow their source to the new topic
                await s.execute(
                    update(NewsItem)
                    .where(NewsItem.source_id == source_id)
                    .values(topic_id=src["topic_id"])
                )
            await s.commit()
            out = source_out(row)
        await self.write_sources()
        return out

    async def delete_source(self, source_id: str) -> None:
        async with self._sm() as s:
            res = await s.execute(delete(NewsSource).where(NewsSource.id == source_id))
            await s.commit()
        if not res.rowcount:  # type: ignore[attr-defined]
            raise NewsError("news_source_not_found", "no such source", 404)
        await self.write_sources()

    async def update_settings(self, raw: Any) -> dict[str, Any]:
        changes = check_settings(raw)
        async with self._sm() as s:
            row = await s.get(SystemState, SETTINGS_KEY, with_for_update=True)
            value = settings_out(row.value if row else None) | changes
            if row is None:
                s.add(SystemState(key=SETTINGS_KEY, value=value))
            else:
                row.value, row.updated_at = value, self._clock()
            await s.commit()
        return value

    # --- import / export (the CLI) --------------------------------------------------------

    async def import_config(self, data: Any, *, replace: bool = False) -> dict[str, int]:
        """Topics, sources and settings from one JSON document (the format of
        `deploy/news.example.json`). Everything is checked before anything is written.
        Without `replace` the existing topics and sources not in the file stay."""
        if not isinstance(data, dict) or data.get("v") != 1:
            raise NewsError("invalid_value", 'expected {"v": 1, "topics": [...], ...}')
        extra = set(data) - {"v", "topics", "sources", "settings"}
        if extra:
            raise NewsError("invalid_value", f"unknown field(s): {', '.join(sorted(extra))}")
        topics = [check_topic(t) for t in data.get("topics") or []]
        sources = [check_source(x) for x in data.get("sources") or []]
        settings = check_settings(data["settings"]) if "settings" in data else {}
        for i, t in enumerate(topics):
            t.setdefault("position", i)
        if len({t["id"] for t in topics}) != len(topics):
            raise NewsError("invalid_value", "a topic id is repeated")
        if len({x["id"] for x in sources}) != len(sources) or \
                len({x["url"] for x in sources}) != len(sources):  # fmt: skip
            raise NewsError("invalid_value", "a source id or URL is repeated")
        async with self._sm() as s:
            have = set((await s.execute(select(NewsTopic.id))).scalars())
            known = have | {t["id"] for t in topics} if not replace else {t["id"] for t in topics}
            for x in sources:
                if x["topic_id"] not in known:
                    raise NewsError("invalid_value", f"source {x['id']}: no topic {x['topic_id']}")
            if replace:
                await s.execute(delete(NewsSource).where(
                    NewsSource.id.not_in([x["id"] for x in sources] or [""])))  # fmt: skip
                await s.execute(delete(NewsTopic).where(
                    NewsTopic.id.not_in([t["id"] for t in topics] or [""])))  # fmt: skip
            now = self._clock()
            for t in topics:
                await s.execute(
                    insert(NewsTopic)
                    .values(**t)
                    .on_conflict_do_update(index_elements=["id"], set_={**t, "updated_at": now})
                )
            for x in sources:
                clash = await s.scalar(
                    select(NewsSource.id).where(
                        NewsSource.url == x["url"], NewsSource.id != x["id"]
                    )
                )
                if clash is not None:
                    raise NewsError("invalid_value", f"source {x['id']}: the URL is {clash}'s")
                await s.execute(
                    insert(NewsSource)
                    .values(**x, origin="import")
                    .on_conflict_do_update(index_elements=["id"], set_={**x, "updated_at": now})
                )
            if settings:
                row = await s.get(SystemState, SETTINGS_KEY, with_for_update=True)
                value = settings_out(row.value if row else None) | settings
                if row is None:
                    s.add(SystemState(key=SETTINGS_KEY, value=value))
                else:
                    row.value, row.updated_at = value, now
            await s.commit()
        await self.write_sources()
        log.info("news config imported", extra={"topics": len(topics), "sources": len(sources)})
        return {"topics": len(topics), "sources": len(sources), "settings": len(settings)}

    async def export_config(self) -> dict[str, Any]:
        async with self._sm() as s:
            topics = (await s.execute(select(NewsTopic).order_by(NewsTopic.position))).scalars()
            t_out = [{"id": t.id, "name": t.name, "description": t.description,
                      "position": t.position, "enabled": t.enabled} for t in topics]  # fmt: skip
            sources = (await s.execute(select(NewsSource).order_by(NewsSource.id))).scalars()
            s_out = [{"id": x.id, "topic": x.topic_id, "url": x.url, "kind": x.kind,
                      "window_h": x.window_h, "every_min": x.every_min, "enabled": x.enabled}
                     for x in sources]  # fmt: skip
        return {"v": 1, "topics": t_out, "sources": s_out, "settings": await self.settings()}

    # --- retention ------------------------------------------------------------------------

    async def cleanup(self) -> int:
        now = self._clock()
        async with self._sm() as s:
            old = and_(NewsItem.saved_at.is_(None), NewsItem.seen_at < now - self._keep)
            res = await s.execute(delete(NewsItem).where(old))
            await s.execute(delete(NewsBatch).where(NewsBatch.ingested_at < now - BATCH_KEEP))
            await s.commit()
        n = int(res.rowcount or 0)  # type: ignore[attr-defined]
        if n:
            log.info("old news items dropped", extra={"items": n})
        return n


# --- files ---------------------------------------------------------------------------------------


def _batch_names(d: Path) -> list[str]:
    try:
        return sorted(n for n in os.listdir(d) if BATCH_NAME_RE.match(n))
    except OSError:
        return []


def _read_capped(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as f:
        raw = f.read(limit + 1)
    if len(raw) > limit:
        raise InvalidBatch(f"larger than {limit} bytes")
    return raw


def _write_file(tmp: Path, path: Path, text: str) -> None:
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o640)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _apply_report(src: NewsSource, rep: dict[str, Any], at: datetime, now: datetime) -> None:
    if src.last_at is not None and at < src.last_at:
        return  # an older batch read late
    src.last_at, src.last_status, src.last_http = at, rep["status"], rep["http"]
    if rep["status"] in ("ok", "not_modified"):
        src.last_ok_at = at
        src.errors_in_row = 0
        src.last_error_code = src.last_error = None
        if rep["status"] == "ok":
            src.last_items, src.last_new = rep["items"], rep["new"]
    elif rep["status"] == "error":
        src.errors_in_row = (src.errors_in_row or 0) + 1
        src.last_error_code, src.last_error = rep["error_code"], rep["error"]
    else:  # skipped: rate limited or out of time - the reason, no count
        src.last_error_code = rep["error_code"]
    src.updated_at = now
