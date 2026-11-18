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
from collections import Counter
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
from delve.llama import LlamaClient, LlamaUnavailable
from delve.news_model import (
    Candidate,
    ModelAnswerError,
    TopicRef,
    content_of,
    group_body,
    parse_groups,
    parse_scores,
    parse_whys,
    score_body,
    system_prompt,
    why_body,
)

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
# N1b: the model steps
SCORE_BATCH = 12  # ~10 s of llama time: a run that comes in waits at most that (N327)
SCORE_WINDOW = timedelta(hours=48)  # older unscored items are left alone
SCORE_MAX_ATTEMPTS = 3
GROUP_MIN_SCORE = 5
GROUP_WINDOW = timedelta(hours=36)
GROUP_MAX_ITEMS = 40
GROUP_NEW_MAX = 16  # the rest of the 40: the known stories that look related
GROUP_POOL_MAX = 1000
GROUP_MIN_NEW = 8  # or the oldest new one waited GROUP_MAX_WAIT
GROUP_MAX_WAIT = timedelta(minutes=20)
WHY_MIN_SCORE = 5
WHY_BATCH = 5
WHY_MAX_ATTEMPTS = 2
FAIL_BACKOFF_S = 60.0
PUSH_MIN_SCORE = 9
PUSH_WINDOW = timedelta(hours=12)  # a story older than this is not pushed any more
PUSH_UNTIL_HOUR = 22  # pushes wait from 22:00 until digest_at
WEEKLY_TOP = 10
WEEKLY_MIN_SCORE = 5
RELEASE_SCORE = 6  # a day of releases is good to know, not a must-read: a rule, not llama
WAIT_LOG_S = 1800.0  # a News step that waits for llama says so this often (N342)
RESCORE_MAX_HOURS = 48  # = SCORE_WINDOW: older stories would not be scored again
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


_STOP_WORDS = (
    "the and for with from that this into over after before about have has will says said "
    "than what when where your their more most just also been were they them amid ahead news "
    "live update updates video report reports first here there which while could would "
    "should other years year week today hogy volt lesz csak mint utan elott alatt szerint "
    "akkor most majd lehet kell ujabb ismet miatt ellen kozott amely mert minden"
)
_STOP = frozenset(_STOP_WORDS.split())


def title_words(title: str) -> set[str]:
    """The words of a title that can tie two stories together (names, numbers)."""
    return {
        w
        for w in fold_title(title).split()
        if w not in _STOP and (len(w) >= 4 or (len(w) >= 2 and any(c.isdigit() for c in w)))
    }


def pick_related(
    new: Sequence[tuple[int, str, str | None]],
    pool: Sequence[tuple[int, str, str | None]],
    room: int,
) -> list[int]:
    """Which known stories (id, title, topic) go into a grouping with the new ones: first
    those sharing words with a new title (a word in fewer titles counts more), then the
    ones of the same topics, then the rest - in the pool's order (best first)."""
    if room <= 0:
        return []
    words = {i: title_words(t) for i, t, _ in [*new, *pool]}
    df = Counter(w for ws in words.values() for w in ws)
    wanted: set[str] = set()
    for i, _, _ in new:
        wanted |= words[i]
    ranked = []
    for n, (i, _, _) in enumerate(pool):
        weight = sum(1 / df[w] for w in words[i] & wanted)
        if weight > 0:
            ranked.append((-weight, n, i))
    picked = [i for _, _, i in sorted(ranked)][:room]
    taken = set(picked)
    topics = {topic for _, _, topic in new}
    rest = [i for i, _, t in pool if i not in taken and t in topics]
    rest += [i for i, _, t in pool if i not in taken and t not in topics]
    return picked + rest[: room - len(picked)]


_YEAR = re.compile(r"(19|20)\d\d")


def hanging_together(members: Sequence[tuple[int, str, str | None]]) -> list[int]:
    """Of the stories (id, title, topic) the model put into one group, the ones that hang
    together: two are tied when they have the same topic or a title word in common (not a
    year). The largest tied part stays (on a tie, the one with the first member); the rest
    were put there by mistake - live, a sailing sponsor and two GTA launch stories, a road
    concession and a GTA story became one."""
    words = [{w for w in title_words(t) if not _YEAR.fullmatch(w)} for _, t, _ in members]
    part = list(range(len(members)))

    def root(n: int) -> int:
        while part[n] != n:
            part[n] = part[part[n]]
            n = part[n]
        return n

    for a in range(len(members)):
        for b in range(a + 1, len(members)):
            same_topic = members[a][2] is not None and members[a][2] == members[b][2]
            if same_topic or words[a] & words[b]:
                part[root(a)] = root(b)
    sizes = Counter(root(n) for n in range(len(members)))
    best = max(sizes.values(), default=0)
    keep = next((root(n) for n in range(len(members)) if sizes[root(n)] == best), None)
    return [members[n][0] for n in range(len(members)) if root(n) == keep]


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


def _publisher(raw: Any) -> str | None:
    """A feed title as the publisher's name: "Scuttlebutt Sailing News: Providing sailing
    news for sailors" -> "Scuttlebutt Sailing News"."""
    name = _clip(raw, PUBLISHER_CHARS)
    if name and len(name) > 30 and name.find(": ") > 3:
        name = name.split(": ", 1)[0]
    return name


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
        publisher=_publisher(it.get("publisher")),
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
        llama: LlamaClient | None = None,
        notify: Callable[[int], None] | None = None,
        idle_s: float = 180.0,
        call_timeout_s: float = 120.0,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
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
        self._llama = llama
        self.notify = notify
        self._idle_s = idle_s
        self._call_timeout_s = call_timeout_s
        self._mono = monotonic
        self._retry_at = 0.0
        self._seen_total: float | None = None  # llama's token counters, last seen
        self._active_at = monotonic()  # when llama was last seen busy (start: now)
        self._wait_reason = "in use"
        self._waiting_since: float | None = None  # a step is due, llama is not free
        self._wait_logged = 0.0

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
        while True:
            try:
                await self.ingest()
                await self.push_due()
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
                    norm_title=fold_title(key), releases=releases, score=RELEASE_SCORE,
                    scored_at=seen, grouped_at=seen,
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
        row.why, row.why_attempts = None, 0  # the why may name the newest one
        await s.flush()
        return 0

    # --- the model steps (N1b) -------------------------------------------------------------

    async def work(self) -> bool:
        """One model step between runs (`RunManager.side_work`, after the watchers): a batch
        of scores, a grouping, or a batch of Hungarian whys. True if llama was asked
        (the worker looks at its queue again before the next step)."""
        if self._llama is None or self._mono() < self._retry_at:
            return False
        async with self._sm() as s:
            step = await self._next_step(s)
        if step is None:
            self._waiting_since = None
            return False
        if not await self._llama_idle():
            self._note_wait()
            return False
        self._waiting_since = None
        kind, rows = step
        started = self._mono()
        try:
            if kind == "score":
                tokens = await self._score(rows)
            elif kind == "group":
                tokens = await self._group(rows)
            else:
                tokens = await self._why(rows)
        except (LlamaUnavailable, ModelAnswerError) as exc:
            self._retry_at = self._mono() + FAIL_BACKOFF_S
            await self._step_failed(kind, rows)
            log.warning("news step failed", extra={"step": kind, "items": len(rows),
                                                   "error": str(exc)[:200]})  # fmt: skip
            return False
        finally:
            await self._own_use()
        ms = int((self._mono() - started) * 1000)
        log.info("news step", extra={"step": kind, "items": len(rows), "ms": ms, **tokens})
        if kind == "why":
            await self.push_due()
        return True

    async def _llama_idle(self) -> bool:
        """llama-server free, and nobody used it for `idle_s`: a coding session talks to it
        directly, and its one slot's prompt cache would be pushed out by a News request."""
        assert self._llama is not None  # noqa: S101
        try:
            busy = (await self._llama.slot()).processing
            m = await self._llama.metrics()
        except LlamaUnavailable:
            self._retry_at = self._mono() + FAIL_BACKOFF_S
            self._wait_reason = "unavailable"
            return False
        self._wait_reason = "in use"
        total = m.get("prompt_tokens_total", 0.0) + m.get("tokens_predicted_total", 0.0)
        now = self._mono()
        if busy or total != self._seen_total:
            self._seen_total = total
            self._active_at = now
            return False
        return now - self._active_at >= self._idle_s

    def _note_wait(self) -> None:
        """A long wait shows in the log: once after half an hour, then every half hour."""
        now = self._mono()
        if self._waiting_since is None:
            self._waiting_since = self._wait_logged = now
            return
        if now - self._wait_logged >= WAIT_LOG_S:
            self._wait_logged = now
            log.info(
                "news waiting",
                extra={
                    "reason": self._wait_reason,
                    "minutes": int((now - self._waiting_since) // 60),
                },
            )

    async def _own_use(self) -> None:
        """Our own request does not count as somebody using llama."""
        if self._llama is None:
            return
        with contextlib.suppress(LlamaUnavailable):
            m = await self._llama.metrics()
            self._seen_total = m.get("prompt_tokens_total", 0.0) + m.get(
                "tokens_predicted_total", 0.0
            )

    async def _next_step(self, s: AsyncSession) -> tuple[str, list[NewsItem]] | None:
        now = self._clock()
        live = and_(self._visible(), NewsItem.kind.in_(KINDS))
        rows = list(
            (
                await s.execute(
                    self._base()
                    .where(
                        live,
                        NewsItem.kind == "feed",
                        NewsItem.score.is_(None),
                        NewsItem.score_attempts < SCORE_MAX_ATTEMPTS,
                        NewsItem.seen_at >= now - SCORE_WINDOW,
                    )
                    .order_by(NewsItem.seen_at.desc(), NewsItem.id.desc())
                    .limit(SCORE_BATCH)
                )
            ).scalars()
        )
        if rows:
            return "score", rows
        fresh = and_(
            live,
            NewsItem.kind == "feed",  # a release row is a day of one feed: nothing to merge
            NewsItem.score >= GROUP_MIN_SCORE,
            NewsItem.seen_at >= now - GROUP_WINDOW,
        )
        new = list(
            (
                await s.execute(
                    self._base()
                    .where(fresh, NewsItem.grouped_at.is_(None))
                    .order_by(NewsItem.id)
                    .limit(GROUP_NEW_MAX)
                )
            ).scalars()
        )
        waited = bool(new) and min(r.scored_at or now for r in new) <= now - GROUP_MAX_WAIT
        if new and (len(new) >= GROUP_MIN_NEW or waited):
            # the others of the day and a half (compared before or not yet), the related
            # ones first: a story from another batch of the day must meet its pair (N339)
            pool = list(
                (
                    await s.execute(
                        self._base()
                        .where(fresh, NewsItem.id.not_in([r.id for r in new]))
                        .order_by(NewsItem.score.desc(), NewsItem.id.desc())
                        .limit(GROUP_POOL_MAX)
                    )
                ).scalars()
            )
            ids = pick_related(
                [(r.id, r.title, r.topic_id) for r in new],
                [(r.id, r.title, r.topic_id) for r in pool],
                GROUP_MAX_ITEMS - len(new),
            )
            by_id = {r.id: r for r in pool}
            return "group", new + [by_id[i] for i in ids]
        rows = list(
            (
                await s.execute(
                    self._base()
                    .where(
                        live,
                        NewsItem.score >= WHY_MIN_SCORE,
                        NewsItem.why.is_(None),
                        NewsItem.why_attempts < WHY_MAX_ATTEMPTS,
                        NewsItem.grouped_at.is_not(None),
                        NewsItem.seen_at >= now - SCORE_WINDOW,
                    )
                    .order_by(NewsItem.score.desc(), NewsItem.id.desc())
                    .limit(WHY_BATCH)
                )
            ).scalars()
        )
        if rows:
            return "why", rows
        return None

    async def _prompt(self) -> tuple[list[TopicRef], str]:
        async with self._sm() as s:
            topics = [
                TopicRef(t.id, t.name, t.description)
                for t in (
                    await s.execute(
                        select(NewsTopic)
                        .where(NewsTopic.enabled)
                        .order_by(NewsTopic.position, NewsTopic.id)
                    )
                ).scalars()
            ]
            row = await s.get(SystemState, SETTINGS_KEY)
        setup = settings_out(row.value if row else None)["setup"]
        return topics, system_prompt(topics, setup)

    async def _ask(self, body: dict[str, Any]) -> tuple[str, dict[str, int]]:
        assert self._llama is not None  # noqa: S101
        answer = await self._llama.chat(body, timeout_s=self._call_timeout_s)
        return content_of(answer)

    async def _score(self, rows: list[NewsItem]) -> dict[str, int]:
        topics, system = await self._prompt()
        text, tokens = await self._ask(score_body(system, [_candidate(r) for r in rows]))
        scores = parse_scores(text, len(rows), len(topics))
        if not scores:
            raise ModelAnswerError("no score line in the answer")
        now = self._clock()
        async with self._sm() as s:
            for n, r in enumerate(rows, 1):
                row = await s.get(NewsItem, r.id, with_for_update=True)
                if row is None:
                    continue
                if n in scores:
                    score, topic = scores[n]
                    row.score, row.scored_at = score, now
                    if topic:
                        row.topic_id = topics[topic - 1].id
                else:
                    row.score_attempts += 1
                    if row.score_attempts >= SCORE_MAX_ATTEMPTS:  # left out three times
                        row.score, row.scored_at = 0, now
            await s.commit()
        return tokens

    async def _group(self, rows: list[NewsItem]) -> dict[str, int]:
        _topics, system = await self._prompt()
        text, tokens = await self._ask(group_body(system, [_candidate(r) for r in rows]))
        groups = [] if text.strip().lower().startswith("none") else parse_groups(text, len(rows))
        now = self._clock()
        merged = left_out = 0
        async with self._sm() as s:
            for group in groups:
                members = [rows[n - 1] for n in group]
                keep = hanging_together([(m.id, m.title, m.topic_id) for m in members])
                left_out += len(members) - len(keep)
                members = [m for m in members if m.id in keep]
                if len(members) < 2:
                    continue
                if all(m.grouped_at is not None for m in members):
                    continue  # stories kept apart before stay apart
                head = min(members, key=lambda m: (m.seen_at, m.id))
                others = [m.id for m in members if m.id != head.id]
                await s.execute(
                    update(NewsItem).where(NewsItem.dup_of.in_(others)).values(dup_of=head.id)
                )
                await s.execute(
                    update(NewsItem).where(NewsItem.id.in_(others)).values(dup_of=head.id)
                )
                best = max(m.score or 0 for m in members)
                await s.execute(
                    update(NewsItem)
                    .where(NewsItem.id == head.id, NewsItem.score < best)
                    .values(score=best, why=None, why_attempts=0)
                )
                merged += len(others)
            await s.execute(
                update(NewsItem)
                .where(NewsItem.id.in_([r.id for r in rows]), NewsItem.grouped_at.is_(None))
                .values(grouped_at=now)
            )
            await s.commit()
        if merged or left_out:
            log.info(
                "news stories merged",
                extra={"groups": len(groups), "items": merged, "left_out": left_out},
            )
        return tokens

    async def _why(self, rows: list[NewsItem]) -> dict[str, int]:
        topics, system = await self._prompt()
        text, tokens = await self._ask(why_body(system, topics, [_candidate(r) for r in rows]))
        whys = parse_whys(text, len(rows), {t.id for t in topics})
        if not whys:
            raise ModelAnswerError("no why line in the answer")
        async with self._sm() as s:
            for n, r in enumerate(rows, 1):
                row = await s.get(NewsItem, r.id, with_for_update=True)
                if row is None:
                    continue
                w = whys.get(n)
                if w is None:
                    row.why_attempts += 1
                    continue
                row.why = w.why
                # releases of the reader's own tools concern his setup by rule (N328), and
                # stay in their feed's topic
                row.setup = True if row.kind == "release" else w.setup
                if w.topic and row.kind != "release":
                    row.topic_id = w.topic
            await s.commit()
        return tokens

    async def _step_failed(self, kind: str, rows: list[NewsItem]) -> None:
        if kind == "group":
            return  # tried again at the next chance
        ids = [r.id for r in rows]
        now = self._clock()
        async with self._sm() as s:
            if kind == "score":
                await s.execute(
                    update(NewsItem)
                    .where(NewsItem.id.in_(ids))
                    .values(score_attempts=NewsItem.score_attempts + 1)
                )
                await s.execute(
                    update(NewsItem)
                    .where(
                        NewsItem.id.in_(ids),
                        NewsItem.score.is_(None),
                        NewsItem.score_attempts >= SCORE_MAX_ATTEMPTS,
                    )
                    .values(score=0, scored_at=now)
                )
            else:
                await s.execute(
                    update(NewsItem)
                    .where(NewsItem.id.in_(ids))
                    .values(why_attempts=NewsItem.why_attempts + 1)
                )
            await s.commit()

    async def rescore(self, hours: int) -> int:
        """The stories of the last `hours` go through score, grouping and why again (after a
        topic description changed), from their feed's topic. Merged duplicates stay merged,
        a story is not pushed twice, read / saved / feedback stay. Release rows keep their
        rule. The number of stories."""
        if not 1 <= hours <= RESCORE_MAX_HOURS:
            raise NewsError("bad_hours", f"hours: 1 - {RESCORE_MAX_HOURS}")
        since = self._clock() - timedelta(hours=hours)
        which = and_(NewsItem.kind == "feed", NewsItem.dup_of.is_(None), NewsItem.seen_at >= since)
        async with self._sm() as s:
            n = await s.scalar(select(func.count()).select_from(NewsItem).where(which))
            await s.execute(
                update(NewsItem)
                .where(which)
                .values(**_unscored())
                .execution_options(synchronize_session=False)
            )
            await s.commit()
        log.info("news rescore", extra={"hours": hours, "items": int(n or 0)})
        return int(n or 0)

    async def split(self, story: int | str) -> tuple[int, str, int]:
        """Undo the model's merges in one story (an id, or a piece of a title that matches
        one story of the last 48 hours): the items whose folded title differs from the
        lead's leave it - those with the same title stay together - and the lead and every
        new story go through score, grouping and why again (the grouping now checks that a
        group hangs together). Items with the lead's own title (the same headline in other
        feeds) stay. (lead id, lead title, how many stories were split off)."""
        now = self._clock()
        async with self._sm() as s:
            head = await self._story(s, story, now)
            members = list(
                (
                    await s.execute(
                        select(NewsItem).where(NewsItem.dup_of == head.id).order_by(NewsItem.id)
                    )
                ).scalars()
            )
            own = fold_title(head.title)
            parts: dict[str, list[NewsItem]] = {}
            for m in members:
                if fold_title(m.title) != own:
                    parts.setdefault(fold_title(m.title), []).append(m)
            leads = []
            for part in parts.values():
                lead = min(part, key=lambda m: (m.score is None, m.seen_at, m.id))
                leads.append(lead.id)
                others = [m.id for m in part if m.id != lead.id]
                if others:
                    await s.execute(
                        update(NewsItem).where(NewsItem.id.in_(others)).values(dup_of=lead.id)
                    )
            if leads:
                await s.execute(update(NewsItem).where(NewsItem.id.in_(leads)).values(dup_of=None))
                await s.execute(
                    update(NewsItem)
                    .where(NewsItem.id.in_([head.id, *leads]))
                    .values(**_unscored())
                    .execution_options(synchronize_session=False)
                )
            await s.commit()
            out = (head.id, head.title, len(leads))
        log.info("news split", extra={"story": out[0], "stories": out[2]})
        return out

    async def _story(self, s: AsyncSession, story: int | str, now: datetime) -> NewsItem:
        recent = now - timedelta(hours=RESCORE_MAX_HOURS)
        if isinstance(story, int):
            row = await s.get(NewsItem, story)
            if row is not None and row.dup_of is not None:
                row = await s.get(NewsItem, row.dup_of)
            if row is None:
                raise NewsError("not_found", f"no story {story}", 404)
        else:
            text = story.strip()
            if len(text) < 3:
                raise NewsError("invalid_value", "a piece of a title: at least 3 characters")
            pattern = "%" + re.sub(r"([\\%_])", r"\\\1", text) + "%"
            found = list(
                (
                    await s.execute(
                        select(NewsItem)
                        .where(
                            NewsItem.dup_of.is_(None),
                            NewsItem.kind == "feed",
                            NewsItem.seen_at >= recent,
                            NewsItem.title.ilike(pattern, escape="\\"),
                        )
                        .order_by(NewsItem.id)
                        .limit(11)
                    )
                ).scalars()
            )
            if not found:
                raise NewsError("not_found", f"no story of the last 48 hours has {text!r}", 404)
            if len(found) > 1:
                lines = "\n".join(f"  {r.id}  {r.title[:90]}" for r in found[:10])
                more = "\n  ..." if len(found) > 10 else ""
                raise NewsError("ambiguous", f"more stories have {text!r}:\n{lines}{more}", 409)
            row = found[0]
        if row.kind != "feed":
            raise NewsError("not_a_story", "a day of releases is never merged", 409)
        if row.seen_at < recent:
            raise NewsError("too_old", "only stories of the last 48 hours are scored again", 409)
        return row

    # --- push ----------------------------------------------------------------------------

    async def push_due(self) -> int:
        """The must-reads (score 9, a why) of the last hours, at most `push_max` a day, not
        between 22:00 and the digest time (they wait). The PushService sends only to the
        devices with `notify.news`."""
        if self.notify is None:
            return 0
        now = self._clock()
        local = now.astimezone(TZ)
        async with self._sm() as s:
            row = await s.get(SystemState, SETTINGS_KEY)
            cfg = settings_out(row.value if row else None)
            hh, mm = (int(x) for x in cfg["digest_at"].split(":"))
            if not cfg["push"] or not (hh, mm) <= (local.hour, local.minute) < (PUSH_UNTIL_HOUR, 0):
                return 0
            day_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
            done = await s.scalar(
                select(func.count()).select_from(NewsItem).where(NewsItem.pushed_at >= day_start)
            )
            room = cfg["push_max"] - int(done or 0)
            if room <= 0:
                return 0
            rows = list(
                (
                    await s.execute(
                        self._base()
                        .where(
                            self._visible(),
                            NewsItem.score >= PUSH_MIN_SCORE,
                            NewsItem.why.is_not(None),
                            NewsItem.pushed_at.is_(None),
                            NewsItem.read_at.is_(None),
                            NewsItem.seen_at >= now - PUSH_WINDOW,
                        )
                        .order_by(NewsItem.score.desc(), NewsItem.id)
                        .limit(room)
                    )
                ).scalars()
            )
            for r in rows:
                r.pushed_at = now
            await s.commit()
            ids = [r.id for r in rows]
        for item_id in ids:
            self.notify(item_id)
        return len(ids)

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
            best = list(
                (
                    await s.execute(
                        self._base()
                        .where(unread)
                        .order_by(
                            NewsItem.score.desc().nulls_last(),
                            func.coalesce(NewsItem.published_at, NewsItem.seen_at).desc(),
                            NewsItem.id.desc(),
                        )
                        .limit(DIGEST_TOP * 10)
                    )
                ).scalars()
            )
            top = _one_per_topic(best, DIGEST_TOP)
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
            weekly = None
            if await self._weekly_day(s, now):
                n = await s.scalar(
                    select(func.count()).select_from(
                        self._weekly_query(now).limit(WEEKLY_TOP).subquery()
                    )
                )
                weekly = {"count": int(n or 0)}
        return {
            "top": items,
            "new": int(new or 0),
            "updated_at": _iso(last_at),
            "state": "empty" if last_at is None else "ok",
            "sources": int(enabled or 0),
            "failing": int(failing or 0),
            "weekly": weekly,
        }

    async def _weekly_day(self, s: AsyncSession, now: datetime) -> bool:
        """Monday from the digest time on: the Dashboard shows "Last week" first."""
        local = now.astimezone(TZ)
        if local.weekday() != 0:
            return False
        row = await s.get(SystemState, SETTINGS_KEY)
        hh, mm = settings_out(row.value if row else None)["digest_at"].split(":")
        return (local.hour, local.minute) >= (int(hh), int(mm))

    def _weekly_query(self, now: datetime) -> Any:
        """The best stories of the last 7 days that were not opened."""
        return (
            self._base()
            .where(
                self._visible(),
                NewsItem.read_at.is_(None),
                NewsItem.score >= WEEKLY_MIN_SCORE,
                NewsItem.seen_at >= now - timedelta(days=7),
            )
            .order_by(NewsItem.score.desc(), NewsItem.id.desc())
        )

    async def items(
        self,
        *,
        topic: str | None = None,
        saved: bool = False,
        weekly: bool = False,
        before: int | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], int | None]:
        if weekly:  # one page, best first
            async with self._sm() as s:
                q = self._weekly_query(self._clock()).limit(min(limit, WEEKLY_TOP))
                rows = list((await s.execute(q)).scalars())
                return await self._with_counts(s, rows), None
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


def _one_per_topic(rows: list[NewsItem], n: int) -> list[NewsItem]:
    """The best `n`, at most one of a topic while other topics have one to show (a big day
    of one topic does not fill the Dashboard block); the order stays the rank."""
    picked: list[int] = []
    topics: set[str | None] = set()
    for k, r in enumerate(rows):
        if len(picked) < n and r.topic_id not in topics:
            picked.append(k)
            topics.add(r.topic_id)
    for k in range(len(rows)):
        if len(picked) >= n:
            break
        if k not in picked:
            picked.append(k)
    return [rows[k] for k in sorted(picked)]


def _unscored() -> dict[str, Any]:
    """The values that send a lead story through score, grouping and why again, from its
    feed's topic (the model's topic is decided again too)."""
    feed_topic = (
        select(NewsSource.topic_id).where(NewsSource.id == NewsItem.source_id).scalar_subquery()
    )
    return {
        "score": None, "scored_at": None, "score_attempts": 0, "why": None, "why_attempts": 0,
        "setup": None, "grouped_at": None,
        "topic_id": func.coalesce(feed_topic, NewsItem.topic_id),
    }  # fmt: skip


def _candidate(r: NewsItem) -> Candidate:
    return Candidate(title=r.title, publisher=r.publisher, excerpt=r.excerpt)


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
