"""The Inbox (I0, API 23): one list of what needs the reader.

Nothing is copied: the list is read from the tables that own the things -

    approval         an approval the agent waits for
    late_approval    one the agent stopped waiting for, still approvable
    watch_hit        a listing a watcher's judge accepted (or its filters, without criteria)
    must_read        a story scored 9, not read yet
    feed_failing     a News feed that failed 3 rounds in a row
    switch_failed    a model switch that failed (llama-server is back on the old model, or
                     down)
    schedule_failed  a schedule whose last run failed, or that was missed

- and `inbox_done` says what the reader cleared. An approval cannot be cleared: it leaves
when it is answered or its time runs out. A hit or a story leaves when cleared (the app
clears a story it showed, a hit it opened) or after 7 days; a notice when cleared or when
its cause is gone, and it comes back if the thing fails anew (`mark`). Finished runs, the
morning brief and ordinary stories never enter.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import (
    Approval,
    Conversation,
    InboxDone,
    NewsItem,
    NewsSource,
    NewsTopic,
    Schedule,
    Watch,
    WatchItem,
)
from delve.events import EventHub
from delve.news import item_out as story_out
from delve.news import source_out as feed_out
from delve.schedules import _render as render_schedules
from delve.serialize import approval_out, iso
from delve.watches import item_out as hit_out

TZ = ZoneInfo("Europe/Budapest")

KINDS = (
    "approval",
    "late_approval",
    "watch_hit",
    "must_read",
    "feed_failing",
    "switch_failed",
    "schedule_failed",
)
# what the reader can clear; an approval is answered instead
DONEABLE = frozenset(KINDS[2:])
# what stays cleared for good (a notice is cleared for one failure: its mark)
ONCE = frozenset({"watch_hit", "must_read"})
WINDOW = timedelta(days=7)
MUST_READ_SCORE = 9
FEED_FAILS = 3


class InboxError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def item_out(
    kind: str, ref: Any, at: datetime | None, *, title: str, source: str | None, **part: Any
) -> dict[str, Any]:
    """One item. `title` is its gist (the command, the listing, the headline, the feed's
    address, the model, the schedule's name), `source` where it comes from (the chat, the
    watcher, the publisher, the topic, the model before, the error); the thing itself is
    under its own key, the others are null."""
    return {
        "id": f"{kind}:{ref}",
        "kind": kind,
        "at": iso(at),
        "title": title,
        "source": source,
        "doneable": kind in DONEABLE,
        "approval": None,
        "hit": None,
        "story": None,
        "feed": None,
        "switch": None,
        "schedule": None,
        **part,
    }


def _ts(raw: Any) -> datetime | None:
    try:
        ts = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


class InboxService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hub: EventHub,
        *,
        switch: Callable[[], dict[str, Any] | None] = lambda: None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sm = sessionmaker
        self._hub = hub
        self._switch = switch  # SystemService.switch: the failed model switch, if any
        self._clock = clock

    # --- the sources ------------------------------------------------------------------

    async def _approvals(self, s: AsyncSession, now: datetime) -> list[tuple[str, dict[str, Any]]]:
        q = (
            select(Approval, Conversation.title, Conversation.preview)
            .join(Conversation, Conversation.id == Approval.conversation_id)
            .where(
                or_(
                    Approval.status == "pending",
                    and_(Approval.status == "expired_in_hermes", Approval.expires_at > now),
                )
            )
        )
        out = []
        for a, title, preview in (await s.execute(q)).all():
            kind = "approval" if a.status == "pending" else "late_approval"
            out.append(
                ("", item_out(kind, a.request_id, a.created_at, title=a.command,
                              source=title or preview, approval=approval_out(a)))
            )  # fmt: skip
        return out

    async def _hits(self, s: AsyncSession, now: datetime) -> list[tuple[str, dict[str, Any]]]:
        at = func.coalesce(WatchItem.judged_at, WatchItem.seen_at)
        q = (
            select(WatchItem, Watch.name)
            .join(Watch, Watch.slug == WatchItem.slug)
            .where(
                WatchItem.verdict.in_(("match", "mechanical")),
                WatchItem.notify.is_(True),
                Watch.deleted_at.is_(None),
                at >= now - WINDOW,
            )
        )
        return [
            ("", item_out("watch_hit", i.id, i.judged_at or i.seen_at, title=i.title,
                          source=name, hit=hit_out(i)))
            for i, name in (await s.execute(q)).all()
        ]  # fmt: skip

    async def _stories(self, s: AsyncSession, now: datetime) -> list[tuple[str, dict[str, Any]]]:
        q = (
            select(NewsItem)
            .outerjoin(NewsSource, NewsSource.id == NewsItem.source_id)
            .outerjoin(NewsTopic, NewsTopic.id == NewsItem.topic_id)
            .where(
                NewsItem.dup_of.is_(None),
                or_(NewsItem.source_id.is_(None), NewsSource.enabled),
                or_(NewsItem.topic_id.is_(None), NewsTopic.enabled),
                NewsItem.score >= MUST_READ_SCORE,
                NewsItem.read_at.is_(None),
                NewsItem.seen_at >= now - WINDOW,
            )
        )
        return [
            ("", item_out("must_read", i.id, i.scored_at or i.seen_at, title=i.title,
                          source=i.publisher, story=story_out(i)))
            for i in (await s.execute(q)).scalars()
        ]  # fmt: skip

    async def _feeds(self, s: AsyncSession) -> list[tuple[str, dict[str, Any]]]:
        q = (
            select(NewsSource, NewsTopic.name)
            .join(NewsTopic, NewsTopic.id == NewsSource.topic_id)
            .where(NewsSource.enabled, NewsTopic.enabled, NewsSource.errors_in_row >= FEED_FAILS)
        )
        return [
            # cleared for this run of failures: a feed that worked since fails anew
            (iso(f.last_ok_at) or "",
             item_out("feed_failing", f.id, f.last_at or f.updated_at, title=f.url,
                      source=name, feed=feed_out(f)))
            for f, name in (await s.execute(q)).all()
        ]  # fmt: skip

    def _switches(self) -> list[tuple[str, dict[str, Any]]]:
        sw = self._switch()
        if not sw or sw.get("state") not in ("failed", "down"):
            return []
        at = str(sw.get("at") or "")
        return [
            (at, item_out("switch_failed", "llama", _ts(at), title=str(sw.get("to") or ""),
                          source=sw.get("from"), switch=dict(sw)))
        ]  # fmt: skip

    async def _schedules(self, s: AsyncSession) -> list[tuple[str, dict[str, Any]]]:
        rows = list((await s.execute(select(Schedule))).scalars())
        out = []
        for sc, view in zip(rows, await render_schedules(s, rows), strict=True):
            if view["last_status"] != "failed":
                continue
            missed = view["last_error"] == "missed"
            # one failure: that run, or that missed time (the next one is set by then)
            mark = "|".join(
                str(view[k] or "") for k in ("last_run_id", "last_run_at", "last_error")
            ) + (f"|{view['next_run_at']}" if missed else "")
            at = sc.updated_at if missed else (sc.last_run_at or sc.updated_at)
            out.append(
                (mark, item_out("schedule_failed", sc.id, at, title=sc.name,
                                source=view["last_error"], schedule=view))
            )  # fmt: skip
        return out

    async def _open(self, s: AsyncSession, now: datetime) -> list[tuple[str, dict[str, Any]]]:
        """Every item that is the case now, cleared or not, with its mark."""
        return [
            *await self._approvals(s, now),
            *await self._hits(s, now),
            *await self._stories(s, now),
            *await self._feeds(s),
            *self._switches(),
            *await self._schedules(s),
        ]

    # --- reads ------------------------------------------------------------------------

    async def _view(self, s: AsyncSession, now: datetime) -> dict[str, Any]:
        cleared = {(d.kind, d.ref): d for d in (await s.execute(select(InboxDone))).scalars()}
        items = []
        for mark, item in await self._open(s, now):
            kind, _, ref = item["id"].partition(":")
            d = cleared.get((kind, ref))
            if d is not None and (kind in ONCE or d.mark == mark):
                continue
            items.append(item)
        items.sort(key=lambda i: (i["at"] or "", i["id"]), reverse=True)
        midnight = datetime.combine(now.astimezone(TZ).date(), time.min, tzinfo=TZ)
        done = [
            {**d.item, "done_at": iso(d.done_at)}
            for d in sorted(cleared.values(), key=lambda d: d.done_at, reverse=True)
            if d.done_at >= midnight
        ]
        return {
            "data": items,
            "done": done,
            "open": len(items),
            "approvals": sum(1 for i in items if i["approval"] is not None),
            "updated_at": iso(now),
        }

    async def view(self) -> dict[str, Any]:
        """GET /api/v1/inbox: the open items newest first, what was cleared today (Budapest
        time; each with `done_at`), and the counts."""
        async with self._sm() as s:
            return await self._view(s, self._clock())

    # --- writes -----------------------------------------------------------------------

    def _changed(self, view: dict[str, Any]) -> None:
        self._hub.publish("inbox.updated", {"open": view["open"], "updated_at": view["updated_at"]})

    async def done(self, item_id: str) -> dict[str, Any]:
        """Clears an open item; the view after it."""
        now = self._clock()
        kind, _, ref = item_id.partition(":")
        async with self._sm() as s:
            found = next(((m, i) for m, i in await self._open(s, now) if i["id"] == item_id), None)
            if found is None:
                raise InboxError(404, "inbox_item_not_found", "no such open item")
            mark, item = found
            if kind not in DONEABLE:
                raise InboxError(409, "not_doneable", "an approval is answered, not cleared")
            await s.execute(
                insert(InboxDone)
                .values(kind=kind, ref=ref, mark=mark, item=item, done_at=now)
                .on_conflict_do_update(
                    index_elements=[InboxDone.kind, InboxDone.ref],
                    set_={"mark": mark, "item": item, "done_at": now},
                )
            )
            # a cleared hit or story cannot come back once it is past the window
            await s.execute(
                delete(InboxDone).where(
                    InboxDone.kind.in_(ONCE), InboxDone.done_at < now - WINDOW - WINDOW
                )
            )
            await s.commit()
            view = await self._view(s, now)
        self._changed(view)
        return view

    async def undo(self, item_id: str) -> dict[str, Any]:
        """Takes a clearing back (the item shows again if it is still the case)."""
        kind, _, ref = item_id.partition(":")
        async with self._sm() as s:
            gone = await s.execute(
                delete(InboxDone)
                .where(InboxDone.kind == kind, InboxDone.ref == ref)
                .returning(InboxDone.kind)
            )
            if gone.first() is None:
                raise InboxError(404, "inbox_item_not_found", "that item was not cleared")
            await s.commit()
            view = await self._view(s, self._clock())
        self._changed(view)
        return view
