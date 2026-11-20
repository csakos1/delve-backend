"""The Inbox (I0): what enters it, what leaves, clearing and taking that back, REST."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import (
    Approval,
    Conversation,
    NewsItem,
    NewsSource,
    NewsTopic,
    Run,
    Schedule,
    Watch,
    WatchItem,
)
from delve.events import EventHub
from delve.inbox import InboxError, InboxService

pg = pytest.mark.pg
NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)


def ago(**kw: Any) -> datetime:
    return NOW - timedelta(**kw)


def approval(rid: str, status: str, *, expires: datetime | None = None,
             created: datetime | None = None) -> Approval:  # fmt: skip
    return Approval(
        request_id=rid, run_id="r", conversation_id="c", command=f"sudo {rid}",
        command_sha256="0", action_class="other", status=status,
        created_at=created or ago(minutes=5), hermes_deadline=NOW + timedelta(minutes=4),
        expires_at=expires or NOW + timedelta(hours=4),
    )  # fmt: skip


def hit(item: str, verdict: str, *, at: datetime, notify: bool = True) -> WatchItem:
    return WatchItem(
        slug="rtx", item_id=item, card_id=item, title=f"RTX {item}", url="https://x.example/a",
        verdict=verdict, notify=notify, seen_at=at, judged_at=at,
    )  # fmt: skip


def story(key: str, score: int | None, *, at: datetime, **kw: Any) -> NewsItem:
    return NewsItem(
        key=key, kind="feed", title=f"Story {key}", url=f"https://x.example/{key}",
        publisher="Sail World", source_id="feed", topic_id="sailing", seen_at=at, scored_at=at,
        norm_title=key, score=score, **kw,
    )  # fmt: skip


async def seed(db: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    """One of everything, and beside each what must stay out."""
    sid = uuid.uuid4()
    async with db() as s:
        s.add_all([
            Conversation(id="c", title="Arch update", origin="app"),
            NewsTopic(id="sailing", name="Sailing"),
        ])  # fmt: skip
        await s.flush()
        s.add_all([
            approval("apr_wait", "pending"),
            approval("apr_late", "expired_in_hermes", created=ago(hours=6)),
            approval("apr_gone", "expired_in_hermes", expires=ago(minutes=1)),
            approval("apr_done", "approved_once"),
            Watch(slug="rtx", name="RTX 4070", source="page", url="https://x.example/",
                  filters={}, stop_after_hit=False, schedule="every 4h", def_hash="0",
                  defined_at=ago(days=30)),
            hit("h_match", "match", at=ago(hours=2)),
            hit("h_mech", "mechanical", at=ago(hours=3)),
            hit("h_reject", "reject", at=ago(hours=1)),
            hit("h_old", "match", at=ago(days=8)),
            hit("h_history", "match", at=ago(hours=1), notify=False),
            NewsSource(id="feed", topic_id="sailing", url="https://x.example/rss"),
            NewsSource(id="bad", topic_id="sailing", url="https://bad.example/rss",
                       errors_in_row=3, last_status="error", last_at=ago(minutes=30),
                       last_ok_at=ago(days=1)),
            NewsSource(id="shaky", topic_id="sailing", url="https://shaky.example/rss",
                       errors_in_row=2, last_status="error"),
        ])  # fmt: skip
        await s.flush()
        s.add_all([
            story("nine", 9, at=ago(hours=4)),
            story("eight", 8, at=ago(hours=1)),
            story("read", 9, at=ago(hours=1), read_at=ago(minutes=10)),
            story("stale", 9, at=ago(days=8)),
            Schedule(id=sid, kind="prompt", name="Morning check", cron="0 7 * * *",
                     prompt="p", conversation_mode="new", last_run_at=ago(hours=5),
                     updated_at=ago(hours=5)),
            Schedule(id=uuid.uuid4(), kind="prompt", name="Fine", cron="0 8 * * *",
                     prompt="p", conversation_mode="new"),
            Run(run_id="run_bad", conversation_id="c", idempotency_key=uuid.uuid4(),
                status="failed", input="p", thinking=False,
                origin="schedule", schedule_id=sid, queued_at=ago(hours=5), error="boom"),
        ])  # fmt: skip
        await s.commit()
    return sid


def service(db: async_sessionmaker[AsyncSession], hub: EventHub | None = None,
            switch: dict[str, Any] | None = None) -> InboxService:  # fmt: skip
    return InboxService(db, hub or EventHub(), switch=lambda: switch, clock=lambda: NOW)


FAILED_SWITCH = {"state": "failed", "from": "q6", "to": "q8", "by": "pixel",
                 "at": "2026-11-21T11:00:00+00:00", "error": "timeout",
                 "elapsed_s": 180}  # fmt: skip


@pg
async def test_what_enters_newest_first(db: async_sessionmaker[AsyncSession]) -> None:
    sid = await seed(db)
    view = await service(db, switch=FAILED_SWITCH).view()
    assert [i["id"] for i in view["data"]] == [
        "approval:apr_wait",
        "feed_failing:bad",
        "switch_failed:llama",
        "watch_hit:1",
        "watch_hit:2",
        "must_read:1",
        f"schedule_failed:{sid}",
        "late_approval:apr_late",
    ]
    assert view["open"] == 8 and view["approvals"] == 2 and view["done"] == []
    by = {i["kind"]: i for i in view["data"]}
    assert by["approval"]["title"] == "sudo apr_wait"
    assert by["approval"]["source"] == "Arch update"
    assert by["approval"]["approval"]["status"] == "pending"
    assert by["approval"]["doneable"] is False
    assert by["watch_hit"]["source"] == "RTX 4070" and by["watch_hit"]["hit"]["slug"] == "rtx"
    assert by["must_read"]["story"]["score"] == 9 and by["must_read"]["source"] == "Sail World"
    assert by["feed_failing"]["title"] == "https://bad.example/rss"
    assert by["feed_failing"]["source"] == "Sailing"
    assert by["switch_failed"]["title"] == "q8" and by["switch_failed"]["switch"]["error"]
    assert by["schedule_failed"]["title"] == "Morning check"
    assert by["schedule_failed"]["schedule"]["last_run_id"] == "run_bad"
    for item in view["data"]:
        others = [k for k in ("approval", "hit", "story", "feed", "switch", "schedule")
                  if item[k] is not None]  # fmt: skip
        assert len(others) == 1


@pg
async def test_clearing_and_taking_it_back(db: async_sessionmaker[AsyncSession]) -> None:
    await seed(db)
    hub = EventHub(validate=True)
    svc = service(db, hub)
    view = await svc.done("watch_hit:1")
    assert "watch_hit:1" not in [i["id"] for i in view["data"]] and view["open"] == 6
    (done,) = view["done"]
    assert done["id"] == "watch_hit:1" and done["title"] == "RTX h_match" and done["done_at"]
    assert hub.since(0)[-1].type == "inbox.updated"  # type: ignore[index]
    assert hub.since(0)[-1].data["open"] == 6  # type: ignore[index]

    # an approval is answered, not cleared; what is not the case cannot be cleared;
    # clearing twice is the same as once
    with pytest.raises(InboxError) as err:
        await svc.done("approval:apr_wait")
    assert (err.value.status, err.value.code) == (409, "not_doneable")
    with pytest.raises(InboxError) as err:
        await svc.done("watch_hit:99")
    assert err.value.status == 404
    assert len((await svc.done("watch_hit:1"))["done"]) == 1

    view = await svc.undo("watch_hit:1")
    assert "watch_hit:1" in [i["id"] for i in view["data"]] and view["done"] == []
    with pytest.raises(InboxError):
        await svc.undo("watch_hit:1")


@pg
async def test_a_cleared_notice_comes_back_when_it_fails_anew(
    db: async_sessionmaker[AsyncSession],
) -> None:
    await seed(db)
    svc = service(db, switch=FAILED_SWITCH)
    await svc.done("feed_failing:bad")
    view = await svc.done("switch_failed:llama")
    kinds = {i["kind"] for i in view["data"]}
    assert "feed_failing" not in kinds and "switch_failed" not in kinds

    # the same failures: still cleared
    assert "feed_failing" not in {i["kind"] for i in (await svc.view())["data"]}
    # the feed worked in between and fails again; another switch failed
    async with db() as s:
        feed = await s.get(NewsSource, "bad")
        assert feed is not None
        feed.last_ok_at = ago(minutes=5)
        await s.commit()
    again = service(db, switch={**FAILED_SWITCH, "at": "2026-11-21T11:30:00+00:00"})
    kinds = {i["kind"] for i in (await again.view())["data"]}
    assert {"feed_failing", "switch_failed"} <= kinds


@pg
async def test_done_today_ends_at_local_midnight(db: async_sessionmaker[AsyncSession]) -> None:
    await seed(db)
    await service(db).done("must_read:1")
    # 23:30 UTC is 00:30 the next day in Budapest
    later = InboxService(db, EventHub(), clock=lambda: NOW.replace(hour=23, minute=30))
    view = await later.view()
    assert view["done"] == []
    assert "must_read:1" not in [i["id"] for i in view["data"]]


@pg
def test_api_inbox(client: TestClient, token: str) -> None:
    a = {"Authorization": f"Bearer {token}"}
    assert client.get("/api/v1/inbox").status_code == 401
    view = client.get("/api/v1/inbox", headers=a).json()
    assert view["data"] == [] and view["done"] == [] and view["open"] == 0
    r = client.post("/api/v1/inbox/done", headers=a, json={"id": "watch_hit:9"})
    assert r.status_code == 404 and r.json()["detail"]["code"] == "inbox_item_not_found"
    r = client.post("/api/v1/inbox/undo", headers=a, json={"id": "watch_hit:9"})
    assert r.status_code == 404
    assert client.post("/api/v1/inbox/done", headers=a, json={}).status_code == 422
