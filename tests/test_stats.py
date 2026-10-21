"""Observability (B5): buckets, the series, GPU sampling and cleanup, `GET /stats`."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Approval, Conversation, GpuSample, PushLog, Run
from delve.runs.late import AUTO_DECIDED_BY
from delve.stats import (
    RANGES,
    TZ,
    GpuBucket,
    RunRow,
    StatsService,
    bucket_edges,
    bucket_of,
    edges_between,
    queue_peaks,
    stats_out,
)
from delve.system import HelperClient

from .fake_helper import FakeHelper
from .test_api import auth

pg = pytest.mark.pg

# Wed 21 Oct 2026 20:41:12 in Budapest (CEST, UTC+2)
NOW = datetime(2026, 10, 21, 18, 41, 12, tzinfo=UTC)
SAMPLING = {"interval_s": 60, "ok": True, "error": None, "last_at": None}


def at(hours_ago: float) -> datetime:
    return NOW - timedelta(hours=hours_ago)


# --- buckets ---------------------------------------------------------------------------


def test_24h_hourly_buckets_end_with_the_current_hour() -> None:
    edges = bucket_edges(NOW, timedelta(hours=1), 24)
    assert len(edges) == 25
    assert edges[-2] == datetime(2026, 10, 21, 18, 0, tzinfo=UTC)  # 20:00 local holds now
    assert edges[-1] == datetime(2026, 10, 21, 19, 0, tzinfo=UTC)
    assert edges[0] == datetime(2026, 10, 20, 19, 0, tzinfo=UTC)
    assert bucket_of(edges, NOW) == 23
    assert bucket_of(edges, edges[0]) == 0
    assert bucket_of(edges, edges[-1]) is None
    assert bucket_of(edges, edges[0] - timedelta(seconds=1)) is None


def test_7d_buckets_follow_local_six_hours() -> None:
    edges = bucket_edges(NOW, timedelta(hours=6), 28)
    local = [e.astimezone(TZ) for e in edges]
    assert {t.hour for t in local} == {0, 6, 12, 18}
    assert local[-2] == datetime(2026, 10, 21, 18, 0, tzinfo=TZ)
    assert local[-1] == datetime(2026, 10, 22, 0, 0, tzinfo=TZ)
    assert len(edges) == 29


def test_30d_days_are_local_midnights_across_dst() -> None:
    now = datetime(2026, 11, 2, 10, 0, tzinfo=UTC)  # after DST ended on Sun 25 Oct
    edges = bucket_edges(now, timedelta(days=1), 30)
    local = [e.astimezone(TZ) for e in edges]
    assert all((t.hour, t.minute) == (0, 0) for t in local)
    assert local[-1].date().isoformat() == "2026-11-03"
    lengths = {
        t.date().isoformat(): (b - a).total_seconds() / 3600
        for t, a, b in zip(local, edges, edges[1:], strict=False)
    }
    assert lengths["2026-10-25"] == 25  # the day the clocks went back
    assert lengths["2026-10-24"] == lengths["2026-10-26"] == 24


def test_gpu_buckets_divide_the_same_span() -> None:
    for key, spec in RANGES.items():
        edges = bucket_edges(NOW, spec.bucket, spec.count)
        fine = edges_between(edges[0], edges[-1], spec.gpu_bucket)
        assert (fine[0], fine[-1]) == (edges[0], edges[-1]), key
    spec = RANGES["24h"]
    edges = bucket_edges(NOW, spec.bucket, spec.count)
    assert len(edges_between(edges[0], edges[-1], spec.gpu_bucket)) == 145


def test_queue_peaks() -> None:
    edges = bucket_edges(NOW, timedelta(hours=1), 24)
    b = edges[20]
    peaks = queue_peaks(
        [
            (b + timedelta(minutes=5), b + timedelta(minutes=20)),
            (b + timedelta(minutes=10), b + timedelta(minutes=70)),  # into the next hour
            (b + timedelta(minutes=20), b + timedelta(minutes=30)),  # starts as #1 leaves
            (edges[0] - timedelta(hours=2), edges[0] + timedelta(minutes=1)),  # from before
            (b, b),  # never waited
        ],
        edges,
    )
    assert peaks[0] == 1
    assert peaks[20] == 2
    assert peaks[21] == 1
    assert peaks[22] == 0
    assert sum(peaks) == 4


# --- the series --------------------------------------------------------------------------


def run(
    ended_h: float,
    *,
    status: str = "completed",
    gen: float | None = None,
    prefill: float | None = None,
    prompt: int = 0,
    inp: int = 0,
    read: int = 0,
    out: int = 0,
    waited_min: float = 0,
) -> RunRow:
    ended = at(ended_h)
    started = ended - timedelta(minutes=1)
    perf: dict[str, Any] = {"prompt_tokens": prompt}
    if gen is not None:
        perf["gen_tps"] = gen
    if prefill is not None:
        perf["prefill_tps"] = prefill
    usage = {"input_tokens": inp, "output_tokens": out, "total_tokens": inp + out,
             "cache_read_tokens": read}  # fmt: skip
    return RunRow(status, started - timedelta(minutes=waited_min), started, ended, usage, perf)


def test_series_from_runs() -> None:
    runs = [
        run(0.2, gen=21.0, prefill=600.0, prompt=1200, inp=1000, read=800, out=50),
        run(0.4, gen=23.0, prefill=9000.0, prompt=40, inp=1000, read=900, out=50),  # tiny prompt
        run(5.0, gen=20.0, inp=2000, read=1000, out=100, waited_min=3),
        run(5.1, status="failed", gen=99.0, inp=500, out=5),  # failed: tokens count, speed not
        run(30.0, gen=10.0, inp=4000, out=10),  # yesterday-ish: out of the 24 h, in the days
    ]
    lat = [(at(1.0), 30.0), (at(1.2), 90.0), (at(2.0), 42.0), (at(26.0), 999.0)]
    gpu = GpuBucket(0, "NVIDIA GeForce RTX 4070 SUPER", [None] * 144, [None] * 144, 71.4, 187.2)
    hours = bucket_edges(NOW, timedelta(hours=1), 24)
    out = stats_out(
        range_key="24h", now=NOW, runs=runs, latencies=lat, queued_now=2, gpus=[gpu],
        gpu_edges=edges_between(hours[0], hours[-1], timedelta(minutes=10)),
        sampling=SAMPLING,
    )  # fmt: skip
    assert (out["range"], out["bucket_s"], out["gpu_bucket_s"]) == ("24h", 3600, 600)
    assert out["from"] == "2026-10-20T19:00:00+00:00"
    gen = out["generation"]
    assert gen["value"] == 21.0  # the newest completed run
    assert len(gen["points"]) == 24
    assert gen["points"][23] == {"t": "2026-10-21T18:00:00+00:00", "v": 22.0}
    assert gen["points"][18]["v"] == 20.0  # 5 h ago; the failed run's 99 stays out
    assert gen["points"][0]["v"] is None
    pre = out["prefill"]
    assert (pre["value"], pre["points"][23]["v"]) == (600, 600.0)
    assert out["prefill_min_prompt_tokens"] == 500
    assert out["cache_hit"]["points"][23]["v"] == 85.0  # 1700 / 2000
    assert out["cache_hit"]["value"] == 67.5  # 2700 / 4000 over the range, completed runs
    lat_s = out["approval_latency"]
    assert lat_s["value"] == 42  # median of 30, 90, 42
    assert lat_s["points"][22]["v"] == 60  # 30 and 90 in the same hour
    assert lat_s["points"][21]["v"] == 42
    q = out["queue"]
    assert q["value"] == 2
    assert q["points"][18]["v"] == 1
    days = out["tokens_per_day"]
    assert [d["date"] for d in days["days"]][-2:] == ["2026-10-20", "2026-10-21"]
    assert len(days["days"]) == 7
    assert days["value"] == 1050 + 1050 + 2100 + 505
    assert days["days"][-2]["tokens"] == 4010
    g = out["gpus"][0]
    assert (g["index"], g["temp"]["value"], g["power"]["value"]) == (0, 71, 187)
    assert len(g["temp"]["points"]) == 144


def test_empty_range() -> None:
    out = stats_out(
        range_key="30d", now=NOW, runs=[], latencies=[], queued_now=0, gpus=[],
        gpu_edges=[at(1), at(0)], sampling=SAMPLING,
    )  # fmt: skip
    assert out["generation"]["value"] is None
    assert out["approval_latency"]["value"] is None
    assert out["cache_hit"]["value"] is None
    assert {p["v"] for p in out["queue"]["points"]} == {0}
    assert len(out["generation"]["points"]) == 30
    assert out["tokens_per_day"]["value"] == 0 and len(out["tokens_per_day"]["days"]) == 30
    assert out["gpus"] == []


# --- the service on a database -------------------------------------------------------------


@pytest.fixture
def helper() -> Iterator[FakeHelper]:
    fake = FakeHelper().start()
    try:
        yield fake
    finally:
        fake.stop()


async def _seed(db: async_sessionmaker[AsyncSession], now: datetime) -> None:
    def at(hours_ago: float) -> datetime:
        return now - timedelta(hours=hours_ago)

    async with db() as s:
        s.add(Conversation(id="c", thinking=False))
        await s.flush()

        def add_run(rid: str, ended: datetime | None, status: str, **kw: Any) -> None:
            s.add(Run(run_id=rid, conversation_id="c", idempotency_key=uuid.uuid4(),
                      status=status, input="x", thinking=False, **kw))  # fmt: skip

        add_run("r1", at(0.5), "completed", queued_at=at(0.6), started_at=at(0.55),
                ended_at=at(0.5), usage={"input_tokens": 1000, "output_tokens": 10,
                "total_tokens": 1010, "cache_read_tokens": 900},
                perf={"gen_tps": 21.4, "prefill_tps": 642.0, "prompt_tokens": 800})  # fmt: skip
        add_run("r2", None, "queued", queued_at=at(0.1))
        add_run("r3", None, "queued", queued_at=at(0.05))
        add_run("old", at(24 * 40), "completed", queued_at=at(24 * 40), started_at=at(24 * 40),
                ended_at=at(24 * 40), perf={"gen_tps": 5.0})  # fmt: skip
        await s.flush()
        for rid, by, mins in (("a1", "pixel", 0.5), ("a2", AUTO_DECIDED_BY, 2)):
            s.add(
                Approval(
                    request_id=rid, run_id="r1", conversation_id="c", command="ls",
                    command_sha256="0" * 64, action_class="other", status="approved_once",
                    choice="once", decided_by=by, created_at=at(0.6),
                    responded_at=at(0.6) + timedelta(minutes=mins),
                    hermes_deadline=at(0.5), expires_at=at(-1),
                )
            )  # fmt: skip
        await s.commit()


@pg
async def test_sample_and_stats(db: async_sessionmaker[AsyncSession], helper: FakeHelper) -> None:
    svc = StatsService(db, HelperClient(helper.path))
    now = datetime.now(UTC)
    await _seed(db, now)
    assert await svc.sample(now=now - timedelta(minutes=2)) == 2
    assert await svc.sample(now=now - timedelta(minutes=1)) == 2
    assert await svc.sample(now=now - timedelta(minutes=1)) == 2  # same second: kept once
    async with db() as s:
        assert await s.scalar(select(func.count()).select_from(GpuSample)) == 4

    out = await svc.stats("24h", now=now)
    assert out["generation"]["value"] == 21.4
    assert out["prefill"]["value"] == 642
    assert out["queue"]["value"] == 2
    assert out["queue"]["points"][-1]["v"] == 2
    assert out["approval_latency"]["value"] == 30  # the automatic late approval stays out
    assert out["cache_hit"]["value"] == 90.0
    day = (now - timedelta(hours=0.5)).astimezone(TZ).date().isoformat()  # r1's local day
    assert {d["date"]: d["tokens"] for d in out["tokens_per_day"]["days"]}[day] == 1010
    gpus = out["gpus"]
    assert [(g["index"], g["name"]) for g in gpus] == [
        (0, "NVIDIA GeForce RTX 4070 SUPER"),
        (1, "NVIDIA GeForce RTX 3050"),
    ]
    assert gpus[0]["temp"]["value"] == 51
    assert gpus[0]["power"]["value"] == 40
    assert {p["v"] for p in gpus[1]["power"]["points"]} - {None} == {14.5}
    assert out["sampling"]["ok"] is True
    assert out["sampling"]["last_at"] is not None

    out30 = await svc.stats("30d", now=now)
    assert len(out30["generation"]["points"]) == 30
    assert out30["generation"]["value"] == 21.4  # the 40-day-old run is out of range


@pg
async def test_sample_failure_is_reported_once(
    db: async_sessionmaker[AsyncSession], helper: FakeHelper, caplog: pytest.LogCaptureFixture
) -> None:
    svc = StatsService(db, HelperClient(helper.path))
    helper.fail["gpu.status"] = ("nvidia_smi_failed", "NVIDIA-SMI has failed")
    with caplog.at_level("WARNING", logger="delve.stats"):
        assert await svc.sample() == 0
        assert await svc.sample() == 0
    assert svc.last_error is not None and "nvidia_smi_failed" in svc.last_error
    assert sum("gpu sample failed" in r.getMessage() for r in caplog.records) == 1
    out = await svc.stats("24h")
    assert out["sampling"]["ok"] is False and out["gpus"] == []
    del helper.fail["gpu.status"]
    assert await svc.sample() == 2
    assert svc.last_error is None


@pg
async def test_cleanup_keeps_the_retention(db: async_sessionmaker[AsyncSession]) -> None:
    svc = StatsService(
        db, HelperClient(Path("/nonexistent/h.sock")), gpu_retention_days=30,
        push_log_retention_days=90,
    )  # fmt: skip
    now = datetime.now(UTC)
    async with db() as s:
        for days in (31, 29):
            s.add(GpuSample(ts=now - timedelta(days=days), gpu_index=0, temp_c=40.0))
        for days in (91, 89):
            s.add(PushLog(kind="water", ok=True, sent_at=now - timedelta(days=days)))
        await s.commit()
    assert await svc.cleanup(now=now) == (1, 1)
    assert await svc.cleanup(now=now) == (0, 0)
    async with db() as s:
        assert await s.scalar(select(func.count()).select_from(GpuSample)) == 1
        assert await s.scalar(select(func.count()).select_from(PushLog)) == 1


@pg
async def test_get_stats(client: TestClient, token: str) -> None:
    r = client.get("/api/v1/stats", headers=auth(token))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["range"] == "24h" and len(body["generation"]["points"]) == 24
    assert body["tz"] == "Europe/Budapest"
    r = client.get("/api/v1/stats?range=7d", headers=auth(token))
    assert r.status_code == 200 and len(r.json()["queue"]["points"]) == 28
    assert client.get("/api/v1/stats?range=1y", headers=auth(token)).status_code == 422
    assert client.get("/api/v1/stats").status_code == 401
