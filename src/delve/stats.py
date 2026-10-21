"""Observability (B5): the numbers behind the app's Observability screen.

Six series come from tables the backend keeps anyway, so they reach back to the first
run: generation and prefill speed (`runs.perf`), the cache hit ratio and tokens per day
(`runs.usage`), the queue length (`runs.queued_at` .. `started_at`) and the approval
latency (`approvals.created_at` .. `responded_at`, answers from a person only).

GPU temperature and power have no such source: the sampler here asks the helper for
`gpu.status` every DELVE_STATS_SAMPLE_S and writes `gpu_samples` (the system view of B3
itself keeps no history). The same loop drops samples older than
DELVE_GPU_RETENTION_DAYS and `push_log` rows older than DELVE_PUSH_LOG_RETENTION_DAYS,
once an hour.

Buckets follow Europe/Budapest, like the schedules: hours for 24 h, six hours (00, 06,
12, 18) for 7 d, days for 30 d; the last bucket holds the current moment. The GPU series
use finer buckets (10 min, 1 h, 6 h) over the same span.
"""

from __future__ import annotations

import asyncio
import bisect
import contextlib
import logging
import statistics
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import ARRAY, and_, bindparam, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import TZ as TZ_TYPE
from delve.db.models import Approval, GpuSample, PushLog, Run
from delve.runs.late import AUTO_DECIDED_BY
from delve.system import HelperClient, HelperFailed, HelperUnavailable

log = logging.getLogger("delve.stats")

TZ = ZoneInfo("Europe/Budapest")
PREFILL_MIN_PROMPT_TOKENS = 500  # below this the prefill speed is noise (plan 12/14)
CLEANUP_EVERY_S = 3600.0
HELPER_TIMEOUT_S = 15.0


@dataclass(frozen=True)
class RangeSpec:
    bucket: timedelta
    count: int
    gpu_bucket: timedelta
    days: int  # tokens per day: how many days


RANGES: dict[str, RangeSpec] = {
    "24h": RangeSpec(timedelta(hours=1), 24, timedelta(minutes=10), 7),
    "7d": RangeSpec(timedelta(hours=6), 28, timedelta(hours=1), 7),
    "30d": RangeSpec(timedelta(days=1), 30, timedelta(hours=6), 30),
}

_LOCAL_STEP = timedelta(hours=6)  # steps this long or longer follow the wall clock


# --- buckets (pure) ------------------------------------------------------------------


def _floor(now: datetime, step: timedelta) -> datetime:
    """The start of the bucket holding `now`: UTC arithmetic below six hours (local hour
    and ten-minute boundaries are the UTC ones), local wall time from six hours up."""
    if step < _LOCAL_STEP:
        s = int(step.total_seconds())
        epoch = int(now.timestamp())
        return datetime.fromtimestamp(epoch - epoch % s, UTC)
    local = now.astimezone(TZ)
    if step >= timedelta(days=1):
        return local.replace(hour=0, minute=0, second=0, microsecond=0)
    hours = int(step.total_seconds() // 3600)
    return local.replace(hour=local.hour - local.hour % hours, minute=0, second=0, microsecond=0)


def _add(t: datetime, step: timedelta, k: int) -> datetime:
    if step < _LOCAL_STEP:
        return (t + k * step).astimezone(UTC)
    # wall-clock arithmetic: a DST day is 23 or 25 hours long, its bucket too
    return (t.astimezone(TZ).replace(tzinfo=None) + k * step).replace(tzinfo=TZ).astimezone(UTC)


def bucket_edges(now: datetime, step: timedelta, count: int) -> list[datetime]:
    """`count + 1` edges in UTC; the last bucket holds `now`."""
    cur = _floor(now, step)
    return [_add(cur, step, k) for k in range(-(count - 1), 2)]


def edges_between(start: datetime, end: datetime, step: timedelta) -> list[datetime]:
    out = [start.astimezone(UTC)]
    while out[-1] < end:
        out.append(_add(start, step, len(out)))
    return out


def bucket_of(edges: Sequence[datetime], t: datetime) -> int | None:
    i = bisect.bisect_right(edges, t) - 1
    return i if 0 <= i < len(edges) - 1 else None


def queue_peaks(
    intervals: Iterable[tuple[datetime, datetime]], edges: Sequence[datetime]
) -> list[int]:
    """The most runs waiting at once in each bucket; an interval is [queued, left)."""
    lo, hi = edges[0], edges[-1]
    events: list[tuple[datetime, int]] = []
    for start, end in intervals:
        if end <= start or end <= lo or start >= hi:
            continue
        events.append((max(start, lo), 1))
        if end < hi:
            events.append((end, -1))
    events.sort()  # at the same instant -1 comes before +1: [start, end) intervals
    out = []
    level = j = 0
    for i in range(len(edges) - 1):
        b_start, b_end = edges[i], edges[i + 1]
        while j < len(events) and events[j][0] <= b_start:
            level += events[j][1]
            j += 1
        peak = level
        while j < len(events) and events[j][0] < b_end:
            level += events[j][1]
            peak = max(peak, level)
            j += 1
        out.append(peak)
    return out


def _num(d: Any, key: str) -> float | None:
    v = d.get(key) if isinstance(d, dict) else None
    if isinstance(v, int | float) and not isinstance(v, bool):
        return float(v)
    return None


def _round(v: float | None, nd: int = 1) -> float | None:
    return None if v is None else round(v, nd)


def _points(edges: Sequence[datetime], values: Sequence[Any]) -> list[dict[str, Any]]:
    return [{"t": e.isoformat(), "v": v} for e, v in zip(edges, values, strict=False)]


def _series(
    value: Any, edges: Sequence[datetime], values: Sequence[Any], **extra: Any
) -> dict[str, Any]:
    return {"value": value, **extra, "points": _points(edges, values)}


@dataclass
class RunRow:
    status: str
    queued_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    usage: dict[str, Any] | None
    perf: dict[str, Any] | None


@dataclass
class GpuBucket:
    """One GPU's averages per bucket, and its last sample."""

    index: int
    name: str | None
    temp: list[float | None]
    power: list[float | None]
    last_temp: float | None
    last_power: float | None


def _tokens(usage: dict[str, Any] | None) -> int:
    total = _num(usage, "total_tokens")
    if total is None:
        total = (_num(usage, "input_tokens") or 0) + (_num(usage, "output_tokens") or 0)
    return int(total)


def stats_out(
    *,
    range_key: str,
    now: datetime,
    runs: Sequence[RunRow],
    latencies: Sequence[tuple[datetime, float]],
    queued_now: int,
    gpus: Sequence[GpuBucket],
    gpu_edges: Sequence[datetime],
    sampling: dict[str, Any],
) -> dict[str, Any]:
    spec = RANGES[range_key]
    edges = bucket_edges(now, spec.bucket, spec.count)
    n = spec.count
    lo, hi = edges[0], edges[-1]

    gen: list[list[float]] = [[] for _ in range(n)]
    pre: list[list[float]] = [[] for _ in range(n)]
    cache_read = [0.0] * n
    cache_in = [0.0] * n
    last_gen: tuple[datetime, float] | None = None
    last_pre: tuple[datetime, float] | None = None
    waiting: list[tuple[datetime, datetime]] = []

    day_edges = bucket_edges(now, timedelta(days=1), spec.days)
    day_tokens = [0] * spec.days

    for r in runs:
        waiting.append((r.queued_at, r.started_at or r.ended_at or now))
        if r.ended_at is None:
            continue
        d = bucket_of(day_edges, r.ended_at)
        if d is not None and r.usage:
            day_tokens[d] += _tokens(r.usage)
        i = bucket_of(edges, r.ended_at)
        if i is None or r.status != "completed":
            continue
        tps = _num(r.perf, "gen_tps")
        if tps is not None and tps > 0:
            gen[i].append(tps)
            if last_gen is None or r.ended_at >= last_gen[0]:
                last_gen = (r.ended_at, tps)
        ptps = _num(r.perf, "prefill_tps")
        prompt = _num(r.perf, "prompt_tokens")
        if ptps is not None and ptps > 0 and (prompt or 0) >= PREFILL_MIN_PROMPT_TOKENS:
            pre[i].append(ptps)
            if last_pre is None or r.ended_at >= last_pre[0]:
                last_pre = (r.ended_at, ptps)
        inp = _num(r.usage, "input_tokens")
        if inp:
            cache_in[i] += inp
            cache_read[i] += _num(r.usage, "cache_read_tokens") or 0

    lat: list[list[float]] = [[] for _ in range(n)]
    for at, seconds in latencies:
        i = bucket_of(edges, at)
        if i is not None:
            lat[i].append(seconds)
    all_lat = [s for bucket in lat for s in bucket]
    total_in, total_read = sum(cache_in), sum(cache_read)

    def mean(xs: list[float]) -> float | None:
        return round(statistics.fmean(xs), 1) if xs else None

    def pct(read: float, inp: float) -> float | None:
        return round(100 * read / inp, 1) if inp else None

    def median_s(xs: list[float]) -> int | None:
        return round(statistics.median(xs)) if xs else None

    return {
        "range": range_key,
        "from": lo.isoformat(),
        "to": hi.isoformat(),
        "now": now.isoformat(),
        "tz": "Europe/Budapest",
        "bucket_s": int(spec.bucket.total_seconds()),
        "gpu_bucket_s": int(spec.gpu_bucket.total_seconds()),
        "generation": _series(_round(last_gen[1]) if last_gen else None, edges[:-1],
                              [mean(b) for b in gen]),
        "prefill": _series(
            _round(last_pre[1], 0) if last_pre else None, edges[:-1], [mean(b) for b in pre]
        ),
        "prefill_min_prompt_tokens": PREFILL_MIN_PROMPT_TOKENS,
        "queue": _series(queued_now, edges[:-1], queue_peaks(waiting, edges)),
        "approval_latency": _series(median_s(all_lat), edges[:-1], [median_s(b) for b in lat]),
        "cache_hit": _series(pct(total_read, total_in), edges[:-1],
                             [pct(r, i) for r, i in zip(cache_read, cache_in, strict=True)]),
        "tokens_per_day": {
            "value": day_tokens[-1],
            "days": [
                {"date": e.astimezone(TZ).date().isoformat(), "tokens": t}
                for e, t in zip(day_edges, day_tokens, strict=False)
            ],
        },
        "gpus": [
            {
                "index": g.index,
                "name": g.name,
                "temp": _series(_round(g.last_temp, 0), gpu_edges[:-1], g.temp),
                "power": _series(_round(g.last_power, 0), gpu_edges[:-1], g.power),
            }
            for g in gpus
        ],
        "sampling": sampling,
    }  # fmt: skip


# --- the service ---------------------------------------------------------------------


class StatsService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        helper: HelperClient,
        *,
        sample_s: float = 60.0,
        gpu_retention_days: int = 30,
        push_log_retention_days: int = 90,
        cleanup_s: float = CLEANUP_EVERY_S,
    ) -> None:
        self._sm = sessionmaker
        self._helper = helper
        self._sample_s = sample_s
        self._gpu_keep = timedelta(days=gpu_retention_days)
        self._push_keep = timedelta(days=push_log_retention_days)
        self._cleanup_s = cleanup_s
        self._task: asyncio.Task[None] | None = None
        self.last_sample_at: datetime | None = None
        self.last_error: str | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="stats-sampler")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        next_cleanup = 0.0
        loop = asyncio.get_running_loop()
        while True:
            if self._sample_s > 0:
                await self.sample()
            if loop.time() >= next_cleanup:
                try:
                    await self.cleanup()
                except Exception:
                    log.exception("stats cleanup failed")
                next_cleanup = loop.time() + self._cleanup_s
            await asyncio.sleep(self._sample_s if self._sample_s > 0 else self._cleanup_s)

    async def sample(self, *, now: datetime | None = None) -> int:
        """One `gpu.status` reading into `gpu_samples`; the number of rows written."""
        ts = (now or datetime.now(UTC)).replace(microsecond=0)
        try:
            data = await self._helper.call("gpu.status", timeout_s=HELPER_TIMEOUT_S)
        except (HelperUnavailable, HelperFailed) as exc:
            err = f"{type(exc).__name__}: {exc}"[:200]
            if err != self.last_error:  # once per kind of failure, not every minute
                log.warning("gpu sample failed", extra={"error": err})
            self.last_error = err
            return 0
        gpus = data.get("gpus")
        rows = []
        for g in gpus if isinstance(gpus, list) else []:
            index = g.get("index") if isinstance(g, dict) else None
            if not isinstance(index, int) or isinstance(index, bool):
                continue
            mem = _num(g, "mem_used_mib")
            rows.append(
                GpuSample(
                    ts=ts, gpu_index=index,
                    name=g["name"] if isinstance(g.get("name"), str) else None,
                    temp_c=_num(g, "temp_c"), power_w=_num(g, "power_w"),
                    util_pct=_num(g, "util_pct"), mem_used_mib=None if mem is None else int(mem),
                )
            )  # fmt: skip
        try:
            async with self._sm() as s:
                for row in rows:
                    await s.merge(row)  # the same second twice (a manual sample): keep one
                await s.commit()
        except Exception as exc:
            err = f"db: {type(exc).__name__}"
            if err != self.last_error:
                log.warning("gpu sample not saved", extra={"error": err})
            self.last_error = err
            return 0
        if self.last_error is not None:
            log.info("gpu sampling works again")
        self.last_error = None
        self.last_sample_at = ts
        return len(rows)

    async def cleanup(self, *, now: datetime | None = None) -> tuple[int, int]:
        now = now or datetime.now(UTC)
        async with self._sm() as s:
            gpu = await s.execute(delete(GpuSample).where(GpuSample.ts < now - self._gpu_keep))
            push = await s.execute(delete(PushLog).where(PushLog.sent_at < now - self._push_keep))
            await s.commit()
        counts = (int(getattr(gpu, "rowcount", 0) or 0), int(getattr(push, "rowcount", 0) or 0))
        if any(counts):
            log.info("old stats dropped", extra={"gpu_samples": counts[0], "push_log": counts[1]})
        return counts

    async def stats(self, range_key: str, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(UTC)
        spec = RANGES[range_key]
        edges = bucket_edges(now, spec.bucket, spec.count)
        days = bucket_edges(now, timedelta(days=1), spec.days)
        lo = min(edges[0], days[0])
        hi = edges[-1]
        gpu_edges = edges_between(edges[0], hi, spec.gpu_bucket)
        async with self._sm() as s:
            runs = await self._runs(s, lo, hi)
            latencies = await self._latencies(s, edges[0], hi)
            queued_now = int(
                await s.scalar(select(func.count()).select_from(Run).where(Run.status == "queued"))
                or 0
            )
            gpus = await self._gpus(s, gpu_edges)
            last = await s.scalar(select(func.max(GpuSample.ts)))
        sampling = {
            "interval_s": int(self._sample_s),
            "ok": self.last_error is None,
            "error": self.last_error,
            "last_at": last.isoformat() if last is not None else None,
        }
        return stats_out(
            range_key=range_key, now=now, runs=runs, latencies=latencies,
            queued_now=queued_now, gpus=gpus, gpu_edges=gpu_edges, sampling=sampling,
        )  # fmt: skip

    async def _runs(self, s: AsyncSession, lo: datetime, hi: datetime) -> list[RunRow]:
        left = func.coalesce(Run.started_at, Run.ended_at, func.now())
        q = select(
            Run.status, Run.queued_at, Run.started_at, Run.ended_at, Run.usage, Run.perf
        ).where(
            Run.queued_at < hi,
            or_(left > lo, and_(Run.ended_at.is_not(None), Run.ended_at >= lo)),
        )
        return [RunRow(*row) for row in (await s.execute(q)).all()]

    async def _latencies(
        self, s: AsyncSession, lo: datetime, hi: datetime
    ) -> list[tuple[datetime, float]]:
        q = select(Approval.responded_at, Approval.created_at).where(
            Approval.responded_at.is_not(None),
            Approval.responded_at >= lo,
            Approval.responded_at < hi,
            or_(Approval.decided_by.is_(None), Approval.decided_by != AUTO_DECIDED_BY),
        )
        out: list[tuple[datetime, float]] = []
        for responded, created in (await s.execute(q)).all():
            if responded is not None:  # filtered above; for the type checker
                out.append((responded, max(0.0, (responded - created).total_seconds())))
        return out

    async def _gpus(self, s: AsyncSession, edges: Sequence[datetime]) -> list[GpuBucket]:
        n = len(edges) - 1
        bucket = func.width_bucket(
            GpuSample.ts, bindparam("edges", list(edges), type_=ARRAY(TZ_TYPE))
        )
        q = (
            select(
                GpuSample.gpu_index,
                bucket.label("b"),
                func.avg(GpuSample.temp_c),
                func.avg(GpuSample.power_w),
            )
            .where(GpuSample.ts >= edges[0], GpuSample.ts < edges[-1])
            .group_by(GpuSample.gpu_index, "b")
        )
        temp: dict[int, list[float | None]] = defaultdict(lambda: [None] * n)
        power: dict[int, list[float | None]] = defaultdict(lambda: [None] * n)
        for index, b, t, p in (await s.execute(q)).all():
            k = int(b) - 1  # width_bucket counts from 1 for [edges[0], edges[1])
            if 0 <= k < n:
                temp[index][k] = _round(None if t is None else float(t))
                power[index][k] = _round(None if p is None else float(p))
        # the newest sample per GPU in the range: name and the headline values
        newest = (
            select(GpuSample.gpu_index, func.max(GpuSample.ts).label("ts"))
            .where(GpuSample.ts >= edges[0], GpuSample.ts < edges[-1])
            .group_by(GpuSample.gpu_index)
            .subquery()
        )
        latest = (
            select(GpuSample)
            .join(
                newest,
                and_(GpuSample.gpu_index == newest.c.gpu_index, GpuSample.ts == newest.c.ts),
            )
            .order_by(GpuSample.gpu_index)
        )
        out = []
        for row in (await s.execute(latest)).scalars():
            out.append(
                GpuBucket(
                    index=row.gpu_index, name=row.name, temp=temp[row.gpu_index],
                    power=power[row.gpu_index], last_temp=row.temp_c, last_power=row.power_w,
                )
            )  # fmt: skip
        return out
