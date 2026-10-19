"""Shared harness: a RunManager on a live database against the fake Hermes."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import TERMINAL_RUN_STATUSES, Message, Run
from delve.events import Envelope, EventHub, Subscription
from delve.hermes.client import HermesClient, ThinkingRoute
from delve.llama import LlamaClient
from delve.runs.manager import RunManager, Timing

from .fake_hermes import API_KEY, FIXTURES, FakeHermes, StreamingASGITransport

GGUF = "/home/operator/models/qwen38/Qwen3.8-27B-UD-IQ4_XS.gguf"
ROUTE = ThinkingRoute("custom:llama-qwen38-think", GGUF)
FAST = Timing(
    flush_interval_s=0.0,
    submit_timeout_s=3.0,
    resolve_timeout_s=3.0,
    max_backoff_s=0.02,
    title_delays_s=(0.0,),
    idle_poll_s=0.05,
)


def sse_events(name: str) -> list[dict[str, Any]]:
    text = (FIXTURES / f"{name}.events.sse").read_text(encoding="utf-8")
    return [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]


def final_output(name: str) -> str:
    return next(e for e in sse_events(name) if e["event"] == "run.completed")["output"]  # type: ignore[no-any-return]


@dataclass
class Harness:
    fake: FakeHermes
    hermes: HermesClient
    hub: EventHub
    sub: Subscription
    mgr: RunManager
    db: async_sessionmaker[AsyncSession]

    def events(self) -> list[Envelope]:
        out = []
        while not self.sub.queue.empty():
            out.append(self.sub.queue.get_nowait())
        return out

    async def settle(self) -> None:
        """Wait until every run is terminal and post-run tasks are done."""
        async with asyncio.timeout(10):
            while True:
                async with self.db() as s:
                    open_runs = await s.scalar(
                        select(func.count())
                        .select_from(Run)
                        .where(Run.status.not_in(TERMINAL_RUN_STATUSES))
                    )
                if not open_runs and self.mgr.active_run_id is None:
                    break
                await asyncio.sleep(0.01)
        await self.mgr.wait_background()

    async def send(self, text: str = "hi", **kw: Any) -> tuple[str, str]:
        r = await self.mgr.submit(text=text, client_msg_id=uuid.uuid4(), **kw)
        return r.conversation["id"], r.run_id

    async def run(self, run_id: str) -> Run:
        async with self.db() as s:
            run = await s.get(Run, run_id)
        assert run is not None
        return run

    async def messages(self, conv_id: str) -> list[Message]:
        async with self.db() as s:
            q = select(Message).where(Message.conversation_id == conv_id).order_by(Message.id)
            return list((await s.execute(q)).scalars())


@asynccontextmanager
async def make_harness(
    db: async_sessionmaker[AsyncSession],
    *,
    llama: LlamaClient | None = None,
    timing: Timing = FAST,
) -> AsyncIterator[Harness]:
    fake = FakeHermes()
    hermes = HermesClient(
        "http://hermes", API_KEY, ROUTE, transport=StreamingASGITransport(fake.app)
    )
    hub = EventHub()
    sub = hub.subscribe()
    mgr = RunManager(db, hermes, hub, timing=timing, llama=llama)
    try:
        yield Harness(fake, hermes, hub, sub, mgr, db)
    finally:
        await mgr.close()
        await hermes.aclose()
