"""Context fill and per-run llama counters (plan 6.13)."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Conversation
from delve.events import Envelope
from delve.llama import LlamaClient, Slot, parse_metrics
from delve.runs.context import ContextTracker, Measurement, throughput

from .fake_hermes import FIXTURES, StreamingASGITransport, load_json, load_text
from .fake_llama import FakeLlama
from .harness import FAST, Harness, make_harness

RUN = load_json("protocol/a.create.json")["run_id"]


def recording() -> list[dict[str, Any]]:
    text = (FIXTURES / "llama/slots-run.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def replay(name: str) -> tuple[ContextTracker, Slot, dict[str, Any], int]:
    """Feed one recorded run to a tracker the way the meter does."""
    rows = recording()
    marks = [(i, r) for i, r in enumerate(rows) if r["kind"] == "mark" and r.get("run") == name]
    send = next(i for i, r in marks if r["event"] == "send")
    started = next(i for i, r in marks if r["event"] == "run.started")
    finished, fin = next((i, r) for i, r in marks if r["event"] == "run.finished")
    tools = sum(1 for _, r in marks if r["event"] == "tool.started")

    def slot(r: dict[str, Any]) -> Slot:
        return Slot.parse(r["slots"][0])

    tr = ContextTracker()
    tr.prime(slot(next(r for r in reversed(rows[:send]) if r["kind"] == "slots")))
    for r in rows[started:finished]:
        if r["kind"] == "slots":
            tr.observe(slot(r))
    after = slot(next(r for r in rows[finished:] if r["kind"] == "slots"))  # a late read
    return tr, after, fin["usage"], tools


# --- parsing ------------------------------------------------------------------


def test_slot_and_metrics_parse() -> None:
    s = Slot.parse(load_json("llama/slots.json")[0])
    assert (s.task, s.processing, s.tokens, s.decoded, s.n_ctx) == (522, False, 410, 0, 65536)
    m = parse_metrics(load_text("llama/metrics.txt"))
    assert {"prompt_tokens_total", "tokens_predicted_total", "prompt_seconds_total"} <= set(m)
    sample = '# HELP x\nllamacpp:a 1.5\nllamacpp:b{x="y"} 2\nother 3\nllamacpp:c x'
    assert parse_metrics(sample) == {"a": 1.5}


def test_recording_tokens_include_decoded() -> None:
    """n_prompt_tokens grows with every decoded token: it is the call's whole context."""
    per_task: dict[int, set[int]] = {}
    for r in recording():
        if r["kind"] == "slots" and r["slots"][0]["is_processing"]:
            s = Slot.parse(r["slots"][0])
            if s.decoded:
                per_task.setdefault(s.task, set()).add(s.tokens - s.decoded)
    assert len(per_task) >= 5
    # constant per call, give or take one token read between two counter updates
    assert all(max(v) - min(v) <= 1 for v in per_task.values()), per_task


# --- selection against the recording ---------------------------------------------


def test_tool_run_keeps_its_last_call_not_the_title() -> None:
    tr, after, usage, tools = replay("tool")
    assert tools == 1 and len(tr.tasks) == 2  # two llama calls around the terminal tool
    m = tr.final(after, usage=usage, tool_steps=tools)
    assert after.processing and after.tokens < 1000  # the late read is the title call
    assert m == Measurement(8692, 65536, "sample")


def test_thinking_run() -> None:
    tr, after, usage, tools = replay("thinking")
    m = tr.final(after, usage=usage, tool_steps=tools)
    assert m == Measurement(9143, 65536, "sample")
    assert m.pct == 14.0


def test_short_run_without_samples_falls_back_to_usage() -> None:
    tr, after, usage, tools = replay("basic")
    assert tr.last is None and tools == 0
    m = tr.final(after, usage=usage, tool_steps=tools)
    assert m == Measurement(8572 + 4, 65536, "usage")
    # with tool steps a usage sum spans several calls: no value rather than a wrong one
    assert tr.final(after, usage=usage, tool_steps=1) is None


def test_final_read_of_the_same_task_wins() -> None:
    tr = ContextTracker()
    tr.prime(Slot(10, False, 300, 0, 65536))
    assert tr.observe(Slot(11, True, 9000, 5, 65536))
    assert not tr.observe(Slot(11, True, 9000, 5, 65536))  # unchanged
    assert not tr.observe(Slot(10, True, 320, 3, 65536))  # an earlier (title) task
    assert tr.final(Slot(11, False, 9012, 0, 65536)) == Measurement(9012, 65536, "final_read")
    assert tr.final(Slot(12, False, 400, 0, 65536)) == Measurement(9000, 65536, "sample")
    # idle read, no samples: accepted only for a task newer than the run's start
    empty = ContextTracker()
    empty.prime(Slot(10, False, 300, 0, 65536))
    assert empty.final(Slot(10, False, 300, 0, 65536)) is None
    assert empty.final(Slot(13, False, 8800, 0, 65536)) == Measurement(8800, 65536, "final_read")
    assert empty.final(Slot(13, True, 8800, 0, 65536)) is None


def test_throughput() -> None:
    m0 = {"prompt_tokens_total": 100.0, "prompt_seconds_total": 1.0,
          "tokens_predicted_total": 10.0, "tokens_predicted_seconds_total": 1.0}  # fmt: skip
    m1 = {"prompt_tokens_total": 1300.0, "prompt_seconds_total": 2.0,
          "tokens_predicted_total": 50.0, "tokens_predicted_seconds_total": 3.0}  # fmt: skip
    assert throughput(m0, m1) == {
        "prompt_tokens": 1200, "predicted_tokens": 40, "prefill_tps": 1200.0, "gen_tps": 20.0
    }  # fmt: skip
    assert throughput(None, m1) == {}
    assert throughput(m1, m1) == {"prompt_tokens": 0, "predicted_tokens": 0}


# --- live runs ----------------------------------------------------------------------

pg = pytest.mark.pg


@pytest.fixture
async def llama() -> FakeLlama:
    fl = FakeLlama()
    fl.set(task=100, processing=False, tokens=300)
    return fl


@pytest.fixture
async def hl(db: async_sessionmaker[AsyncSession], llama: FakeLlama) -> AsyncIterator[Harness]:
    client = LlamaClient("http://llama", transport=StreamingASGITransport(llama.app))
    timing = dataclasses.replace(FAST, context_poll_s=0.01)
    async with make_harness(db, llama=client, timing=timing) as h:
        yield h
    await client.aclose()


async def wait_event(h: Harness, pred: Callable[[Envelope], bool]) -> list[Envelope]:
    seen: list[Envelope] = []
    async with asyncio.timeout(5):
        while True:
            ev = await h.sub.queue.get()
            seen.append(ev)
            if pred(ev):
                return seen


def ctx_event(tokens: int, final: bool = False) -> Callable[[Envelope], bool]:
    return lambda e: (
        e.type == "context.updated" and e.data["tokens"] == tokens and e.data["final"] is final
    )


async def conversation(h: Harness, conv_id: str) -> Conversation:
    async with h.db() as s:
        conv = await s.get(Conversation, conv_id)
    assert conv is not None
    return conv


@pg
async def test_live_context_and_totals(hl: Harness, llama: FakeLlama) -> None:
    sc = hl.fake.enqueue("protocol/a", pause_before_seq=2)
    conv_id, _ = await hl.send("load?")
    await hl.mgr.start()
    await wait_event(hl, lambda e: e.type == "tool.started")

    llama.set(task=101, processing=True, tokens=9000, decoded=10)
    seen = await wait_event(hl, ctx_event(9000))
    ev = seen[-1].data
    assert ev["run_id"] == RUN and ev["conversation_id"] == conv_id
    assert (ev["max"], ev["pct"], ev["warn"], ev["source"]) == (65536, 13.7, False, "sample")

    llama.set(task=102, processing=True, tokens=9050, decoded=3)  # the call after the tool
    await wait_event(hl, ctx_event(9050))
    llama.set(task=102, processing=False, tokens=9058)  # done, idle until the title call
    llama.add(prompt_tokens_total=600, prompt_seconds_total=0.5,
              tokens_predicted_total=60, tokens_predicted_seconds_total=3.0)  # fmt: skip
    sc.release.set()
    rest = await wait_event(hl, lambda e: e.type == "conversation.updated")
    await hl.settle()

    types = [e.type for e in rest if e.type != "message.delta"]
    assert types.index("run.finished") < types.index("context.updated")
    assert types.index("context.updated") < types.index("conversation.updated")
    final = next(e.data for e in rest if e.type == "context.updated")
    assert (final["tokens"], final["final"], final["source"]) == (9058, True, "final_read")
    conv_out = rest[-1].data
    assert conv_out["context"]["tokens"] == 9058 and conv_out["context"]["measured_at"]
    assert conv_out["totals"] == {
        "input_tokens": 16290, "output_tokens": 58, "cache_read_tokens": 8117,
        "total_tokens": 16348,
    }  # fmt: skip

    run = await hl.run(RUN)
    assert run.perf is not None
    perf = run.perf
    assert perf["context_source"] == "final_read" and perf["llama_calls_seen"] == 2
    assert (perf["prompt_tokens"], perf["predicted_tokens"]) == (600, 60)
    assert (perf["prefill_tps"], perf["gen_tps"]) == (1200.0, 20.0)
    assert perf["first_delta_s"] > 0
    conv = await conversation(hl, conv_id)
    assert (conv.context_tokens, conv.context_max) == (9058, 65536)
    assert conv.context_measured_at is not None


@pg
async def test_title_call_does_not_overwrite(hl: Harness, llama: FakeLlama) -> None:
    sc = hl.fake.enqueue("protocol/a", pause_before_seq=2)
    conv_id, _ = await hl.send("load?")
    await hl.mgr.start()
    await wait_event(hl, lambda e: e.type == "tool.started")
    llama.set(task=101, processing=True, tokens=9050, decoded=3)
    await wait_event(hl, ctx_event(9050))
    llama.set(task=102, processing=True, tokens=280, decoded=4)  # title already started
    await asyncio.sleep(0.05)
    sc.release.set()
    await hl.settle()
    conv = await conversation(hl, conv_id)
    assert conv.context_tokens == 9050
    run = await hl.run(RUN)
    assert run.perf is not None and run.perf["context_source"] == "sample"


@pg
async def test_earlier_title_call_is_ignored(hl: Harness, llama: FakeLlama) -> None:
    """A title call already running when the run is submitted is not the run's."""
    llama.set(task=100, processing=True, tokens=280, decoded=2)
    sc = hl.fake.enqueue("protocol/a", pause_before_seq=2)
    conv_id, _ = await hl.send("load?")
    await hl.mgr.start()
    await wait_event(hl, lambda e: e.type == "tool.started")
    await asyncio.sleep(0.05)
    llama.set(task=100, processing=False, tokens=290)
    await asyncio.sleep(0.05)
    sc.release.set()
    seen = await wait_event(hl, lambda e: e.type == "conversation.updated")
    assert not [e for e in seen if e.type == "context.updated"]
    conv = await conversation(hl, conv_id)
    assert conv.context_tokens is None  # tool step + no own sample: unknown, not 290
    assert conv.tokens_in == 16290


@pg
async def test_llama_down_never_breaks_a_run(hl: Harness, llama: FakeLlama) -> None:
    llama.down = True
    hl.fake.enqueue("protocol/a")
    conv_id, _ = await hl.send("load?")
    await hl.mgr.start()
    await hl.settle()
    run = await hl.run(RUN)
    assert run.status == "completed"
    assert run.perf is not None and "context_source" not in run.perf
    assert llama.slot_reads >= 2
    conv = await conversation(hl, conv_id)
    assert conv.context_tokens is None
    assert (conv.tokens_in, conv.tokens_out, conv.tokens_cache_read) == (16290, 58, 8117)


@pg
async def test_totals_and_usage_fallback(hl: Harness, llama: FakeLlama) -> None:
    """Two quick runs without samples: totals add up, context comes from usage."""
    hl.fake.enqueue("protocol/e1")
    conv_id, _ = await hl.send("one")
    await hl.mgr.start()
    await hl.settle()
    conv = await conversation(hl, conv_id)
    assert (conv.context_tokens, conv.tokens_in, conv.tokens_out) == (8098 + 22, 8098, 22)

    hl.fake.enqueue("protocol/e2")
    await hl.send("two", conversation_id=conv_id)
    await hl.settle()
    conv = await conversation(hl, conv_id)
    assert conv.context_tokens == 8141 + 12
    assert (conv.tokens_in, conv.tokens_out, conv.tokens_cache_read) == (
        8098 + 8141,
        22 + 12,
        8061 + 8119,
    )
    e2 = await hl.run(load_json("protocol/e2.create.json")["run_id"])
    assert e2.perf is not None and e2.perf["context_source"] == "usage"
