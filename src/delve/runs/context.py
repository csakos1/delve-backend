"""Context fill and throughput of one run, from llama-server (plan 6.13, decision 159).

The slot only shows the latest llama call, and about 0.2 s after a run completes Hermes
starts the session-title call, which overwrites it. So the slot is sampled while the run
is active, and the value kept is the latest sample of the run's own latest call:

- tasks already known before the run was submitted are ignored (a title call of the
  previous run may still be going on);
- task ids only grow, and with `-np 1` the calls run one after another, so the highest
  task seen during the run is its last call;
- a newer task smaller than the run's context so far is not the run's either (its later
  calls carry the whole history), should a title call slip in before sampling stops;
- on completion one more read is taken; it counts only if it is that same task (idle now,
  so it includes the last generated tokens). A newer task is the title call.

A run too short for any sample (one fast call) falls back to its usage: with no tool steps
the run made a single call, whose context is `input_tokens + output_tokens`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from delve.llama import LlamaClient, LlamaUnavailable, Slot

log = logging.getLogger("delve.runs.context")


@dataclass(frozen=True, slots=True)
class Measurement:
    tokens: int
    max: int
    source: str  # sample | final_read | usage

    @property
    def pct(self) -> float:
        return round(100.0 * self.tokens / self.max, 1) if self.max else 0.0


@dataclass
class ContextTracker:
    """Pure selection logic; fed with slot samples (tested against the recorded fixture)."""

    floor: int | None = None  # highest task known before the run; own tasks are above it
    last: Slot | None = None
    tasks: set[int] = field(default_factory=set)

    def prime(self, s: Slot) -> None:
        self.floor = s.task if self.floor is None else max(self.floor, s.task)

    def _own(self, s: Slot) -> bool:
        return self.floor is None or s.task > self.floor

    def observe(self, s: Slot) -> bool:
        """Take a sample taken while the run is active. True if the kept value changed."""
        if not s.processing or not self._own(s):
            return False
        if self.last is not None and (
            s.task < self.last.task or (s.task > self.last.task and s.tokens < self.last.tokens)
        ):
            # an earlier task, or a newer one smaller than the run's context so far: a title
            # call (a run's later calls carry the whole history, so they never shrink)
            return False
        changed = self.last is None or (s.task, s.tokens) != (self.last.task, self.last.tokens)
        self.last = s
        self.tasks.add(s.task)
        return changed

    def final(
        self,
        s: Slot | None,
        *,
        usage: dict[str, Any] | None = None,
        tool_steps: int = 0,
        n_ctx: int | None = None,
    ) -> Measurement | None:
        """The run's context, given one read taken when it completed (or None)."""
        if s is not None and self._own(s):
            if self.last is None and not s.processing:
                return Measurement(s.tokens, s.n_ctx, "final_read")
            if self.last is not None and s.task == self.last.task:
                return Measurement(max(s.tokens, self.last.tokens), s.n_ctx, "final_read")
        if self.last is not None:
            return Measurement(self.last.tokens, self.last.n_ctx, "sample")
        if usage and tool_steps == 0:
            i, o = usage.get("input_tokens"), usage.get("output_tokens")
            size = n_ctx or (s.n_ctx if s is not None else None)
            if isinstance(i, int) and isinstance(o, int) and size:
                return Measurement(i + o, size, "usage")
        return None


def throughput(m0: dict[str, float] | None, m1: dict[str, float] | None) -> dict[str, Any]:
    """Per-run llama counters from two /metrics reads (title calls may add a little)."""
    if not m0 or not m1:
        return {}

    def d(key: str) -> float:
        return max(0.0, m1.get(key, 0.0) - m0.get(key, 0.0))

    out: dict[str, Any] = {
        "prompt_tokens": int(d("prompt_tokens_total")),
        "predicted_tokens": int(d("tokens_predicted_total")),
    }
    if d("prompt_seconds_total") > 0:
        out["prefill_tps"] = round(d("prompt_tokens_total") / d("prompt_seconds_total"), 1)
    if d("tokens_predicted_seconds_total") > 0:
        out["gen_tps"] = round(d("tokens_predicted_total") / d("tokens_predicted_seconds_total"), 1)
    return out


class ContextMeter:
    """Samples the slot while a run is active; one meter per run."""

    def __init__(
        self,
        llama: LlamaClient,
        *,
        interval_s: float,
        on_sample: Callable[[Measurement], None] | None = None,
    ) -> None:
        self._llama = llama
        self._interval = interval_s
        self.on_sample = on_sample
        self.tracker = ContextTracker()
        self._m0: dict[str, float] | None = None
        self._task: asyncio.Task[None] | None = None
        self._n_ctx: int | None = None

    async def prepare(self, *, exclude_current: bool = True) -> None:
        """Before the run is submitted: note the slot's task and the counters."""
        try:
            s = await self._llama.slot()
            self._n_ctx = s.n_ctx
            if exclude_current:
                self.tracker.prime(s)
        except LlamaUnavailable as exc:
            log.debug("llama slot unavailable", extra={"error": str(exc)})
        with contextlib.suppress(LlamaUnavailable):
            self._m0 = await self._llama.metrics()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._poll(), name="context-meter")

    async def _poll(self) -> None:
        while True:
            try:
                s = await self._llama.slot()
                self._n_ctx = s.n_ctx
                last = self.tracker.last if self.tracker.observe(s) else None
                if last is not None and self.on_sample is not None:
                    self.on_sample(Measurement(last.tokens, last.n_ctx, "sample"))
            except LlamaUnavailable:
                pass
            except Exception:
                log.exception("context sample failed")
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def finish(
        self, *, usage: dict[str, Any] | None, tool_steps: int
    ) -> tuple[Measurement | None, dict[str, Any]]:
        """Stop sampling, read the slot and counters once more. Never raises."""
        await self.stop()
        s: Slot | None = None
        m1: dict[str, float] | None = None
        with contextlib.suppress(LlamaUnavailable):
            s = await self._llama.slot()
        with contextlib.suppress(LlamaUnavailable):
            m1 = await self._llama.metrics()
        try:
            m = self.tracker.final(s, usage=usage, tool_steps=tool_steps, n_ctx=self._n_ctx)
        except Exception:
            log.exception("context measurement failed")
            m = None
        perf = throughput(self._m0, m1)
        if m is not None:
            perf["context_source"] = m.source
        perf["llama_calls_seen"] = len(self.tracker.tasks)
        return m, perf
