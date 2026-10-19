"""Read-only client for llama-server's `/slots` and `/metrics` (plan 6.13).

The backend never controls generation through this; it only samples the slot to measure
context fill and reads the Prometheus counters for per-run throughput.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

METRIC_PREFIX = "llamacpp:"


class LlamaUnavailable(Exception):
    """llama-server could not be reached or answered with something unusable."""


@dataclass(frozen=True, slots=True)
class Slot:
    """One `/slots` entry, reduced to what context measurement needs.

    `tokens` is `n_prompt_tokens`. Recorded against llama.cpp e287bd872 (fixture
    `llama/slots-run.jsonl`): while a request generates, `n_prompt_tokens` grows with every
    decoded token (`n_prompt_tokens - next_token[0].n_decoded` stays constant), so it is
    the slot's whole token count, i.e. the context the call occupies.
    """

    task: int
    processing: bool
    tokens: int
    decoded: int
    n_ctx: int

    @classmethod
    def parse(cls, raw: dict[str, Any]) -> Slot:
        nt = raw.get("next_token")
        first = nt[0] if isinstance(nt, list) and nt and isinstance(nt[0], dict) else {}
        return cls(
            task=int(raw["id_task"]),
            processing=bool(raw["is_processing"]),
            tokens=int(raw["n_prompt_tokens"]),
            decoded=int(first.get("n_decoded") or 0),
            n_ctx=int(raw["n_ctx"]),
        )


def parse_metrics(text: str) -> dict[str, float]:
    """`llamacpp:<name> <value>` lines -> {name: value}; comments and labels skipped."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line.startswith(METRIC_PREFIX):
            continue
        name, _, value = line.partition(" ")
        if "{" in name:
            continue
        try:
            out[name[len(METRIC_PREFIX) :]] = float(value.strip())
        except ValueError:
            continue
    return out


class LlamaClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 1.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url, timeout=timeout_s, transport=transport, trust_env=False
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _get(self, path: str) -> httpx.Response:
        try:
            r = await self._http.get(path)
        except httpx.HTTPError as exc:
            raise LlamaUnavailable(f"{path}: {type(exc).__name__}") from exc
        if r.status_code != 200:
            raise LlamaUnavailable(f"{path}: HTTP {r.status_code}")
        return r

    async def slot(self) -> Slot:
        """Slot 0 (llama runs with `-np 1`)."""
        r = await self._get("/slots")
        try:
            data = r.json()
            return Slot.parse(data[0])
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LlamaUnavailable(f"/slots: unexpected body ({type(exc).__name__})") from exc

    async def metrics(self) -> dict[str, float]:
        return parse_metrics((await self._get("/metrics")).text)
