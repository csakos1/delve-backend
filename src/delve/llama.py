"""Read-only client for llama-server's `/slots`, `/metrics`, `/health` and `/props`.

The backend never controls generation through this; it samples the slot to measure
context fill, reads the Prometheus counters for per-run throughput (plan 6.13) and shows
the model and its state in the system view (B3). Starting and stopping the server goes
through the privileged helper (`delve.system`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
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


_GGUF_NAME = re.compile(
    r"^(?P<name>.+?)-(?:UD-)?(?P<quant>(?:I?Q\d\w*|BF16|F16|F32))$", re.IGNORECASE
)


def model_label(path: str, ftype: str | None = None) -> tuple[str, str | None]:
    """`…/Qwen3.8-27B-UD-IQ4_XS.gguf` -> ("Qwen3.8-27B", "IQ4_XS").

    Only the file name leaves the backend, never the path (it names the operator's home).
    """
    stem = PurePosixPath(path).name
    if stem.lower().endswith(".gguf"):
        stem = stem[: -len(".gguf")]
    m = _GGUF_NAME.match(stem)
    if m:
        return m["name"], m["quant"].upper()
    quant = ftype.split(" ", 1)[0] if ftype else None  # "IQ4_XS - 4.25 bpw"
    return stem, quant or None


@dataclass(frozen=True, slots=True)
class Props:
    """What the system view shows from `/props` (fixture `system/llama-props.json`)."""

    model: str
    quant: str | None
    n_ctx: int | None
    slots: int | None

    @classmethod
    def parse(cls, raw: dict[str, Any]) -> Props:
        path = raw.get("model_path") or raw.get("model_alias")
        if not isinstance(path, str) or not path:
            raise KeyError("model_path")
        ftype = raw.get("model_ftype")
        name, quant = model_label(path, ftype if isinstance(ftype, str) else None)
        dgs = raw.get("default_generation_settings")
        n_ctx = dgs.get("n_ctx") if isinstance(dgs, dict) else None
        slots = raw.get("total_slots")
        return cls(
            model=name,
            quant=quant,
            n_ctx=n_ctx if isinstance(n_ctx, int) else None,
            slots=slots if isinstance(slots, int) else None,
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

    async def health(self) -> bool:
        """True once the model is loaded; False while it loads (503)."""
        try:
            r = await self._http.get("/health")
        except httpx.HTTPError as exc:
            raise LlamaUnavailable(f"/health: {type(exc).__name__}") from exc
        if r.status_code == 200:
            return True
        if r.status_code == 503:
            return False
        raise LlamaUnavailable(f"/health: HTTP {r.status_code}")

    async def props(self) -> Props:
        r = await self._get("/props")
        try:
            return Props.parse(r.json())
        except (ValueError, KeyError, TypeError) as exc:
            raise LlamaUnavailable(f"/props: unexpected body ({type(exc).__name__})") from exc

    async def chat(self, body: dict[str, Any], *, timeout_s: float) -> dict[str, Any]:
        """One OpenAI-compatible chat completion (the watchers' judge, W2). The body is
        sent as is; the answer is the parsed JSON object."""
        try:
            r = await self._http.post("/v1/chat/completions", json=body, timeout=timeout_s)
        except httpx.HTTPError as exc:
            raise LlamaUnavailable(f"/v1/chat/completions: {type(exc).__name__}") from exc
        if r.status_code != 200:
            raise LlamaUnavailable(f"/v1/chat/completions: HTTP {r.status_code}")
        try:
            data = r.json()
        except ValueError as exc:
            raise LlamaUnavailable("/v1/chat/completions: not JSON") from exc
        if not isinstance(data, dict):
            raise LlamaUnavailable("/v1/chat/completions: unexpected body")
        return data
