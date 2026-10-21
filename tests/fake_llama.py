"""In-process fake of llama-server's `/slots`, `/metrics`, `/health` and `/props`."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from .fake_hermes import load_json


class FakeLlama:
    def __init__(self) -> None:
        self.slot: dict[str, Any] = load_json("llama/slots.json")[0]
        self.counters: dict[str, float] = {
            "prompt_tokens_total": 1000.0,
            "prompt_seconds_total": 1.0,
            "tokens_predicted_total": 100.0,
            "tokens_predicted_seconds_total": 5.0,
        }
        self.down = False
        self.slot_reads = 0
        # /health: "ok" -> 200, "loading" -> 503, "gone" -> 500 (the client: unavailable)
        self.health = "ok"
        self.props: dict[str, Any] = load_json("system/llama-props.json")
        self.app = self._build()

    def set(self, *, task: int, processing: bool, tokens: int, decoded: int = 0) -> None:
        self.slot = {
            **self.slot,
            "id_task": task,
            "is_processing": processing,
            "n_prompt_tokens": tokens,
            "next_token": [{**self.slot["next_token"][0], "n_decoded": decoded}],
        }

    def add(self, **delta: float) -> None:
        for k, v in delta.items():
            self.counters[k] = self.counters.get(k, 0.0) + v

    def _build(self) -> FastAPI:
        app = FastAPI()
        fake = self

        @app.get("/slots")
        async def slots() -> Response:
            fake.slot_reads += 1
            if fake.down:
                return JSONResponse({"error": "down"}, status_code=503)
            return JSONResponse([fake.slot])

        @app.get("/health")
        async def health() -> Response:
            code = {"ok": 200, "loading": 503}.get(fake.health, 500)
            return JSONResponse({"status": fake.health}, status_code=code)

        @app.get("/props")
        async def props() -> Response:
            if fake.health != "ok":
                return JSONResponse({"error": fake.health}, status_code=503)
            return JSONResponse(fake.props)

        @app.get("/metrics")
        async def metrics() -> Response:
            if fake.down:
                return PlainTextResponse("down", status_code=503)
            lines = []
            for k, v in fake.counters.items():
                lines += [f"# TYPE llamacpp:{k} counter", f"llamacpp:{k} {v}"]
            return PlainTextResponse("\n".join(lines) + "\n")

        return app
