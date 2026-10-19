"""In-process fake of the Hermes API server, replaying recorded fixtures.

Used through `httpx.ASGITransport`, so no sockets are opened. Every request is recorded
in `FakeHermes.requests` for assertions on paths, headers and bodies.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

FIXTURES = Path(__file__).resolve().parent / "fixtures"
API_KEY = "fake-key"


def load_json(rel: str) -> Any:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def load_text(rel: str) -> str:
    return (FIXTURES / rel).read_text(encoding="utf-8")


_BLOCK_ID = re.compile(r"^id: (\d+)$", re.M)


def sse_after(text: str, after: int | None) -> str:
    """Drop event blocks with id <= after (what Last-Event-ID support would do)."""
    if after is None:
        return text
    blocks = text.split("\n\n")
    kept = [b for b in blocks if not ((m := _BLOCK_ID.search(b)) and int(m.group(1)) <= after)]
    return "\n\n".join(kept)


def sse_cut(text: str, last_seq: int) -> str:
    """Truncate the stream after the block with id == last_seq (simulated disconnect)."""
    blocks = text.split("\n\n")
    out: list[str] = []
    for b in blocks:
        out.append(b)
        m = _BLOCK_ID.search(b)
        if m and int(m.group(1)) == last_seq:
            break
    return "\n\n".join(out) + "\n\n"


@dataclass
class Scenario:
    """What the fake does for one run. `events` is a fixture path (*.events.sse)."""

    run_id: str
    events: str
    status: str | dict[str, Any] | None = None  # GET /v1/runs/{id}: fixture path or body
    cut_after_seq: int | None = None  # end the stream early, without a terminal event
    cut_once: bool = False  # cut only the first connection; reconnects get the rest
    honour_last_event_id: bool = True
    approvals: dict[str, Any] = field(default_factory=dict)  # request_id -> response/error
    # hold the stream before this seq until POST /stop (or `release.set()`)
    pause_before_seq: int | None = None
    release: asyncio.Event = field(default_factory=asyncio.Event)
    connections: int = 0
    stopped: bool = False
    text: str | None = None  # SSE body to serve instead of the `events` fixture


@dataclass
class Recorded:
    method: str
    path: str
    headers: dict[str, str]
    query: dict[str, str]
    body: Any


class FakeHermes:
    def __init__(self, *, version: str = "0.22.2", chunk_size: int = 7) -> None:
        self.version = version
        self.chunk_size = chunk_size
        self.capabilities: dict[str, Any] = load_json("protocol/capabilities.json")
        self.toolsets: dict[str, Any] = load_json("toolsets/toolsets.json")
        self.queue: list[Scenario] = []
        self.runs: dict[str, Scenario] = {}
        self.idempotency: dict[str, tuple[str, str]] = {}  # key -> (body json, run_id)
        self.sessions: dict[str, dict[str, Any]] = {}
        self.messages: dict[str, dict[str, Any]] = {}
        self.requests: list[Recorded] = []
        self.fail_next: list[tuple[int, dict[str, Any]]] = []
        self.app = self._build()

    # --- scripting ----------------------------------------------------------

    def enqueue(self, name: str, **kw: Any) -> Scenario:
        """Queue the recorded run `name` (e.g. "protocol/a") for the next create_run."""
        created = load_json(f"{name}.create.json")
        sc = Scenario(run_id=created["run_id"], events=f"{name}.events.sse", **kw)
        self.queue.append(sc)
        return sc

    def calls(self, method: str, prefix: str) -> list[Recorded]:
        return [r for r in self.requests if r.method == method and r.path.startswith(prefix)]

    # --- app ----------------------------------------------------------------

    def _build(self) -> FastAPI:
        app = FastAPI()
        fake = self

        @app.middleware("http")
        async def record_and_auth(request: Request, call_next: Any) -> Response:
            raw = await request.body()
            body: Any = None
            if raw:
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = raw.decode("utf-8", "replace")
            fake.requests.append(
                Recorded(
                    request.method,
                    request.url.path,
                    {k.lower(): v for k, v in request.headers.items()},
                    dict(request.query_params),
                    body,
                )
            )
            authorized = request.headers.get("authorization") == f"Bearer {API_KEY}"
            if request.url.path != "/health" and not authorized:
                return _err(401, "invalid_api_key", "Invalid API key")
            if fake.fail_next:
                status, payload = fake.fail_next.pop(0)
                return JSONResponse(payload, status_code=status)
            resp: Response = await call_next(request)
            return resp

        @app.get("/health")
        async def health() -> dict[str, Any]:
            return {**load_json("protocol/health.json"), "version": fake.version}

        @app.get("/v1/capabilities")
        async def capabilities() -> dict[str, Any]:
            return fake.capabilities

        @app.get("/v1/toolsets")
        async def toolsets() -> dict[str, Any]:
            return fake.toolsets

        @app.post("/v1/runs")
        async def create_run(request: Request) -> Response:
            body = await request.json()
            if not isinstance(body.get("session_id"), str):
                return _err(400, "invalid_request", "session_id required")
            key = request.headers.get("idempotency-key")
            canon = json.dumps(body, sort_keys=True)
            if key and key in fake.idempotency:
                prev_body, run_id = fake.idempotency[key]
                if prev_body != canon:
                    return _err(409, "idempotency_key_conflict", "Idempotency-Key reused")
                return JSONResponse(
                    {"run_id": run_id, "status": "started", "replayed": True}, status_code=202
                )
            if not fake.queue:
                return _err(500, "no_scenario", "fake: no scenario queued")
            sc = fake.queue.pop(0)
            fake.runs[sc.run_id] = sc
            if key:
                fake.idempotency[key] = (canon, sc.run_id)
            return JSONResponse(
                {"run_id": sc.run_id, "status": "started", "replayed": False}, status_code=202
            )

        @app.get("/v1/runs/{run_id}/events")
        async def events(run_id: str, request: Request) -> Response:
            sc = fake.runs.get(run_id)
            if sc is None:
                return _err(404, "run_not_found", f"Run not found: {run_id}")
            text = sc.text if sc.text is not None else load_text(sc.events)
            sc.connections += 1
            if sc.cut_after_seq is not None and not (sc.cut_once and sc.connections > 1):
                text = sse_cut(text, sc.cut_after_seq)
            lei = request.headers.get("last-event-id")
            if sc.honour_last_event_id and lei and lei.isdigit():
                text = sse_after(text, int(lei))
            parts = [text]
            if sc.pause_before_seq is not None:
                marker = f"id: {sc.pause_before_seq}\n"
                at = text.find(marker)
                if at > 0:
                    parts = [text[:at], text[at:]]

            async def gen() -> AsyncIterator[bytes]:
                for n, part in enumerate(parts):
                    if n:
                        await sc.release.wait()
                    for i in range(0, len(part), fake.chunk_size):
                        yield part[i : i + fake.chunk_size].encode("utf-8")
                        await asyncio.sleep(0)

            return StreamingResponse(gen(), media_type="text/event-stream")

        @app.get("/v1/runs/{run_id}")
        async def get_run(run_id: str) -> Response:
            sc = fake.runs.get(run_id)
            if sc is None or sc.status is None:
                return _err(404, "run_not_found", f"Run not found: {run_id}")
            return JSONResponse(sc.status if isinstance(sc.status, dict) else load_json(sc.status))

        @app.post("/v1/runs/{run_id}/approval")
        async def approval(run_id: str, request: Request) -> Response:
            sc = fake.runs.get(run_id)
            body = await request.json()
            if sc is None:
                return _err(404, "run_not_found", f"Run not found: {run_id}")
            result = sc.approvals.get(body.get("request_id"))
            if result is None:
                return _err(400, "invalid_approval_request", "Approval request_id is invalid.")
            if "error" in result:
                return JSONResponse(result, status_code=400)
            sc.release.set()  # an answered approval lets a paused stream go on
            return JSONResponse(result)

        @app.post("/v1/runs/{run_id}/stop")
        async def stop(run_id: str) -> Response:
            sc = fake.runs.get(run_id)
            if sc is None:
                return _err(404, "run_not_found", f"Run not found: {run_id}")
            sc.stopped = True
            sc.release.set()
            return JSONResponse({"run_id": run_id, "status": "stopping"})

        @app.get("/api/sessions/{sid}")
        async def session(sid: str) -> Response:
            if sid not in fake.sessions:
                return _err(404, "session_not_found", "Session not found")
            return JSONResponse(fake.sessions[sid])

        @app.get("/api/sessions/{sid}/messages")
        async def messages(sid: str) -> Response:
            if sid not in fake.messages:
                return _err(404, "session_not_found", "Session not found")
            return JSONResponse(fake.messages[sid])

        return app


def _err(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        {"error": {"message": message, "type": "invalid_request_error", "param": None,
                   "code": code}},
        status_code=status,
    )  # fmt: skip


class StreamingASGITransport(httpx.AsyncBaseTransport):
    """Like httpx.ASGITransport, but the response body streams as the app sends it.

    httpx's own ASGI transport buffers the whole body, so a fake stream paused mid-way
    (waiting for an approval answer or a stop) would never deliver its first events.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "root_path": "",
            "headers": [(k.lower(), v) for k, v in request.headers.raw],
            "server": ("hermes", 80),
            "client": ("127.0.0.1", 40000),
        }
        sent_body = False
        disconnected = asyncio.Event()
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

        async def receive() -> dict[str, Any]:
            nonlocal sent_body
            if not sent_body:
                sent_body = True
                return {"type": "http.request", "body": body, "more_body": False}
            await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            await queue.put(message)

        async def run() -> None:
            try:
                await self.app(scope, receive, send)
            finally:
                await queue.put(None)

        task = asyncio.create_task(run())
        start = await queue.get()
        if start is None or start["type"] != "http.response.start":
            await task
            raise httpx.TransportError("fake app sent no response")
        return httpx.Response(
            status_code=start["status"],
            headers=start.get("headers", []),
            stream=_StreamingBody(queue, task, disconnected),
            request=request,
        )


class _StreamingBody(httpx.AsyncByteStream):
    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any] | None],
        task: asyncio.Task[None],
        disconnected: asyncio.Event,
    ) -> None:
        self._queue, self._task, self._disconnected = queue, task, disconnected

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            msg = await self._queue.get()
            if msg is None:
                return
            if msg["type"] == "http.response.body":
                if msg.get("body"):
                    yield msg["body"]
                if not msg.get("more_body"):
                    return

    async def aclose(self) -> None:
        self._disconnected.set()
        if not self._task.done():
            self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._task
