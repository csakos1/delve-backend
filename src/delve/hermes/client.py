"""Async client for the Hermes Agent API server (v0.22.2 contract).

The backend is a client only: Hermes is never imported or patched (decision 15).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Self

import httpx

from delve.hermes.models import (
    ALLOWED_CHOICES,
    ApprovalResponse,
    Health,
    RunCreated,
    RunStatus,
    SessionInfo,
    SessionMessage,
    Toolset,
)
from delve.hermes.sse import HermesEvent, aiter_events

log = logging.getLogger("delve.hermes.client")

# Capabilities the backend cannot work without (plan 1.2, 3.2).
REQUIRED_CAPABILITIES = (
    "run_submission",
    "run_events_sse",
    "run_stop",
    "run_approval_response",
    "approval_events",
    "tool_progress_events",
)
EXPECTED_TOOLSETS = ("terminal", "file", "vision", "skills", "agentcards")

# Hermes sends `: keepalive` every 10 s; three missed keepalives mean a dead stream.
STREAM_READ_TIMEOUT_S = 35.0


class HermesError(Exception):
    """Non-2xx answer from Hermes, with its error code when it sent one."""

    def __init__(self, status: int, code: str | None, message: str) -> None:
        super().__init__(f"hermes {status} {code or '-'}: {message}")
        self.status = status
        self.code = code
        self.message = message


class HermesUnavailable(Exception):
    """Hermes could not be reached (connection refused, timeout, reset)."""


class StreamEnded(Exception):
    """The event stream closed before a terminal event; recover via get_run()."""

    def __init__(self, run_id: str, last_seq: int | None) -> None:
        super().__init__(f"event stream of {run_id} ended without a terminal event")
        self.run_id = run_id
        self.last_seq = last_seq


@dataclass(frozen=True, slots=True)
class ThinkingRoute:
    """Per-run route for thinking conversations (decision 121)."""

    provider: str
    model: str


def build_run_body(
    *,
    session_id: str,
    input: str | Sequence[Mapping[str, Any]],
    thinking: bool,
    route: ThinkingRoute,
    instructions: str | None = None,
) -> dict[str, Any]:
    """Run body. Only thinking runs carry provider/model; others use the global route."""
    if isinstance(input, str):
        if not input.strip():
            raise ValueError("empty input")
    elif not input:
        raise ValueError("empty input")
    body: dict[str, Any] = {
        "input": input if isinstance(input, str) else [dict(p) for p in input],
        "session_id": session_id,
    }
    if instructions:
        body["instructions"] = instructions
    if thinking:
        body["provider"] = route.provider
        body["model"] = route.model
    return body


def route_applied(runtime: Mapping[str, Any] | None, route: ThinkingRoute) -> bool:
    """True if a finished run used the per-run thinking route.

    Hermes reports it in `run.completed.runtime` (and `GET /v1/runs/{id}`):
    `route_source: "raw_request"` plus `requested: {provider, model}`. A thinking run
    without it means the route was ignored, e.g. after a Hermes update.
    """
    if not runtime or runtime.get("route_source") != "raw_request":
        return False
    requested = runtime.get("requested")
    return (
        isinstance(requested, Mapping)
        and requested.get("provider") == route.provider
        and requested.get("model") == route.model
    )


def capability_problems(
    capabilities: Mapping[str, Any], health: Health, expected_version: str
) -> list[str]:
    """Fail-fast check (plan 3.2). Empty list = compatible."""
    problems: list[str] = []
    if health.status != "ok":
        problems.append(f"health status {health.status!r}")
    if health.version != expected_version:
        problems.append(f"version {health.version!r} != expected {expected_version!r}")
    for name in REQUIRED_CAPABILITIES:
        if not _capability(capabilities, name):
            problems.append(f"capability {name} missing")
    return problems


def _capability(caps: Mapping[str, Any], name: str) -> bool:
    """Find a flag at the top level or one level down (e.g. under `features`).
    A flag is `true` or an object with `supported: true`."""
    candidates: list[Any] = [caps.get(name)]
    candidates += [v.get(name) for v in caps.values() if isinstance(v, Mapping)]
    for value in candidates:
        if value is True:
            return True
        if isinstance(value, Mapping) and value.get("supported") is True:
            return True
    return False


class HermesClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        route: ThinkingRoute,
        *,
        timeout_s: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.route = route
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(timeout_s),
            transport=transport,
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # --- low level ---------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        auth: bool = True,
    ) -> Any:
        hdrs = dict(headers or {})
        try:
            req = self._http.build_request(method, path, json=json, params=params, headers=hdrs)
            if not auth:
                req.headers.pop("Authorization", None)
            resp = await self._http.send(req)
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            raise HermesUnavailable(f"{method} {path}: {type(exc).__name__}") from exc
        if resp.status_code >= 400:
            raise _error_from(resp)
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as exc:
            raise HermesError(resp.status_code, "invalid_json", resp.text[:200]) from exc

    # --- endpoints ---------------------------------------------------------

    async def health(self) -> Health:
        return Health.model_validate(await self._request("GET", "/health", auth=False))

    async def health_detailed(self) -> dict[str, Any]:
        """`GET /health/detailed` (Bearer): readiness checks (disk, gateway, …)."""
        data = await self._request("GET", "/health/detailed")
        if not isinstance(data, dict):
            raise HermesError(200, "invalid_shape", "health/detailed is not an object")
        return data

    async def capabilities(self) -> dict[str, Any]:
        data = await self._request("GET", "/v1/capabilities")
        if not isinstance(data, dict):
            raise HermesError(200, "invalid_shape", "capabilities is not an object")
        return data

    async def create_run(
        self,
        *,
        session_id: str,
        input: str | Sequence[Mapping[str, Any]],
        thinking: bool,
        idempotency_key: uuid.UUID,
        instructions: str | None = None,
    ) -> RunCreated:
        body = build_run_body(
            session_id=session_id,
            input=input,
            thinking=thinking,
            route=self.route,
            instructions=instructions,
        )
        data = await self._request(
            "POST", "/v1/runs", json=body, headers={"Idempotency-Key": str(idempotency_key)}
        )
        return RunCreated.model_validate(data)

    async def stream_events(
        self, run_id: str, *, after_seq: int | None = None
    ) -> AsyncIterator[HermesEvent]:
        """Yield the run's events until a terminal one.

        With `after_seq`, sends `Last-Event-ID` and also drops replayed events locally, so
        it is safe whether or not Hermes honours the header. Raises StreamEnded if the
        connection closes first, HermesUnavailable on connect/read failure.
        """
        headers = {"Accept": "text/event-stream"}
        if after_seq is not None:
            headers["Last-Event-ID"] = str(after_seq)
        timeout = httpx.Timeout(10.0, read=STREAM_READ_TIMEOUT_S)
        last_seq = after_seq
        try:
            async with self._http.stream(
                "GET", f"/v1/runs/{run_id}/events", headers=headers, timeout=timeout
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    raise _error_from(resp)
                async for ev in aiter_events(resp.aiter_text(), after_seq=after_seq):
                    last_seq = ev.seq
                    yield ev
                    if ev.terminal:
                        return
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            raise HermesUnavailable(f"event stream {run_id}: {type(exc).__name__}") from exc
        raise StreamEnded(run_id, last_seq)

    async def get_run(self, run_id: str) -> RunStatus:
        return RunStatus.model_validate(await self._request("GET", f"/v1/runs/{run_id}"))

    async def respond_approval(self, run_id: str, request_id: str, choice: str) -> ApprovalResponse:
        if choice not in ALLOWED_CHOICES:  # never 'always' (decision 22)
            raise ValueError(f"approval choice {choice!r} not allowed")
        data = await self._request(
            "POST",
            f"/v1/runs/{run_id}/approval",
            json={"request_id": request_id, "choice": choice},
        )
        return ApprovalResponse.model_validate(data)

    async def stop_run(self, run_id: str) -> dict[str, Any] | None:
        data = await self._request("POST", f"/v1/runs/{run_id}/stop")
        return data if isinstance(data, dict) else None

    async def get_session(self, session_id: str) -> SessionInfo:
        data = await self._request("GET", f"/api/sessions/{session_id}")
        if not isinstance(data, dict) or not isinstance(data.get("session"), dict):
            raise HermesError(200, "invalid_shape", "session response has no session object")
        return SessionInfo.model_validate(data["session"])

    async def get_session_messages(
        self, session_id: str, *, limit: int = 500
    ) -> list[SessionMessage]:
        data = await self._request(
            "GET",
            f"/api/sessions/{session_id}/messages",
            params={"inline_images": "false", "limit": limit},
        )
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise HermesError(200, "invalid_shape", "messages response has no data list")
        return [SessionMessage.model_validate(m) for m in data["data"]]

    async def toolsets(self) -> list[Toolset]:
        data = await self._request("GET", "/v1/toolsets")
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise HermesError(200, "invalid_shape", "toolsets response has no data list")
        return [Toolset.model_validate(t) for t in data["data"]]


def _error_from(resp: httpx.Response) -> HermesError:
    code: str | None = None
    message = resp.text[:300]
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err = body["error"]
        code = err.get("code") if isinstance(err.get("code"), str) else None
        message = str(err.get("message", message))
    return HermesError(resp.status_code, code, message)


def new_reasoning(
    messages: Sequence[SessionMessage], known_ids: set[str]
) -> list[tuple[str, SessionMessage]]:
    """Assistant messages with thinking text whose Hermes id is not yet stored (plan 6.4/3).

    Returns (hermes_message_id, message) in Hermes order; a tool-using run can produce
    several assistant messages, each with its own reasoning.
    """
    out: list[tuple[str, SessionMessage]] = []
    for m in messages:
        mid = str(m.id)
        if m.role == "assistant" and m.thinking_text and mid not in known_ids:
            out.append((mid, m))
    return out
