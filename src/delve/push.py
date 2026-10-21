"""Push notifications over Firebase Cloud Messaging (plan 6.6, B1, decisions 260-265).

The backend sends *data* messages only; the app builds the notification itself (design
18), so it can add the Allow once / Deny / Open actions and remove a notification once the
approval is answered elsewhere.

Kinds (every value is a string, as FCM requires):

* `approval` - an approval that can be answered: `request_id, conversation_id, title,
  action_class, command` (at most 80 characters), `status` (`pending` or
  `expired_in_hermes`), `hermes_deadline, expires_at`. Sent again when the status changes.
* `approval_resolved` - `request_id, status`: the approval was answered, expired or
  cancelled; the app removes its notification. Only for approvals that were pushed.
* `run_done` - `conversation_id, run_id, title, status` (`completed` or `failed`),
  `preview` (the answer's first 120 characters). Only to devices without a live WebSocket,
  and only for runs of at least `PUSH_RUN_DONE_MIN_S` seconds (scheduled runs always).
* `card` - a new water-level card from the scheduled script: `card_id, type, severity,
  station, level_cm, lake_avg_cm, trend_cm_24h, lake_avg_trend_cm_24h, measured_at`
  (empty when unknown). Not for cards older than an hour (history on first ingest).

Priority: every kind goes as HIGH. A NORMAL message waits while the phone dozes -
screen off, lying still - until a maintenance window or until it wakes, so a 08:52 water
reading showed up when the app was opened minutes later. FCM deprioritizes HIGH messages
that show nothing; each of these shows a notification or removes one it showed
(`approval_resolved`, which must not leave a stale approval on the lock screen).

Who gets what is per device (`devices.notify`, `PATCH /api/v1/devices/me`). An
`UNREGISTERED` answer clears the device's FCM token. Every send is logged in `push_log`.

Without `FCM_SERVICE_ACCOUNT_FILE` pushing is off (development, tests).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Approval, Card, Conversation, Device, Message, PushLog, Run
from delve.events import Envelope

log = logging.getLogger("delve.push")

NOTIFY_DEFAULTS: Mapping[str, bool] = {
    "approvals": True,
    "run_done": True,
    "water": False,
    "brief": False,
}
ANSWERABLE = frozenset({"pending", "expired_in_hermes"})
COMMAND_CHARS = 80  # N5: the notification shows the start of the command
PREVIEW_CHARS = 120
FCM_ENDPOINT = "https://fcm.googleapis.com"
FCM_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
MAX_TTL_S = 28 * 24 * 3600  # FCM's limit
RESOLVED_TTL_S = 24 * 3600
RUN_DONE_TTL_S = 6 * 3600
CARD_TTL_S = 12 * 3600
# a card read from history (first ingest, a replaced file) is not news
CARD_PUSH_MAX_AGE_S = 3600
WATER_PUSH_FIELDS = (
    "station",
    "level_cm",
    "lake_avg_cm",
    "trend_cm_24h",
    "lake_avg_trend_cm_24h",
    "measured_at",
)


def notify_settings(raw: Mapping[str, Any] | None) -> dict[str, bool]:
    """The device's settings with defaults for missing keys; unknown keys dropped."""
    out = dict(NOTIFY_DEFAULTS)
    for key, value in (raw or {}).items():
        if key in out and isinstance(value, bool):
            out[key] = value
    return out


def clip(text: str | None, limit: int) -> str:
    flat = " ".join((text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


# --- presence --------------------------------------------------------------------------


@dataclass(eq=False)
class Connection:
    device_id: uuid.UUID
    last_rx: float


class Presence:
    """Live WebSocket connections per device. A connection counts while a frame arrived
    from it within `stale_s` (the app pings every 20 s): a phone that lost its network
    may leave a socket that looks open on this side for minutes."""

    def __init__(self, stale_s: float = 60.0, clock: Callable[[], float] = time.monotonic):
        self.stale_s = stale_s
        self._clock = clock
        self._conns: set[Connection] = set()

    def enter(self, device_id: uuid.UUID) -> Connection:
        conn = Connection(device_id, self._clock())
        self._conns.add(conn)
        return conn

    def touch(self, conn: Connection) -> None:
        conn.last_rx = self._clock()

    def leave(self, conn: Connection) -> None:
        self._conns.discard(conn)

    def live(self, device_id: uuid.UUID) -> bool:
        now = self._clock()
        return any(
            c.device_id == device_id and now - c.last_rx <= self.stale_s for c in self._conns
        )


# --- FCM HTTP v1 ---------------------------------------------------------------------


class PushConfigError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class ServiceAccount:
    client_email: str
    private_key: rsa.RSAPrivateKey = field(repr=False)
    private_key_id: str
    token_uri: str
    project_id: str

    @classmethod
    def from_file(cls, path: Path) -> ServiceAccount:
        try:
            raw = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            raise PushConfigError(f"cannot read {path}: {type(exc).__name__}") from exc
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ServiceAccount:
        if raw.get("type") != "service_account":
            raise PushConfigError("not a service account key (type != service_account)")
        try:
            key = serialization.load_pem_private_key(raw["private_key"].encode(), password=None)
            fields = (raw["client_email"], raw["private_key_id"], raw["token_uri"])
        except (KeyError, ValueError, TypeError, AttributeError) as exc:
            raise PushConfigError(f"invalid service account key: {type(exc).__name__}") from exc
        if not isinstance(key, rsa.RSAPrivateKey):
            raise PushConfigError("the service account key is not an RSA key")
        return cls(
            client_email=fields[0],
            private_key=key,
            private_key_id=fields[1],
            token_uri=fields[2],
            project_id=str(raw.get("project_id") or ""),
        )


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def signed_assertion(sa: ServiceAccount, now: int) -> str:
    """An RS256 JWT for the OAuth 2.0 JWT bearer grant (RFC 7523)."""
    header = {"alg": "RS256", "typ": "JWT", "kid": sa.private_key_id}
    claims = {
        "iss": sa.client_email,
        "scope": FCM_SCOPE,
        "aud": sa.token_uri,
        "iat": now,
        "exp": now + 3600,
    }
    signing_input = ".".join(
        _b64(json.dumps(part, separators=(",", ":")).encode()) for part in (header, claims)
    )
    sig = sa.private_key.sign(signing_input.encode(), padding.PKCS1v15(), hashes.SHA256())
    return signing_input + "." + _b64(sig)


@dataclass(frozen=True, slots=True)
class SendResult:
    ok: bool
    error: str | None = None
    unregistered: bool = False  # the token is dead: forget it


class FcmClient:
    def __init__(
        self,
        sa: ServiceAccount,
        project_id: str | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        endpoint: str = FCM_ENDPOINT,
        attempts: int = 3,
        backoff_s: float = 1.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.sa = sa
        self.project_id = project_id or sa.project_id
        if not self.project_id:
            raise PushConfigError("no project id: set FCM_PROJECT_ID")
        self._http = httpx.AsyncClient(transport=transport, timeout=15.0)
        self._endpoint = endpoint.rstrip("/")
        self._attempts = attempts
        self._backoff_s = backoff_s
        self._clock = clock
        self._token: str | None = None
        self._token_until = 0.0
        self._token_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _access_token(self, *, force: bool = False) -> str:
        async with self._token_lock:
            now = self._clock()
            if self._token is not None and not force and now < self._token_until:
                return self._token
            r = await self._http.post(
                self.sa.token_uri,
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": signed_assertion(self.sa, int(now)),
                },
            )
            if r.status_code != 200:
                raise PushConfigError(f"token request failed: HTTP {r.status_code}")
            body = r.json()
            token = body.get("access_token")
            if not isinstance(token, str) or not token:
                raise PushConfigError("token response without access_token")
            expires_in = body.get("expires_in")
            ttl = float(expires_in) if isinstance(expires_in, int | float) else 3600.0
            self._token, self._token_until = token, now + max(60.0, ttl - 300.0)
            return token

    async def send(
        self,
        token: str,
        data: Mapping[str, str],
        *,
        high: bool,
        ttl_s: int,
        collapse_key: str | None = None,
    ) -> SendResult:
        android: dict[str, Any] = {
            "priority": "HIGH" if high else "NORMAL",
            "ttl": f"{max(0, min(ttl_s, MAX_TTL_S))}s",
        }
        if collapse_key:
            android["collapse_key"] = collapse_key
        body = {"message": {"token": token, "data": dict(data), "android": android}}
        url = f"{self._endpoint}/v1/projects/{self.project_id}/messages:send"
        refreshed = False
        error = "not sent"
        for attempt in range(self._attempts):
            try:
                access = await self._access_token()
                r = await self._http.post(
                    url, json=body, headers={"Authorization": f"Bearer {access}"}
                )
            except PushConfigError as exc:
                return SendResult(False, str(exc))
            except httpx.HTTPError as exc:
                error = f"transport: {type(exc).__name__}"
                await self._pause(attempt, None)
                continue
            if r.status_code == 200:
                return SendResult(True)
            code = _fcm_error_code(r)
            error = f"HTTP {r.status_code} {code}".rstrip()
            if code in ("UNREGISTERED", "SENDER_ID_MISMATCH") or (
                r.status_code == 404 and code == ""
            ):
                return SendResult(False, error, unregistered=True)
            if r.status_code == 401 and not refreshed:
                refreshed = True
                with contextlib.suppress(PushConfigError, httpx.HTTPError):
                    await self._access_token(force=True)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                await self._pause(attempt, r.headers.get("Retry-After"))
                continue
            return SendResult(False, error)
        return SendResult(False, error)

    async def _pause(self, attempt: int, retry_after: str | None) -> None:
        if attempt + 1 >= self._attempts:
            return
        delay = self._backoff_s * 2**attempt
        if retry_after and retry_after.isdigit():
            delay = max(delay, float(retry_after))
        await asyncio.sleep(min(delay, 10.0))


def _fcm_error_code(r: httpx.Response) -> str:
    """The `FcmError.errorCode` of an error response, else the status name."""
    try:
        err = r.json().get("error") or {}
    except ValueError:
        return ""
    for detail in err.get("details") or []:
        if isinstance(detail, dict) and detail.get("errorCode"):
            return str(detail["errorCode"])
    return str(err.get("status") or "")


# --- the service -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Job:
    kind: str  # "approval" | "run"
    ref: str
    delay_s: float = 0.0


class PushService:
    """Turns hub events into pushes. `listener` is registered on the `EventHub`; the work
    (database reads, HTTP) happens on one background task, in event order."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        fcm: FcmClient | None,
        presence: Presence,
        *,
        run_done_min_s: float = 20.0,
        approval_delay_s: float = 1.0,
    ) -> None:
        self._sm = sessionmaker
        self.fcm = fcm
        self.presence = presence
        self._run_done_min_s = run_done_min_s
        # a follow-up run's approval is answered automatically within milliseconds
        # (decision 153); waiting a moment avoids a notification that flashes up
        self._approval_delay_s = approval_delay_s
        self._queue: asyncio.Queue[_Job] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._pushed: dict[str, str] = {}  # request_id -> status last pushed

    @property
    def enabled(self) -> bool:
        return self.fcm is not None

    def listener(self, env: Envelope) -> None:
        if not self.enabled:
            return
        if env.type == "approval.request":
            self._queue.put_nowait(
                _Job("approval", str(env.data["request_id"]), self._approval_delay_s)
            )
        elif env.type == "approval.updated":
            self._queue.put_nowait(_Job("approval", str(env.data["request_id"])))
        elif env.type == "run.finished":
            self._queue.put_nowait(_Job("run", str(env.data["run_id"])))
        elif env.type == "card.new" and _card_worth_a_push(env.data):
            self._queue.put_nowait(_Job("card", str(env.data["id"])))

    async def start(self) -> None:
        if self.enabled and self._task is None:
            self._task = asyncio.create_task(self._worker(), name="push")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self.fcm is not None:
            await self.fcm.aclose()

    async def drain(self) -> None:
        """Wait until every queued event is handled (tests)."""
        await self._queue.join()

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                if job.delay_s:
                    await asyncio.sleep(job.delay_s)
                if job.kind == "approval":
                    await self._approval(job.ref)
                elif job.kind == "card":
                    await self._card(job.ref)
                else:
                    await self._run_done(job.ref)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("push failed", extra={"kind": job.kind, "ref": job.ref})
            finally:
                self._queue.task_done()

    async def _approval(self, request_id: str) -> None:
        async with self._sm() as s:
            a = await s.get(Approval, request_id)
            if a is None:
                return
            title = await _title(s, a.conversation_id)
            pushed_before = request_id in self._pushed or await _was_pushed(
                s, "approval", request_id
            )
        if a.status in ANSWERABLE:
            if self._pushed.get(request_id) == a.status:
                return
            data = {
                "kind": "approval",
                "request_id": a.request_id,
                "conversation_id": a.conversation_id,
                "title": title,
                "action_class": a.action_class,
                "command": clip(a.command, COMMAND_CHARS),
                "status": a.status,
                "hermes_deadline": _iso(a.hermes_deadline),
                "expires_at": _iso(a.expires_at),
            }
            ttl = RESOLVED_TTL_S
            if a.expires_at is not None:
                ttl = int((a.expires_at - datetime.now(UTC)).total_seconds())
            if ttl <= 0:
                return
            self._pushed[request_id] = a.status
            await self._send(
                "approvals", "approval", request_id, data, high=True, ttl_s=ttl,
                collapse_key=request_id,
            )  # fmt: skip
        elif pushed_before:
            self._pushed.pop(request_id, None)
            data = {"kind": "approval_resolved", "request_id": request_id, "status": a.status}
            await self._send(
                "approvals", "approval_resolved", request_id, data, high=True,
                ttl_s=RESOLVED_TTL_S, collapse_key=request_id,
            )  # fmt: skip

    async def _run_done(self, run_id: str) -> None:
        async with self._sm() as s:
            run = await s.get(Run, run_id)
            if run is None or run.status not in ("completed", "failed"):
                return
            if run.origin != "schedule":
                if run.started_at is None or run.ended_at is None:
                    return
                if (run.ended_at - run.started_at).total_seconds() < self._run_done_min_s:
                    return
            title = await _title(s, run.conversation_id)
            answer = (
                await s.execute(
                    select(Message.content)
                    .where(Message.run_id == run_id, Message.role == "assistant")
                    .order_by(Message.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        data = {
            "kind": "run_done",
            "conversation_id": run.conversation_id,
            "run_id": run_id,
            "title": title,
            "status": run.status,
            "preview": clip(answer, PREVIEW_CHARS),
        }
        await self._send(
            "run_done", "run_done", run_id, data, high=True, ttl_s=RUN_DONE_TTL_S,
            collapse_key=None, skip_live=True,
        )  # fmt: skip

    async def _card(self, card_id: str) -> None:
        async with self._sm() as s:
            card = await s.get(Card, card_id)
        if card is None or card.type != "water":
            return
        p = card.payload or {}
        data = {
            "kind": "card",
            "card_id": card.id,
            "type": card.type,
            "severity": card.severity,
            **{k: _text(p.get(k)) for k in WATER_PUSH_FIELDS},
        }
        await self._send(
            "water", "card", card_id, data, high=True, ttl_s=CARD_TTL_S,
            collapse_key="water",
        )  # fmt: skip

    async def _send(
        self,
        setting: str,
        kind: str,
        ref: str,
        data: dict[str, str],
        *,
        high: bool,
        ttl_s: int,
        collapse_key: str | None,
        skip_live: bool = False,
    ) -> None:
        assert self.fcm is not None  # noqa: S101  # only called while enabled
        async with self._sm() as s:
            rows = (
                await s.execute(
                    select(Device.id, Device.fcm_token, Device.notify).where(
                        ~Device.revoked, Device.fcm_token.is_not(None)
                    )
                )
            ).all()
        targets = [
            (dev_id, tok)
            for dev_id, tok, notify in rows
            if notify_settings(notify)[setting] and not (skip_live and self.presence.live(dev_id))
        ]
        for dev_id, tok in targets:
            result = await self.fcm.send(
                tok, data, high=high, ttl_s=ttl_s, collapse_key=collapse_key
            )
            async with self._sm() as s:
                s.add(
                    PushLog(device_id=dev_id, kind=kind, ref_id=ref, ok=result.ok,
                            error=result.error)
                )  # fmt: skip
                if result.unregistered:
                    await s.execute(
                        update(Device)
                        .where(Device.id == dev_id, Device.fcm_token == tok)
                        .values(fcm_token=None)
                    )
                await s.commit()
            if result.ok:
                log.info("push sent", extra={"kind": kind, "ref": ref, "device": str(dev_id)})
            else:
                log.warning(
                    "push failed",
                    extra={"kind": kind, "ref": ref, "device": str(dev_id),
                           "error": result.error, "unregistered": result.unregistered},
                )  # fmt: skip


async def _was_pushed(s: AsyncSession, kind: str, ref: str) -> bool:
    row = (
        await s.execute(
            select(PushLog.id)
            .where(PushLog.kind == kind, PushLog.ref_id == ref, PushLog.ok)
            .limit(1)
        )
    ).scalar_one_or_none()
    return row is not None


async def _title(s: AsyncSession, conversation_id: str) -> str:
    """Hermes' title, or until there is one, the conversation's first message (what the
    app's list shows); empty if neither exists."""
    conv = await s.get(Conversation, conversation_id)
    if conv is None:
        return ""
    if conv.title:
        return clip(conv.title, 80)
    first = (
        await s.execute(
            select(Message.content)
            .where(Message.conversation_id == conversation_id, Message.role == "user")
            .order_by(Message.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    return clip(first, 80)


def _card_worth_a_push(card: Mapping[str, Any]) -> bool:
    """Water level after each scheduled reading (the cron script), when it is new."""
    if card.get("type") != "water" or card.get("origin") != "script":
        return False
    try:
        ts = datetime.fromisoformat(str(card.get("ts")))
    except ValueError:
        return False
    return (datetime.now(UTC) - ts).total_seconds() <= CARD_PUSH_MAX_AGE_S


def _text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _iso(ts: datetime | None) -> str:
    return ts.isoformat() if ts else ""


def build_push(
    sessionmaker: async_sessionmaker[AsyncSession],
    presence: Presence,
    *,
    service_account_file: Path | None,
    project_id: str | None,
    run_done_min_s: float,
    transport: httpx.AsyncBaseTransport | None = None,
) -> PushService:
    """The app's push service; disabled (with an ERROR log when it was configured but
    could not be loaded) if there is no usable service account."""
    fcm: FcmClient | None = None
    if service_account_file is not None:
        try:
            sa = ServiceAccount.from_file(service_account_file)
            fcm = FcmClient(sa, project_id, transport=transport)
        except PushConfigError as exc:
            log.error("push disabled", extra={"reason": str(exc)})
    return PushService(sessionmaker, fcm, presence, run_done_min_s=run_done_min_s)
