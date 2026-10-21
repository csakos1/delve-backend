"""The morning brief (B4, plan 6.15, decisions 109 and N55).

A deterministic summary without the model: the host's uptime and last boot, the agent's
open cron incidents (both through the privileged helper), the latest package-update
count (the agent's `arch-updates` cron script publishes an `updates` card), the latest
scheduled water reading and the llama-server / Hermes state of the system view. It is
written straight into `cards` (`type: brief`, `origin: backend`) and published as
`card.new`; the push service sends it to the devices that turned the brief on.

A source that cannot be read leaves its fields null and is named in `missing`; the brief
is still written. `payload`:

    date               local date (Europe/Budapest), YYYY-MM-DD
    uptime_s, booted_at, open_incidents, incidents[≤5] {id, state, job, error, last_seen},
    updates_count, updates_checked_at, water {station, level_cm, lake_avg_cm,
    trend_cm_24h, lake_avg_trend_cm_24h, measured_at}, llama_state, hermes_state,
    missing [host | incidents | updates | water | system]
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Card
from delve.events import EventHub
from delve.serialize import card_out
from delve.system import HelperClient, HelperFailed, HelperUnavailable, SystemService

log = logging.getLogger("delve.brief")

TZ = ZoneInfo("Europe/Budapest")
SOURCE_MAX_AGE = timedelta(hours=36)  # an older updates / water card is not today's news
INCIDENTS_TIMEOUT_S = 45.0  # the helper starts `hermes` in the agent's user manager (25 s)
MAX_INCIDENTS = 5
WATER_KEYS = (
    "station",
    "level_cm",
    "lake_avg_cm",
    "trend_cm_24h",
    "lake_avg_trend_cm_24h",
    "measured_at",
)
BRIEF_KEYS = (
    "date",
    "uptime_s",
    "booted_at",
    "open_incidents",
    "incidents",
    "updates_count",
    "updates_checked_at",
    "water",
    "llama_state",
    "hermes_state",
    "missing",
)


def _iso_unix(ts: Any) -> str | None:
    if isinstance(ts, int) and not isinstance(ts, bool) and ts > 0:
        return datetime.fromtimestamp(ts, UTC).isoformat()
    return None


def _clip(v: Any, n: int) -> str | None:
    if not isinstance(v, str) or not v.strip():
        return None
    flat = " ".join(v.split())
    return flat if len(flat) <= n else flat[: n - 1] + "…"


def open_incidents(raw: list[Any]) -> list[dict[str, Any]]:
    """The helper's incident list -> the ones still open (not acknowledged), newest first.

    `resolved` counts as open: the job recovered, but nobody acknowledged the failure
    (the same rule as `ops/post-boot-check.sh`).
    """
    out = [
        {
            "id": str(i.get("id")),
            "state": str(i.get("state")),
            "job": i.get("job"),
            "error": _clip(i.get("error"), 160),
            "last_seen": i.get("last_seen"),
        }
        for i in raw
        if isinstance(i, dict) and i.get("state") != "closed"
    ]
    out.sort(key=lambda i: str(i.get("last_seen") or ""), reverse=True)
    return out


def brief_severity(payload: dict[str, Any]) -> str:
    if payload.get("llama_state") in ("failed", "down") or payload.get("hermes_state") == "down":
        return "alert"
    if (payload.get("open_incidents") or 0) > 0 or payload.get("missing"):
        return "warn"
    return "info"


def brief_payload(
    *,
    now: datetime,
    host: dict[str, Any] | None,
    incidents: list[Any] | None,
    updates: Card | None,
    water: Card | None,
    system: dict[str, Any] | None,
) -> dict[str, Any]:
    """Pure: the brief's payload from what could be read (None = not available)."""
    missing: list[str] = []
    out: dict[str, Any] = {k: None for k in BRIEF_KEYS}
    out["date"] = now.astimezone(TZ).date().isoformat()
    if host is not None:
        up = host.get("uptime_s")
        out["uptime_s"] = up if isinstance(up, int) and not isinstance(up, bool) else None
        out["booted_at"] = _iso_unix(host.get("booted_at"))
    else:
        missing.append("host")
    if incidents is not None:
        still_open = open_incidents(incidents)
        out["open_incidents"] = len(still_open)
        out["incidents"] = still_open[:MAX_INCIDENTS]
    else:
        missing.append("incidents")
    if updates is not None:
        count = (updates.payload or {}).get("count")
        out["updates_count"] = count if isinstance(count, int) else None
        out["updates_checked_at"] = updates.ts.isoformat()
    else:
        missing.append("updates")
    if water is not None:
        p = water.payload or {}
        out["water"] = {k: p.get(k) for k in WATER_KEYS}
    else:
        missing.append("water")
    if system is not None:
        out["llama_state"] = (system.get("llama") or {}).get("status")
        out["hermes_state"] = (system.get("hermes") or {}).get("status")
    else:
        missing.append("system")
    out["missing"] = missing
    return out


class BriefService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hub: EventHub,
        helper: HelperClient,
        system: SystemService | None,
    ) -> None:
        self._sm = sessionmaker
        self._hub = hub
        self._helper = helper
        self._system = system

    async def _helper_data(self, verb: str, timeout_s: float) -> dict[str, Any] | None:
        try:
            return await self._helper.call(verb, timeout_s=timeout_s)
        except (HelperUnavailable, HelperFailed) as exc:
            log.warning("brief source failed", extra={"verb": verb, "error": str(exc)[:200]})
            return None

    async def _latest(self, card_type: str, since: datetime) -> Card | None:
        async with self._sm() as s:
            card: Card | None = (
                await s.execute(
                    select(Card)
                    .where(Card.type == card_type, Card.origin == "script", Card.ts >= since)
                    .order_by(Card.ts.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        return card

    async def build(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(UTC)
        host = await self._helper_data("host.status", 15.0)
        inc = await self._helper_data("agent.incidents", INCIDENTS_TIMEOUT_S)
        incidents = inc.get("incidents") if inc is not None else None
        system: dict[str, Any] | None = None
        if self._system is not None:
            try:
                system = await self._system.snapshot(fresh=True)
            except Exception:
                log.exception("brief: system view failed")
        return brief_payload(
            now=now,
            host=host,
            incidents=incidents if isinstance(incidents, list) else None,
            updates=await self._latest("updates", now - SOURCE_MAX_AGE),
            water=await self._latest("water", now - SOURCE_MAX_AGE),
            system=system,
        )

    async def publish(self, card_id: str, now: datetime | None = None) -> dict[str, Any]:
        """Build the brief and store it as card `card_id` (idempotent on the id)."""
        now = now or datetime.now(UTC)
        payload = await self.build(now)
        values = {
            "id": card_id,
            "v": 1,
            "type": "brief",
            "severity": brief_severity(payload),
            "ts": now,
            "origin": "backend",
            "session_id": None,
            "payload": payload,
        }
        async with self._sm() as s:
            inserted = (
                await s.execute(
                    insert(Card)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=[Card.id])
                    .returning(Card)
                )
            ).scalar_one_or_none()
            await s.commit()
            card = inserted or await s.get(Card, card_id)
            out = card_out(card) if card is not None else None
        if inserted is not None and out is not None:
            self._hub.publish("card.new", out)
            log.info(
                "brief published",
                extra={"card_id": card_id, "severity": values["severity"],
                       "missing": payload["missing"]},
            )  # fmt: skip
        if out is None:  # pragma: no cover - the row was just written
            raise RuntimeError("brief card vanished")
        return out


def brief_card_id(schedule_id: uuid.UUID, due: datetime) -> str:
    """One card per scheduled run: a retry after a crash finds the same id."""
    return uuid.uuid5(schedule_id, f"brief/{due.astimezone(UTC).isoformat()}").hex
