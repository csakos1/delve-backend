"""Answering approvals from the app (plan 6.5 steps 1-2; late approval is plan 12/5)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Approval
from delve.events import EventHub
from delve.hermes.client import HermesClient, HermesError, HermesUnavailable
from delve.hermes.models import ALLOWED_CHOICES
from delve.runs.manager import LateApprovalError, NotFound, RunManager
from delve.runs.steps import STATUS_FOR_CHOICE
from delve.serialize import approval_out

log = logging.getLogger("delve.approvals")

# Hermes' answers when the approval is no longer waiting on its side
GONE_CODES = frozenset({"approval_not_pending", "approval_not_active", "invalid_approval_request"})
DEFAULT_LIST_STATUSES = ("pending", "expired_in_hermes")


class ApprovalError(Exception):
    """A response that cannot be applied; `code` goes to the client as is."""

    def __init__(self, code: str, message: str, http_status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class ApprovalService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        hermes: HermesClient,
        hub: EventHub,
        manager: RunManager | None = None,
    ) -> None:
        self._sm = sessionmaker
        self._manager = manager  # for late approvals
        self._hermes = hermes
        self._hub = hub
        self._locks: dict[str, asyncio.Lock] = {}

    async def list(
        self, statuses: tuple[str, ...] = DEFAULT_LIST_STATUSES, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self._sm() as s:
            rows = (
                await s.execute(
                    select(Approval)
                    .where(Approval.status.in_(statuses))
                    .order_by(Approval.created_at.desc())
                    .limit(limit)
                )
            ).scalars()
            return [approval_out(a) for a in rows]

    async def respond(self, request_id: str, choice: str, decided_by: str) -> dict[str, Any]:
        """Forward a choice to Hermes while the approval is pending there.

        `always` (or anything else) is refused before any network call (decision 22).
        """
        if choice not in ALLOWED_CHOICES:
            raise ApprovalError("invalid_choice", f"choice {choice!r} not allowed", 400)
        lock = self._locks.setdefault(request_id, asyncio.Lock())
        try:
            async with lock:
                return await self._respond(request_id, choice, decided_by)
        finally:
            if not lock.locked():
                self._locks.pop(request_id, None)

    async def _respond(self, request_id: str, choice: str, decided_by: str) -> dict[str, Any]:
        async with self._sm() as s:
            approval = await s.get(Approval, request_id)
            if approval is None:
                raise ApprovalError("approval_not_found", "approval not found", 404)
            status, run_id = approval.status, approval.run_id
        if status == "expired_in_hermes":
            return await self._late(request_id, choice, decided_by)
        if status == "expired":
            raise ApprovalError("approval_expired", "the approval has expired")
        if status != "pending":
            raise ApprovalError("approval_already_decided", f"approval is {status}")

        try:
            await self._hermes.respond_approval(run_id, request_id, choice)
        except HermesUnavailable as exc:
            raise ApprovalError("hermes_unavailable", "Hermes is not reachable", 503) from exc
        except HermesError as exc:
            if exc.code in GONE_CODES:
                changed = await self._mark_gone(request_id)
                if changed is not None:
                    self._hub.publish("approval.updated", changed)
                    # Hermes gave up just now: the same answer becomes a late approval
                    return await self._late(request_id, choice, decided_by)
                # someone else (another device, the stream) settled it in the meantime
                raise ApprovalError("approval_already_decided", "approval was decided") from exc
            raise ApprovalError(f"hermes_{exc.code or exc.status}", exc.message, 502) from exc

        target = STATUS_FOR_CHOICE[choice]
        async with self._sm() as s:
            row = (
                await s.execute(
                    update(Approval)
                    .where(
                        Approval.request_id == request_id, Approval.status.in_(("pending", target))
                    )
                    .values(
                        status=target,
                        choice=choice,
                        decided_by=decided_by,
                        responded_at=func.coalesce(Approval.responded_at, func.now()),
                    )
                    .returning(Approval)
                )
            ).scalar_one_or_none()
            await s.commit()
            out = approval_out(row) if row is not None else None
        if out is None:
            raise ApprovalError("approval_already_decided", "approval was decided")
        log.info(
            "approval answered",
            extra={"request_id": request_id, "run_id": run_id, "choice": choice,
                   "decided_by": decided_by},
        )  # fmt: skip
        self._hub.publish("approval.updated", out)
        return out

    async def _late(self, request_id: str, choice: str, decided_by: str) -> dict[str, Any]:
        if choice == "deny":
            async with self._sm() as s:
                row = (
                    await s.execute(
                        update(Approval)
                        .where(
                            Approval.request_id == request_id,
                            Approval.status == "expired_in_hermes",
                        )
                        .values(
                            status="denied",
                            choice="deny",
                            decided_by=decided_by,
                            responded_at=func.now(),
                        )
                        .returning(Approval)
                    )
                ).scalar_one_or_none()
                await s.commit()
                out = approval_out(row) if row is not None else None
            if out is None:
                raise ApprovalError("approval_already_decided", "approval was decided")
            self._hub.publish("approval.updated", out)
            return out
        if self._manager is None:
            raise ApprovalError("late_approval_unavailable", "late approval is not available")
        try:
            return await self._manager.start_followup(request_id, choice, decided_by)
        except LateApprovalError as exc:
            code = "approval_already_decided" if exc.code == "approval_not_late" else exc.code
            raise ApprovalError(code, exc.message) from exc
        except NotFound as exc:
            raise ApprovalError("approval_not_found", "approval not found", 404) from exc

    async def _mark_gone(self, request_id: str) -> dict[str, Any] | None:
        async with self._sm() as s:
            row = (
                await s.execute(
                    update(Approval)
                    .where(Approval.request_id == request_id, Approval.status == "pending")
                    .values(status="expired_in_hermes")
                    .returning(Approval)
                )
            ).scalar_one_or_none()
            await s.commit()
            return approval_out(row) if row is not None else None
