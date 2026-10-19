"""Late approval (plan 6.5 step 4) and approval housekeeping.

An approval Hermes gave up on (`expired_in_hermes`) can still be answered until its
`expires_at`. A late `once`/`session` starts a follow-up run in the same conversation
(`origin='late_approval'`) whose input asks the agent to run exactly that command. When
the follow-up run asks for approval of a command with the same SHA-256, the backend
answers `once` by itself; any other command needs a fresh answer (fail-closed).

    expired_in_hermes -> late_approved -> followup_started -> executed | superseded
                                                           -> cancelled (run stopped)

All helpers run inside the caller's transaction.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, case, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from delve.db.models import ACTIVE_RUN_STATUSES, Approval, Run

LATE_PROMPT = (
    "A felhasználó utólag jóváhagyta a korábban jóváhagyásra váró parancsot. "
    "Futtasd le pontosan ezt a parancsot a terminal toollal, változtatás nélkül, "
    "utána röviden írd le az eredményt:\n\n{command}"
)
AUTO_DECIDED_BY = "late_approval"


def late_prompt(command: str) -> str:
    return LATE_PROMPT.format(command=command)


async def followup_started(s: AsyncSession, queue_id: str, run_id: str) -> list[Approval]:
    """The follow-up run got its Hermes id: follow the rename and mark it started."""
    rows = await s.execute(
        update(Approval)
        .where(Approval.followup_run_id == queue_id)
        .values(
            followup_run_id=run_id,
            status=case(
                (Approval.status == "late_approved", "followup_started"),
                else_=Approval.status,
            ),
        )
        .returning(Approval)
    )
    return list(rows.scalars())


async def match_followup(
    s: AsyncSession, run_id: str, new: Approval
) -> tuple[Approval | None, bool]:
    """The original approval behind this follow-up run, and whether the new request is
    for the very same command. A different command supersedes the original."""
    orig = (
        await s.execute(
            select(Approval)
            .where(Approval.followup_run_id == run_id, Approval.status == "followup_started")
            .with_for_update()
        )
    ).scalar_one_or_none()
    if orig is None or orig.request_id == new.request_id:
        return None, False
    if orig.command_sha256 == new.command_sha256:
        return orig, True
    orig.status = "superseded"
    await s.flush()
    return orig, False


async def auto_approved(
    s: AsyncSession, new_request_id: str, orig_request_id: str
) -> list[Approval]:
    """Record the backend's own `once` for the follow-up request; the original executed."""
    out: list[Approval] = []
    new = (
        await s.execute(
            update(Approval)
            .where(
                Approval.request_id == new_request_id,
                Approval.status.in_(("pending", "approved_once")),
            )
            .values(
                status="approved_once",
                choice="once",
                decided_by=AUTO_DECIDED_BY,
                responded_at=func.coalesce(Approval.responded_at, func.now()),
            )
            .returning(Approval)
        )
    ).scalar_one_or_none()
    if new is not None:
        out.append(new)
    orig = (
        await s.execute(
            update(Approval)
            .where(Approval.request_id == orig_request_id, Approval.status == "followup_started")
            .values(status="executed")
            .returning(Approval)
        )
    ).scalar_one_or_none()
    if orig is not None:
        out.append(orig)
    return out


async def settle_followup(s: AsyncSession, run_id: str, run_status: str) -> list[Approval]:
    """At the end of a follow-up run, an original that was neither executed nor
    superseded is settled: stopped -> cancelled; otherwise -> superseded."""
    target = "cancelled" if run_status == "cancelled" else "superseded"
    rows = await s.execute(
        update(Approval)
        .where(
            Approval.followup_run_id == run_id,
            Approval.status.in_(("late_approved", "followup_started")),
        )
        .values(status=target)
        .returning(Approval)
    )
    return list(rows.scalars())


async def followup_not_started(s: AsyncSession, queue_id: str, run_status: str) -> list[Approval]:
    """The follow-up never reached Hermes. Stopped -> cancelled; a failed submission
    gives the approval back (still answerable until expires_at)."""
    values: dict[str, object] = {"status": "cancelled"}
    if run_status != "cancelled":
        values = {"status": "expired_in_hermes", "followup_run_id": None, "choice": None}
    rows = await s.execute(
        update(Approval)
        .where(Approval.followup_run_id == queue_id, Approval.status == "late_approved")
        .values(**values)
        .returning(Approval)
    )
    return list(rows.scalars())


async def expire_due(s: AsyncSession, now: datetime) -> list[Approval]:
    rows = await s.execute(
        update(Approval)
        .where(Approval.status == "expired_in_hermes", Approval.expires_at <= now)
        .values(status="expired")
        .returning(Approval)
    )
    return list(rows.scalars())


async def reconcile_pending(s: AsyncSession, active_run_id: str | None) -> list[Approval]:
    """Pending approvals whose run is no longer active cannot be answered in Hermes any
    more (e.g. after a gateway restart): they become answerable late."""
    active = select(Run.run_id).where(Run.status.in_(ACTIVE_RUN_STATUSES))
    cond = and_(Approval.status == "pending", Approval.run_id.not_in(active))
    if active_run_id:
        cond = and_(cond, Approval.run_id != active_run_id)
    rows = await s.execute(
        update(Approval).where(cond).values(status="expired_in_hermes").returning(Approval)
    )
    return list(rows.scalars())
