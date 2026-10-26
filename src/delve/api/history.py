"""Message lists with their tool steps (and each step's approval) attached."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from delve.attachments import attachment_out
from delve.db.models import Approval, Attachment, Message, ToolStep
from delve.serialize import message_out, step_out


async def messages_with_steps(s: AsyncSession, msgs: Sequence[Message]) -> list[dict[str, Any]]:
    run_ids = {m.run_id for m in msgs if m.role == "assistant" and m.run_id}
    by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if run_ids:
        found = list(
            (
                await s.execute(
                    select(ToolStep).where(ToolStep.run_id.in_(run_ids)).order_by(ToolStep.id)
                )
            ).scalars()
        )
        req_ids = {t.approval_request_id for t in found if t.approval_request_id}
        approvals: dict[str, Approval] = {}
        if req_ids:
            approvals = {
                a.request_id: a
                for a in (
                    await s.execute(select(Approval).where(Approval.request_id.in_(req_ids)))
                ).scalars()
            }
        for t in found:
            by_run[t.run_id].append(step_out(t, approvals.get(t.approval_request_id or "")))
    user_ids = [m.id for m in msgs if m.role == "user"]
    files: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if user_ids:
        for a in (
            await s.execute(
                select(Attachment)
                .where(Attachment.message_id.in_(user_ids))
                .order_by(Attachment.created_at, Attachment.id)
            )
        ).scalars():
            files[a.message_id or 0].append(attachment_out(a))
    out = []
    for m in msgs:
        item = message_out(m, files.get(m.id))
        if m.role == "assistant":
            item["tool_steps"] = by_run.get(m.run_id or "", [])
        out.append(item)
    return out
