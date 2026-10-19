"""Tool steps and approvals as they arrive on a run's event stream (plan 3.4.1, 6.5).

Every function here runs inside the caller's transaction, together with the insert of the
event itself, so a restart never sees an event without its effects.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from delve.db.models import Approval, ToolStep
from delve.hermes.models import ALLOWED_CHOICES

# --- tool.completed preview (plan 3.4.1) ------------------------------------------

_STATUS_BLOCKED = re.compile(r'"status":\s*"blocked"')
_USER_SUMMARY = re.compile(r'"user_summary":\s*"((?:[^"\\]|\\.)*)"')
_ERROR_BLOCKED = re.compile(r'"error":\s*"BLOCKED:')
_EXIT_CODE = re.compile(r'"exit_code":\s*(-?\d+)')


@dataclass(frozen=True, slots=True)
class StepResult:
    status: str  # ok|error|blocked|denied|timeout|cancelled
    result: dict[str, Any] | None  # the preview, when it is complete JSON
    result_raw: str | None  # the preview text, when it is not (truncated at 500 chars)
    user_summary: str | None


def parse_tool_result(preview: Any, error_flag: bool) -> StepResult:
    """Classify a `tool.completed` event by its preview.

    The preview is a JSON string, at most 500 characters, and can be cut off; then the
    status falls back to a regex scan and the event's `error` flag.
    """
    text = preview if isinstance(preview, str) else None
    data: dict[str, Any] | None = None
    if text:
        try:
            parsed = json.loads(text)
            data = parsed if isinstance(parsed, dict) else None
        except ValueError:
            data = None

    if data is not None:
        summary = data.get("user_summary") if isinstance(data.get("user_summary"), str) else None
        status = _classify(
            blocked=data.get("status") == "blocked",
            error_text=str(data.get("error") or ""),
            summary=summary or "",
            exit_code=data.get("exit_code") if isinstance(data.get("exit_code"), int) else None,
            failed=error_flag or data.get("success") is False,
        )
        return StepResult(status, data, None, summary)

    raw = text or ""
    m = _USER_SUMMARY.search(raw)
    summary = _unescape(m.group(1)) if m else None
    code = _EXIT_CODE.search(raw)
    # a long error message pushes "status"/"user_summary" past the 500-character cut,
    # so the BLOCKED marker in the error field also counts (only on a failed tool call)
    blocked = bool(_STATUS_BLOCKED.search(raw)) or (error_flag and bool(_ERROR_BLOCKED.search(raw)))
    status = _classify(
        blocked=blocked,
        error_text=raw,
        summary=summary or "",
        exit_code=int(code.group(1)) if code else None,
        failed=error_flag,
    )
    return StepResult(status, None, text, summary)


def _classify(
    *, blocked: bool, error_text: str, summary: str, exit_code: int | None, failed: bool
) -> str:
    if blocked or error_text.startswith("BLOCKED:"):
        if "denied by user" in error_text or summary.startswith("You denied"):
            return "denied"
        if "timed out without user response" in error_text or summary.startswith("No answer"):
            return "timeout"
        return "blocked"  # deny rule or hardline: no button can change it (plan 4.4)
    if exit_code == 130:
        return "cancelled"
    return "error" if failed else "ok"


def _unescape(s: str) -> str:
    try:
        out = json.loads(f'"{s}"')
        return out if isinstance(out, str) else s
    except ValueError:
        return s


# --- approvals (plan 6.5) -------------------------------------------------------------

_PKG_UPGRADE = re.compile(r"\b(?:paru|yay|pacman)\b[^|;&]*\s-S\w*u")
_SERVICE = re.compile(r"\bsystemctl\b[^|;&]*\b(?:restart|reload|start|stop)\b")
_DELETE_KILL = re.compile(
    r"(?:^|[\s;&|(])(?:sudo\s+)?(?:rm|kill|pkill|killall)\b|\bfind\b.*\s-delete\b"
)

ACTION_TTL = {
    "pkg_upgrade": timedelta(hours=24),
    "service_restart": timedelta(hours=2),
    "delete_kill": timedelta(minutes=30),
    "other": timedelta(minutes=30),  # N7
}
STATUS_FOR_CHOICE = {"once": "approved_once", "session": "approved_session", "deny": "denied"}


def classify_action(command: str) -> str:
    if _PKG_UPGRADE.search(command):
        return "pkg_upgrade"
    if _SERVICE.search(command):
        return "service_restart"
    if _DELETE_KILL.search(command):
        return "delete_kill"
    return "other"


def command_hash(command: str) -> str:
    return hashlib.sha256(command.encode("utf-8")).hexdigest()


def offered_choices(choices: Any) -> list[str]:
    """Hermes offers `always` too; the phone never does (decision 22)."""
    if not isinstance(choices, list):
        return ["once", "session", "deny"]
    return [c for c in choices if isinstance(c, str) and c in ALLOWED_CHOICES]


# --- recording ------------------------------------------------------------------------


async def record_tool_started(
    s: AsyncSession, run_id: str, seq: int, data: dict[str, Any]
) -> ToolStep:
    preview = data.get("preview")
    step = ToolStep(
        run_id=run_id,
        seq_started=seq,
        tool=str(data.get("tool") or "?"),
        preview_start=preview if isinstance(preview, str) else None,
        status="running",
    )
    s.add(step)
    await s.flush()
    return step


async def record_tool_completed(
    s: AsyncSession, run_id: str, seq: int, data: dict[str, Any]
) -> tuple[ToolStep, Approval | None]:
    """Close the oldest running step of the same tool; returns the step and, if Hermes
    timed out an approval that was still pending, that approval (now expired_in_hermes)."""
    tool = str(data.get("tool") or "?")
    step = (
        await s.execute(
            select(ToolStep)
            .where(ToolStep.run_id == run_id, ToolStep.tool == tool, ToolStep.status == "running")
            .order_by(ToolStep.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if step is None:  # tool.started was not seen (should not happen); keep the result anyway
        step = ToolStep(run_id=run_id, tool=tool, status="running")
        s.add(step)
    res = parse_tool_result(data.get("preview"), bool(data.get("error")))
    step.seq_completed = seq
    step.status = res.status
    step.result, step.result_raw, step.user_summary = res.result, res.result_raw, res.user_summary
    duration = data.get("duration")
    step.duration_s = float(duration) if isinstance(duration, int | float) else None
    await s.flush()

    expired: Approval | None = None
    if step.approval_request_id:
        expired = await _transition(
            s, step.approval_request_id, "expired_in_hermes", only_from=("pending",)
        )
    return step, expired


async def record_approval_request(
    s: AsyncSession,
    *,
    run_id: str,
    conversation_id: str,
    data: dict[str, Any],
    received_at: datetime,
    hermes_timeout: timedelta,
) -> tuple[Approval, ToolStep | None] | None:
    request_id = data.get("request_id")
    command = data.get("command")
    if not isinstance(request_id, str) or not isinstance(command, str):
        return None
    keys = data.get("pattern_keys")
    action = classify_action(command)
    await s.execute(
        pg_insert(Approval)
        .values(
            request_id=request_id,
            run_id=run_id,
            conversation_id=conversation_id,
            command=command,
            command_sha256=command_hash(command),
            description=data.get("description")
            if isinstance(data.get("description"), str)
            else None,
            pattern_keys=[k for k in keys if isinstance(k, str)]
            if isinstance(keys, list)
            else None,
            action_class=action,
            status="pending",
            created_at=received_at,
            hermes_deadline=received_at + hermes_timeout,
            expires_at=received_at + max(ACTION_TTL[action], hermes_timeout),
        )
        .on_conflict_do_nothing()
    )
    approval = (
        await s.execute(select(Approval).where(Approval.request_id == request_id))
    ).scalar_one()
    step = (
        await s.execute(
            select(ToolStep)
            .where(ToolStep.run_id == run_id, ToolStep.status == "running")
            .order_by(ToolStep.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if step is not None and step.approval_request_id is None:
        step.approval_request_id = request_id
        await s.flush()
    return approval, step


async def record_approval_responded(s: AsyncSession, data: dict[str, Any]) -> Approval | None:
    """Hermes resolved an approval: through us or another surface (`decided_by` stays
    NULL then until our own response handler sets it)."""
    request_id, choice = data.get("request_id"), data.get("choice")
    if not isinstance(request_id, str) or choice not in STATUS_FOR_CHOICE:
        return None
    return await _transition(
        s,
        request_id,
        STATUS_FOR_CHOICE[str(choice)],
        only_from=("pending", "expired_in_hermes"),
        choice=str(choice),
    )


async def settle_open(
    s: AsyncSession, run_id: str, run_status: str, now: datetime
) -> tuple[list[ToolStep], list[Approval]]:
    """At run end: running steps are cancelled; pending approvals are cancelled when the
    run was stopped, otherwise they stay answerable late (expired_in_hermes)."""
    steps = list(
        (
            await s.execute(
                select(ToolStep).where(ToolStep.run_id == run_id, ToolStep.status == "running")
            )
        ).scalars()
    )
    for step in steps:
        step.status = "cancelled"
    approvals = list(
        (
            await s.execute(
                select(Approval).where(Approval.run_id == run_id, Approval.status == "pending")
            )
        ).scalars()
    )
    target = "cancelled" if run_status == "cancelled" else "expired_in_hermes"
    for a in approvals:
        a.status = target
    await s.flush()
    return steps, approvals


async def _transition(
    s: AsyncSession,
    request_id: str,
    status: str,
    *,
    only_from: tuple[str, ...],
    choice: str | None = None,
) -> Approval | None:
    values: dict[str, Any] = {"status": status}
    if choice is not None:
        values["choice"] = choice
        values["responded_at"] = func.coalesce(Approval.responded_at, func.now())
    row = (
        await s.execute(
            update(Approval)
            .where(Approval.request_id == request_id, Approval.status.in_(only_from))
            .values(**values)
            .returning(Approval)
            .execution_options(synchronize_session="fetch")
        )
    ).scalar_one_or_none()
    return row
