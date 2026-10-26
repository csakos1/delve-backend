"""Attachments (B6): images the model sees natively, files the agent reads from the inbox.

- An upload (`POST /api/v1/attachments`) is stored unbound; `chat.send` binds up to
  `MAX_PER_MESSAGE` of them to its user message. Unbound uploads go after an hour.
- Images live under `images_dir` (the backend's state directory) and go into the Hermes
  run as `data:` URLs in the last message's content (decision 327). The phone already
  shrinks them (1536 px, JPEG 85); the limit keeps four of them, base64-encoded, under
  the Hermes API server's 10 MB request cap.
- Files go to `inbox_dir/<id>/<name>` (a setgid `delve:agent` directory, decision N63):
  the run's `instructions` give the path, and the agent reads it with its own tools.
- Hermes keeps only a placeholder for an image in the session history, so later runs of
  a conversation with images get a note saying so (N62, measured in fixtures b6-4).
- Both kinds are deleted `retention_days` after the upload; the row stays, with
  `deleted_at` set, so the chat can say the attachment is no longer available.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import os
import re
import shutil
import unicodedata
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.db.models import Attachment, Message, Run

log = logging.getLogger("delve.attachments")

IMAGE_TYPES = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}
MAX_IMAGE_BYTES = 1_572_864  # 1.5 MiB: 4 images base64-encoded stay under Hermes' 10 MB
MAX_FILE_BYTES = 20 * 1024 * 1024  # decision 324
MAX_PER_MESSAGE = 4
MAX_FILENAME_CHARS = 120
UNBOUND_TTL = timedelta(hours=1)
CLEANUP_EVERY_S = 3600.0
DEFAULT_FILE_MIME = "application/octet-stream"
_MIME_RE = re.compile(r"^[a-z0-9][a-z0-9!#$&^_.+-]{0,63}/[a-z0-9][a-z0-9!#$&^_.+-]{0,63}$")

NOTE_EARLIER_IMAGES = (
    "Earlier messages in this conversation included images. You can no longer see them: "
    "the history only keeps a placeholder. If the user asks about an earlier image, say so "
    "and ask them to send it again; do not guess its content."
)
ONLY_ATTACHMENTS_TEXT = "[The user sent attachments without a message.]"


def file_note(path: str, mime: str, size: int) -> str:
    return (
        f"The user attached a file: {path} ({mime}, {size} bytes). "
        "Read it with your tools when the question needs it."
    )


def gone_note(kind: str, filename: str) -> str:
    what = "An image" if kind == "image" else "A file"
    return f"{what} the user attached ({filename}) is no longer available."


class AttachmentError(Exception):
    """A request the attachment rules refuse; `status` is the HTTP status for REST."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def now() -> datetime:
    return datetime.now(UTC)


def sniff_image(mime: str, head: bytes) -> bool:
    """True if the bytes start like the declared image type."""
    if mime == "image/jpeg":
        return head[:3] == b"\xff\xd8\xff"
    if mime == "image/png":
        return head[:8] == b"\x89PNG\r\n\x1a\n"
    if mime == "image/webp":
        return head[:4] == b"RIFF" and head[8:12] == b"WEBP"
    return False


def display_name(name: str | None, *, fallback: str) -> str:
    """The filename as the user sees it: the last path part, control characters out,
    at most 120 characters; `fallback` if nothing is left."""
    base = (name or "").replace("\\", "/").rsplit("/", 1)[-1]
    base = "".join(ch for ch in base if unicodedata.category(ch)[0] != "C").strip()
    base = base.lstrip(".").strip()
    if len(base) > MAX_FILENAME_CHARS:
        stem, dot, ext = base.rpartition(".")
        if dot and 0 < len(ext) <= 16:
            base = stem[: MAX_FILENAME_CHARS - len(ext) - 1] + "." + ext
        else:
            base = base[:MAX_FILENAME_CHARS]
    return base or fallback


def disk_name(name: str) -> str:
    """The inbox file name: the display name with whitespace, quotes and shell
    metacharacters replaced, so the path in the instructions is one plain token."""
    out = re.sub(r"[\s\"'`$;&|<>(){}\[\]*?!#~%]+", "_", name).strip("_")
    return out or "file"


def normalize_mime(mime: str | None) -> str:
    value = (mime or "").split(";", 1)[0].strip().lower()
    return value if _MIME_RE.match(value) else DEFAULT_FILE_MIME


def attachment_out(a: Attachment) -> dict[str, Any]:
    return {
        "id": str(a.id),
        "kind": a.kind,
        "mime": a.mime,
        "filename": a.filename,
        "size_bytes": a.size_bytes,
        "message_id": a.message_id,
        "available": a.deleted_at is None,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


def attachments_preview(rows: Sequence[Attachment]) -> str | None:
    """A chat-list preview for a message without text: "Image", "2 images", file names."""
    images = sum(1 for a in rows if a.kind == "image")
    parts = [] if not images else ["Image" if images == 1 else f"{images} images"]
    parts += [a.filename for a in rows if a.kind == "file"]
    return ", ".join(parts)[:200] or None


@dataclass(frozen=True, slots=True)
class RunInput:
    """What `POST /v1/runs` gets for a run: the input and the run's instructions."""

    input: str | list[dict[str, Any]]
    instructions: str | None


class AttachmentStore:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        images_dir: Path,
        inbox_dir: Path,
        retention_days: int = 14,
        cleanup_s: float = CLEANUP_EVERY_S,
    ) -> None:
        self._sm = sessionmaker
        self.images_dir = images_dir
        self.inbox_dir = inbox_dir
        self._keep = timedelta(days=retention_days)
        self._cleanup_s = cleanup_s
        self._task: asyncio.Task[None] | None = None

    # --- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="attachment-cleanup")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.cleanup()
            except Exception:
                log.exception("attachment cleanup failed")
            await asyncio.sleep(self._cleanup_s)

    # --- paths -------------------------------------------------------------------

    def path_of(self, a: Attachment) -> Path:
        if a.kind == "file":
            if not a.inbox_path:
                raise AttachmentError(410, "attachment_gone", "the file is no longer stored")
            return Path(a.inbox_path)
        return self.images_dir / f"{a.id.hex}{IMAGE_TYPES.get(a.mime, '.bin')}"

    # --- upload ------------------------------------------------------------------

    async def save(
        self, *, kind: str, mime: str | None, filename: str | None, data: bytes
    ) -> dict[str, Any]:
        """Store one upload, unbound. Raises AttachmentError for what the rules refuse."""
        if kind == "image":
            mime = (mime or "").split(";", 1)[0].strip().lower()
            if mime not in IMAGE_TYPES:
                raise AttachmentError(
                    415, "unsupported_image", "images must be image/jpeg, image/png or image/webp"
                )
            if len(data) > MAX_IMAGE_BYTES:
                raise AttachmentError(413, "too_large", f"an image may be {MAX_IMAGE_BYTES} bytes")
            if not sniff_image(mime, data[:16]):
                raise AttachmentError(415, "unsupported_image", f"the bytes are not {mime}")
            name = display_name(filename, fallback=f"image{IMAGE_TYPES[mime]}")
        elif kind == "file":
            mime = normalize_mime(mime)
            if len(data) > MAX_FILE_BYTES:
                raise AttachmentError(413, "too_large", f"a file may be {MAX_FILE_BYTES} bytes")
            name = display_name(filename, fallback="file")
        else:
            raise AttachmentError(422, "invalid_kind", "kind must be image or file")
        if not data:
            raise AttachmentError(422, "empty", "the upload is empty")
        a = Attachment(
            id=uuid.uuid4(),
            kind=kind,
            mime=mime,
            filename=name,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            created_at=now(),
        )
        if kind == "file":
            a.inbox_path = str(self.inbox_dir / a.id.hex / disk_name(name))
        path = self.path_of(a)
        await asyncio.to_thread(_write, path, data, own_dir=kind == "file")
        try:
            async with self._sm() as s:
                s.add(a)
                await s.commit()
                return attachment_out(a)
        except BaseException:
            await asyncio.to_thread(_remove, path, own_dir=kind == "file")
            raise

    async def copy(self, attachment_id: uuid.UUID) -> dict[str, Any]:
        """A new, unbound attachment with the same content (the app's "send again")."""
        a, path = await self.locate(attachment_id)
        try:
            data = await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError as exc:
            raise AttachmentError(410, "attachment_gone", "the attachment was deleted") from exc
        return await self.save(kind=a.kind, mime=a.mime, filename=a.filename, data=data)

    async def locate(self, attachment_id: uuid.UUID) -> tuple[Attachment, Path]:
        async with self._sm() as s:
            a = await s.get(Attachment, attachment_id)
        if a is None:
            raise AttachmentError(404, "attachment_not_found", "no such attachment")
        if a.deleted_at is not None:
            raise AttachmentError(410, "attachment_gone", "the attachment was deleted")
        return a, self.path_of(a)

    # --- binding (inside the caller's transaction) ----------------------------------

    async def bind(
        self, s: AsyncSession, ids: Sequence[uuid.UUID], *, conversation_id: str, message_id: int
    ) -> list[Attachment]:
        if len(ids) > MAX_PER_MESSAGE:
            raise AttachmentError(
                422, "too_many_attachments", f"at most {MAX_PER_MESSAGE} attachments per message"
            )
        if len(set(ids)) != len(ids):
            raise AttachmentError(422, "duplicate_attachment", "an attachment is listed twice")
        rows = {
            a.id: a
            for a in (
                await s.execute(select(Attachment).where(Attachment.id.in_(ids)).with_for_update())
            ).scalars()
        }
        out = []
        for i in ids:
            a = rows.get(i)
            if a is None:
                raise AttachmentError(404, "attachment_not_found", f"attachment {i} not found")
            if a.deleted_at is not None:
                raise AttachmentError(410, "attachment_gone", f"attachment {i} was deleted")
            if a.message_id is not None:
                raise AttachmentError(409, "attachment_used", f"attachment {i} is already sent")
            a.conversation_id, a.message_id = conversation_id, message_id
            out.append(a)
        return out

    # --- the Hermes input of a run ------------------------------------------------

    async def run_input(self, s: AsyncSession, run: Run) -> RunInput:
        """The run's input (a plain string, or a one-message list with image parts) and
        its instructions (file paths, the earlier-images note)."""
        user = await s.scalar(
            select(Message).where(Message.run_id == run.run_id, Message.role == "user")
        )
        own: list[Attachment] = []
        if user is not None:
            own = list(
                (
                    await s.execute(
                        select(Attachment)
                        .where(Attachment.message_id == user.id)
                        .order_by(Attachment.created_at, Attachment.id)
                    )
                ).scalars()
            )
        earlier = select(Attachment.id).where(
            Attachment.conversation_id == run.conversation_id,
            Attachment.kind == "image",
            Attachment.message_id.is_not(None),
        )
        if user is not None:
            earlier = earlier.where(Attachment.message_id < user.id)
        lines = [NOTE_EARLIER_IMAGES] if await s.scalar(earlier.limit(1)) else []
        parts: list[dict[str, Any]] = []
        for a in own:
            path = None if a.deleted_at is not None else self.path_of(a)
            if a.kind == "image":
                data = None if path is None else await asyncio.to_thread(_read_or_none, path)
                if data is None:
                    lines.append(gone_note("image", a.filename))
                    continue
                url = f"data:{a.mime};base64,{base64.b64encode(data).decode()}"
                parts.append({"type": "image_url", "image_url": {"url": url}})
            elif path is not None and await asyncio.to_thread(path.is_file):
                lines.append(file_note(str(path), a.mime, a.size_bytes))
            else:
                lines.append(gone_note("file", a.filename))
        text = run.input
        if not text.strip():
            text = "" if parts else ONLY_ATTACHMENTS_TEXT
        instructions = "\n".join(filter(None, [run.instructions, *lines])) or None
        if not parts:
            return RunInput(text, instructions)
        content = ([{"type": "text", "text": text}] if text else []) + parts
        return RunInput([{"role": "user", "content": content}], instructions)

    # --- cleanup -------------------------------------------------------------------

    async def cleanup(self, *, at: datetime | None = None) -> tuple[int, int]:
        """Drop unbound uploads older than an hour and delete the files of attachments
        past their retention. Returns (dropped uploads, expired attachments)."""
        t = at or now()
        async with self._sm() as s:
            stale = list(
                (
                    await s.execute(
                        select(Attachment).where(
                            Attachment.message_id.is_(None),
                            Attachment.created_at < t - UNBOUND_TTL,
                        )
                    )
                ).scalars()
            )
            old = list(
                (
                    await s.execute(
                        select(Attachment).where(
                            Attachment.message_id.is_not(None),
                            Attachment.deleted_at.is_(None),
                            Attachment.created_at < t - self._keep,
                        )
                    )
                ).scalars()
            )
            for a in old:
                a.deleted_at = t
            if stale:
                await s.execute(delete(Attachment).where(Attachment.id.in_([a.id for a in stale])))
            await s.commit()
        for a in [*stale, *old]:
            with contextlib.suppress(AttachmentError):
                await asyncio.to_thread(_remove, self.path_of(a), own_dir=a.kind == "file")
        if stale or old:
            log.info("attachments cleaned up", extra={"uploads": len(stale), "expired": len(old)})
        return len(stale), len(old)


def _write(path: Path, data: bytes, *, own_dir: bool) -> None:
    """Write via a temporary name, so a reader never sees half a file."""
    if own_dir:
        path.parent.parent.mkdir(parents=True, exist_ok=True)
        path.parent.mkdir()  # new per attachment; inherits setgid and group from the inbox
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.part")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def _remove(path: Path, *, own_dir: bool) -> None:
    # an inbox file sits in its own `<id hex>/` directory; nothing else is removed whole
    if own_dir and re.fullmatch(r"[0-9a-f]{32}", path.parent.name):
        shutil.rmtree(path.parent, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _read_or_none(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None
