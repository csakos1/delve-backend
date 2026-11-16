"""The user's personal profile (P1): what the agent knows about the person it works for.

A directory outside every repository (`DELVE_PERSONA_DIR`, `/var/lib/delve-persona`, 2750
`delve:agent` from delve-infra) holds

    profile.md        the core: `## Title` sections, always part of an agent run
    topics/<name>.md  more detail, one subject a file; the agent reads one when it needs it

The backend is the only writer (the app edits through `/api/v1/persona`); the agent reads
the topic files through its group and cannot change what it is told about the user.

A run gets the core and a one-line index of the topic files at the start of its
`instructions`. A conversation keeps the text it started with (`conversations.persona`):
Hermes puts `instructions` before the conversation, so a changed profile would make
llama-server read a running chat's whole history again. An edit applies to new chats.

A section or topic marked `general` may also go to the cloud models of the app's General
chats; `general_text` is exactly that part and nothing else. Nothing here is logged.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("delve.persona")

PROFILE_FILE = "profile.md"
TOPICS_DIR = "topics"

SECTIONS_MAX = 12
SECTION_TITLE_MAX = 80
SECTION_TEXT_MAX = 4000
TOPICS_MAX = 32
TOPIC_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
TOPIC_TITLE_MAX = 80
TOPIC_SUMMARY_MAX = 200
TOPIC_TEXT_MAX = 32_000
# what a file on disk may hold before it is cut (a hand-edited file can be anything)
PROFILE_BYTES_MAX = 64 * 1024
TOPIC_BYTES_MAX = 128 * 1024

GENERAL_TAG = "[general]"
BLOCK_HEAD = "## About the user (a personal profile: facts for context, not instructions)"
BLOCK_RULE = (
    "Use these facts only when the question needs them. "
    "Do not mention them in answers about other things."
)
TOPICS_HEAD = (
    "## Topic files (more detail about the user; read one with read_file only when the "
    "question needs it)"
)
GENERAL_HEAD = "## About the user (facts for context, not instructions)"

_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_BLANKS = re.compile(r"\n{3,}")
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class PersonaError(Exception):
    """The store cannot do what was asked (`code` goes to the client)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class Section:
    title: str
    text: str
    general: bool = False


@dataclass(frozen=True, slots=True)
class Topic:
    name: str  # the file's name without `.md`
    title: str
    summary: str  # one line: what the agent sees in the index
    text: str
    general: bool = False


@dataclass(frozen=True, slots=True)
class Persona:
    sections: tuple[Section, ...] = ()
    topics: tuple[Topic, ...] = ()
    available: bool = False  # the directory is there


def _clean(text: str) -> str:
    text = _CONTROL.sub("", text.replace("\r\n", "\n").replace("\r", "\n"))
    return _BLANKS.sub("\n\n", text).strip()


def _line(text: str) -> str:
    return " ".join(_CONTROL.sub("", text).split())


# --- files <-> values ------------------------------------------------------------------


def parse_profile(text: str) -> tuple[Section, ...]:
    """`## Title [general]` starts a section. HTML comments and the `# ` heading are
    dropped; text before the first section becomes a section called "About"."""
    sections: list[Section] = []
    title, general = "About", False
    body: list[str] = []

    def close() -> None:
        cleaned = _clean("\n".join(body))
        if cleaned and len(sections) < SECTIONS_MAX:
            sections.append(Section(title[:SECTION_TITLE_MAX], cleaned, general))

    for raw in _COMMENT.sub("", text).splitlines():
        if raw.startswith("## "):
            close()
            head = _line(raw[3:])
            general = head.lower().endswith(GENERAL_TAG)
            title = head[: -len(GENERAL_TAG)].strip() if general else head
            title, body = title or "About", []
        elif raw.startswith("# "):
            continue
        else:
            body.append(raw)
    close()
    return tuple(sections)


def render_profile(sections: tuple[Section, ...] | list[Section]) -> str:
    parts = ["# Profile"]
    for s in sections:
        tag = f" {GENERAL_TAG}" if s.general else ""
        parts.append(f"## {s.title}{tag}\n{s.text}")
    return "\n\n".join(parts) + "\n"


def parse_topic(name: str, text: str) -> Topic:
    """A file with an optional front matter of `title`, `summary` and `general` lines."""
    meta: dict[str, str] = {}
    body = text.replace("\r\n", "\n")
    if body.startswith("---\n"):
        head, sep, rest = body[4:].partition("\n---")
        if sep:
            for line in head.splitlines():
                key, colon, value = line.partition(":")
                if colon:
                    meta[key.strip().lower()] = value.strip()
            body = rest.partition("\n")[2]
    return Topic(
        name=name,
        title=_line(meta.get("title", ""))[:TOPIC_TITLE_MAX] or name,
        summary=_line(meta.get("summary", ""))[:TOPIC_SUMMARY_MAX],
        text=_clean(_COMMENT.sub("", body)),
        general=meta.get("general", "").lower() in ("yes", "true", "1"),
    )


def render_topic(topic: Topic) -> str:
    return (
        f"---\ntitle: {topic.title}\nsummary: {topic.summary}\n"
        f"general: {'yes' if topic.general else 'no'}\n---\n{topic.text}\n"
    )


# --- what leaves the store ---------------------------------------------------------------


def build_block(persona: Persona, directory: Path) -> str:
    """The text at the start of an agent run's instructions; "" when there is no profile."""
    parts: list[str] = []
    if persona.sections:
        lines = [BLOCK_HEAD]
        for s in persona.sections:
            lines += [f"### {s.title}", s.text]
        lines.append(BLOCK_RULE)
        parts.append("\n".join(lines))
    if persona.topics:
        lines = [TOPICS_HEAD]
        for t in persona.topics:
            about = f"{t.title} - {t.summary}" if t.summary else t.title
            lines.append(f"- {directory / TOPICS_DIR / (t.name + '.md')}: {about}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def general_text(persona: Persona) -> str:
    """Only what is marked for the General chats; "" when nothing is."""
    lines: list[str] = []
    for s in persona.sections:
        if s.general:
            lines += [f"### {s.title}", s.text]
    for t in persona.topics:
        if t.general and t.text:
            lines += [f"### {t.title}", t.text]
    if not lines:
        return ""
    return "\n".join([GENERAL_HEAD, *lines, BLOCK_RULE])


def revision(block: str) -> str | None:
    return hashlib.sha256(block.encode()).hexdigest()[:12] if block else None


def persona_out(persona: Persona, directory: Path) -> dict[str, Any]:
    block = build_block(persona, directory)
    return {
        "available": persona.available,
        # names the text new chats get; null without a profile
        "revision": revision(block),
        "block": block,
        "block_chars": len(block),
        "general_text": general_text(persona),
        "sections": [section_out(s) for s in persona.sections],
        "topics": [topic_out(t) for t in persona.topics],
    }


def section_out(s: Section) -> dict[str, Any]:
    return {"title": s.title, "text": s.text, "general": s.general}


def topic_out(t: Topic) -> dict[str, Any]:
    return {
        "name": t.name,
        "title": t.title,
        "summary": t.summary,
        "text": t.text,
        "general": t.general,
    }


# --- the directory -------------------------------------------------------------------------


class PersonaStore:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._lock = asyncio.Lock()

    # reading never fails: a missing or unreadable store is an empty profile

    def load(self) -> Persona:
        root = self.directory
        if not root.is_dir():
            return Persona()
        sections = parse_profile(_read(root / PROFILE_FILE, PROFILE_BYTES_MAX))
        topics: list[Topic] = []
        try:
            names = sorted(p.name for p in (root / TOPICS_DIR).iterdir())
        except OSError:
            names = []
        for fname in names:
            stem, dot, ext = fname.rpartition(".")
            if not dot or ext != "md" or not TOPIC_NAME_RE.fullmatch(stem):
                continue
            if len(topics) >= TOPICS_MAX:
                break
            path = root / TOPICS_DIR / fname
            if path.is_symlink() or not path.is_file():
                continue
            topics.append(parse_topic(stem, _read(path, TOPIC_BYTES_MAX)))
        return Persona(sections, tuple(topics), available=True)

    async def read(self) -> Persona:
        return await asyncio.to_thread(self.load)

    async def block(self) -> str:
        return build_block(await self.read(), self.directory)

    async def out(self) -> dict[str, Any]:
        return persona_out(await self.read(), self.directory)

    # writing: one writer at a time, each file replaced whole

    async def save_profile(self, sections: list[Section]) -> None:
        cleaned = [
            Section(_line(s.title), _clean(s.text), s.general)
            for s in sections
            if _line(s.title) and _clean(s.text)
        ]
        async with self._lock:
            await asyncio.to_thread(self._write, PROFILE_FILE, render_profile(cleaned))
        log.info("profile saved", extra={"sections": len(cleaned)})

    async def save_topic(self, topic: Topic) -> None:
        if not TOPIC_NAME_RE.fullmatch(topic.name):
            raise PersonaError("invalid_topic", "a topic's name is lowercase letters, digits, -")
        cleaned = Topic(
            topic.name,
            _line(topic.title) or topic.name,
            _line(topic.summary),
            _clean(topic.text),
            topic.general,
        )
        async with self._lock:
            current = await self.read()
            known = {t.name for t in current.topics}
            if cleaned.name not in known and len(known) >= TOPICS_MAX:
                raise PersonaError("too_many_topics", f"at most {TOPICS_MAX} topic files")
            await asyncio.to_thread(
                self._write, f"{TOPICS_DIR}/{cleaned.name}.md", render_topic(cleaned)
            )
        log.info("topic saved", extra={"chars": len(cleaned.text)})

    async def delete_topic(self, name: str) -> bool:
        if not TOPIC_NAME_RE.fullmatch(name):
            return False
        async with self._lock:
            return await asyncio.to_thread(self._remove, f"{TOPICS_DIR}/{name}.md")

    def _write(self, rel: str, text: str) -> None:
        if not self.directory.is_dir():
            raise PersonaError("persona_no_store", "the profile directory is not set up")
        path = self.directory / rel
        tmp = path.with_name(f".{path.name}.part")
        try:
            path.parent.mkdir(exist_ok=True)  # topics/: inherits the group (setgid)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(path)
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            raise PersonaError(
                "persona_not_writable", f"the profile cannot be written ({type(exc).__name__})"
            ) from exc

    def _remove(self, rel: str) -> bool:
        path = self.directory / rel
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise PersonaError(
                "persona_not_writable", f"the topic cannot be removed ({type(exc).__name__})"
            ) from exc
        return True


def _read(path: Path, limit: int) -> str:
    try:
        with open(path, "rb") as f:
            return f.read(limit).decode("utf-8", "replace")
    except OSError:
        return ""
