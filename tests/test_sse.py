from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable
from pathlib import Path

import pytest

from delve.hermes.sse import (
    TERMINAL_EVENTS,
    HermesProtocolError,
    SSEMessage,
    SSEParser,
    aiter_events,
    decode_event,
    iter_chunks,
    parse_events,
)

from .conftest import FIXTURES

SSE_FIXTURES = sorted(FIXTURES.glob("*/*.events.sse"))
IDS = [str(p.relative_to(FIXTURES)) for p in SSE_FIXTURES]


def _text(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def test_fixtures_present() -> None:
    assert len(SSE_FIXTURES) == 21


@pytest.mark.parametrize("path", SSE_FIXTURES, ids=IDS)
def test_fixture_contract(path: Path) -> None:
    text = _text(path)
    events = list(parse_events(text))
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    # exactly one terminal event, and it is the last one
    assert [e.event for e in events if e.terminal] == [events[-1].event]
    assert events[-1].event in TERMINAL_EVENTS
    # reasoning.available is dropped, everything else is kept
    assert all(e.event != "reasoning.available" for e in events)
    kept = text.count('"event": "') - text.count('"event": "reasoning.available"')
    assert len(events) == kept
    run_ids = {e.run_id for e in events}
    assert len(run_ids) == 1 and next(iter(run_ids)).startswith("run_")


@pytest.mark.parametrize("path", SSE_FIXTURES, ids=IDS)
def test_streamed_text_ends_with_output(path: Path) -> None:
    """Deltas = optional interim text and blank lines, then exactly the final output."""
    events = list(parse_events(_text(path)))
    last = events[-1]
    if last.event != "run.completed":
        assert "output" not in last.data
        return
    streamed = "".join(e.data["delta"] for e in events if e.event == "message.delta")
    assert streamed.endswith(last.data["output"])
    interim = [e.data["text"] for e in events if e.event == "message.interim"]
    if interim:
        assert streamed.startswith(interim[0])


@pytest.mark.parametrize("size", [1, 2, 3, 7, 64, 100_000])
@pytest.mark.parametrize("path", SSE_FIXTURES[:7], ids=IDS[:7])
def test_chunking_does_not_matter(path: Path, size: int) -> None:
    text = _text(path)
    parser = SSEParser()
    msgs: list[SSEMessage] = []
    for chunk in iter_chunks(text, size):
        msgs.extend(parser.feed(chunk))
    ref = SSEParser().feed(text)
    assert msgs == ref


@pytest.mark.parametrize("newline", ["\r\n", "\r"])
def test_crlf_and_cr_line_endings(newline: str) -> None:
    text = _text(FIXTURES / "protocol/b.events.sse")
    ref = list(parse_events(text))
    converted = text.replace("\n", newline)
    parser = SSEParser()
    msgs: list[SSEMessage] = []
    for chunk in iter_chunks(converted, 5):  # splits CRLF pairs across chunks
        msgs.extend(parser.feed(chunk))
    got = [decode_event(m) for m in msgs]
    assert [e.data if e else None for e in got if e] == [e.data for e in ref]


def test_comments_are_skipped_and_counted() -> None:
    parser = SSEParser()
    msgs = parser.feed(_text(FIXTURES / "protocol/d.events.sse"))
    assert parser.comments[0] == "open"
    assert parser.comments[-1] == "stream closed"
    assert parser.comments.count("keepalive") > 0
    assert all(not m.data.startswith(":") for m in msgs)


def test_key_order_independent() -> None:
    events = list(parse_events(_text(FIXTURES / "protocol/b.events.sse")))
    req = next(e for e in events if e.event == "approval.request")
    assert next(iter(req.data)) != "event"  # recorded with 'command' first
    assert req.data["command"] == "rm -r /tmp/x"
    assert "always" in req.data["choices"]
    assert req.seq == 1


def test_spec_details() -> None:
    p = SSEParser()
    msgs = p.feed("data: a\ndata:b\nid: 7\nevent: x\n\n: c\n\nid: 8\n\ndata\n\n")
    assert msgs[0] == SSEMessage(data="a\nb", id="7", event="x")
    # an id-only block updates last_event_id without dispatching
    assert msgs[1] == SSEMessage(data="", id="8", event="message")
    assert p.last_event_id == "8"
    # unterminated block at EOF is discarded
    assert p.feed("data: tail") == [] and p.flush() == []


def test_decode_errors() -> None:
    with pytest.raises(HermesProtocolError):
        decode_event(SSEMessage(data="{not json", id="1"))
    with pytest.raises(HermesProtocolError):
        decode_event(SSEMessage(data='{"seq": 1}', id="1"))
    with pytest.raises(HermesProtocolError):
        decode_event(SSEMessage(data='["x"]', id="1"))
    # seq falls back to the SSE id
    ev = decode_event(SSEMessage(data='{"event": "message.delta", "delta": "x"}', id="4"))
    assert ev is not None and ev.seq == 4


def test_unknown_event_passes_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="delve.hermes.sse"):
        ev = decode_event(SSEMessage(data='{"event": "subagent.started", "seq": 3}', id="3"))
    assert ev is not None and ev.event == "subagent.started"
    assert any(getattr(r, "event_name", None) == "subagent.started" for r in caplog.records)


async def _agen(chunks: Iterable[str]) -> AsyncIterator[str]:
    for c in chunks:
        yield c


async def test_aiter_events_resume_and_stop_at_terminal() -> None:
    text = _text(FIXTURES / "protocol/a.events.sse")
    text += 'id: 99\ndata: {"event": "message.delta", "delta": "after", "seq": 99}\n\n'
    full = [e async for e in aiter_events(_agen(iter_chunks(text, 11)))]
    assert full[-1].event == "run.completed"
    assert all(e.seq != 99 for e in full)
    resumed = [e async for e in aiter_events(_agen([text]), after_seq=20)]
    assert resumed[0].seq == 21
    assert [e.seq for e in resumed] == [e.seq for e in full if e.seq > 20]
