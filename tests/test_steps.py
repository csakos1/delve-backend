from __future__ import annotations

import json
from typing import Any

import pytest

from delve.runs.steps import (
    classify_action,
    command_hash,
    offered_choices,
    parse_tool_result,
)

from .harness import sse_events


def completed(name: str, tool: str | None = None) -> list[dict[str, Any]]:
    return [
        e
        for e in sse_events(name)
        if e["event"] == "tool.completed" and (tool is None or e["tool"] == tool)
    ]


@pytest.mark.parametrize(
    ("fixture", "tool", "status", "summary"),
    [
        ("protocol/a", None, "ok", None),
        ("protocol/c", None, "ok", None),  # approved once
        ("protocol/appr", None, "denied", "You denied this command — it did not run."),
        ("protocol/b", None, "denied", None),
        ("protocol/d", None, "timeout", None),
        ("runs/t1", None, "blocked", None),  # deny rule
        ("protocol/f", None, "cancelled", None),  # stop, exit 130
        ("runs/s2", "publish_water_card", "error", None),  # success: false
        ("runs/w1", "publish_water_card", "ok", None),
    ],
)
def test_recorded_previews(
    fixture: str, tool: str | None, status: str, summary: str | None
) -> None:
    ev = completed(fixture, tool)[0]
    res = parse_tool_result(ev["preview"], ev["error"])
    assert res.status == status
    assert res.result is not None and res.result_raw is None
    if summary is not None:
        assert res.user_summary == summary


def test_timeout_summary_and_deny_rule_have_no_button() -> None:
    d = parse_tool_result(completed("protocol/d")[0]["preview"], True)
    assert d.user_summary and d.user_summary.startswith("No answer within")
    t1 = parse_tool_result(completed("runs/t1")[0]["preview"], True)
    assert t1.user_summary is None and "deny rule" in str(t1.result)


def test_truncated_preview_falls_back_to_text() -> None:
    ev = completed("runs/w1", "tool_search")[0]
    assert len(ev["preview"]) == 500
    res = parse_tool_result(ev["preview"], ev["error"])
    assert res.status == "ok" and res.result is None and res.result_raw == ev["preview"]

    full = json.dumps(
        {"output": "", "exit_code": -1, "error": "BLOCKED: Command denied by user. " + "x" * 600,
         "status": "blocked", "user_summary": "You denied this command \\u2014 it did not run."}
    )  # fmt: skip
    cut = full[:500]
    res = parse_tool_result(cut, True)
    assert res.status == "denied" and res.result is None
    cut2 = json.dumps(
        {"status": "blocked", "user_summary": "No answer within 5 minutes", "o": "y" * 600}
    )[:500]
    assert parse_tool_result(cut2, True).status == "timeout"
    assert parse_tool_result(cut2, True).user_summary == "No answer within 5 minutes"
    assert parse_tool_result('{"output": "x", "exit_code": 130', True).status == "cancelled"
    assert parse_tool_result(None, False).status == "ok"
    assert parse_tool_result(None, True).status == "error"
    assert parse_tool_result("[1, 2]", False).status == "ok"


@pytest.mark.parametrize(
    ("command", "cls"),
    [
        ("paru -Syu", "pkg_upgrade"),
        ("sudo pacman -Syu --noconfirm", "pkg_upgrade"),
        ("pacman -Ss foo", "other"),
        ("systemctl --user restart llama-server", "service_restart"),
        ("sudo systemctl stop nginx", "service_restart"),
        ("systemctl status nginx", "other"),
        ("rm -r /tmp/x", "delete_kill"),
        ("cd /tmp && rm -rf dl1", "delete_kill"),
        ("find . -name '*.tmp' -delete", "delete_kill"),
        ("pkill mpv", "delete_kill"),
        ("kill -9 1234", "delete_kill"),
        ("ls /tmp/firmware", "other"),
        ("echo term", "other"),
    ],
)
def test_classify_action(command: str, cls: str) -> None:
    assert classify_action(command) == cls


def test_choices_and_hash() -> None:
    assert offered_choices(["once", "session", "always", "deny"]) == ["once", "session", "deny"]
    assert offered_choices(None) == ["once", "session", "deny"]
    assert offered_choices(["always"]) == []
    assert len(command_hash("rm -r /tmp/x")) == 64
