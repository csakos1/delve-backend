"""The app-facing contract as one JSON document: `docs/contract.json`.

    python -m delve.contract > docs/contract.json

A test fails when the committed file differs from what this produces, so every change a
client can see shows up as a diff of that file (and should bump `API_REVISION`). The app
repository keeps a copy and checks its models against it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from delve import API_REVISION, protocol
from delve.db.models import Approval, Conversation, Message, Run, ToolStep
from delve.serialize import approval_out, conversation_out, message_out, run_out, step_out

_T = datetime(2026, 1, 1, tzinfo=UTC)


def _keys(value: Any) -> Any:
    """The key structure of a serializer's output: `true` for a value, nested objects
    expanded. Values may be null; the README says which."""
    if isinstance(value, dict):
        return {k: _keys(v) for k, v in sorted(value.items())}
    return True


def shapes() -> dict[str, Any]:
    conv = Conversation(
        id="c", title="t", created_at=_T, updated_at=_T, archived=False, thinking=False,
        origin="app", last_run_id=None, context_tokens=1, context_max=2,
        context_measured_at=_T, tokens_in=0, tokens_out=0, tokens_cache_read=0, preview="p",
    )  # fmt: skip
    msg = Message(id=1, conversation_id="c", run_id="r", role="user", content="", created_at=_T)
    run = Run(run_id="r", conversation_id="c", status="queued", thinking=False, origin="app")
    appr = Approval(request_id="a", run_id="r", conversation_id="c", command="", status="pending",
                    action_class="other", created_at=_T)  # fmt: skip
    step = ToolStep(id=1, run_id="r", tool="terminal", status="running")
    return {
        "conversation": _keys(conversation_out(conv)),
        "message": _keys(message_out(msg)),
        "run": _keys(run_out(run)),
        "open_run": _keys({**run_out(run), "position": 0}),
        "approval": _keys(approval_out(appr)),
        "tool_step": {**_keys(step_out(step)), "approval": "approval|null"},
    }


def rest() -> list[dict[str, Any]]:
    from delve.app import create_app  # late: the app pulls in settings-dependent modules

    app = create_app(_settings_for_schema(), run_worker=False)
    out = []
    for path, ops in sorted(app.openapi()["paths"].items()):
        for method, op in sorted(ops.items()):
            params = sorted(p["name"] for p in op.get("parameters", []) if p["in"] == "query")
            out.append({"method": method.upper(), "path": path, "query": params})
    out.append({"method": "WS", "path": "/api/v1/ws", "query": []})
    return out


def _settings_for_schema() -> Any:
    from pydantic import SecretStr

    from delve.config import Settings

    return Settings.model_construct(hermes_api_key=SecretStr("x"))


def contract() -> dict[str, Any]:
    return {
        "api_revision": API_REVISION,
        "rest": rest(),
        "shapes": shapes(),
        "ws": {
            "client_frames": {
                k: {"required": sorted(r), "optional": sorted(o)}
                for k, (r, o) in sorted(protocol.CLIENT_FRAMES.items())
            },
            "server_events": {k: sorted(v) for k, v in sorted(protocol.SERVER_EVENTS.items())},
            "connection_frames": sorted(protocol.CONNECTION_FRAMES),
        },
    }


def render() -> str:
    return json.dumps(contract(), indent=2, ensure_ascii=False) + "\n"


def main() -> None:
    print(render(), end="")


if __name__ == "__main__":
    main()
