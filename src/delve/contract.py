"""The app-facing contract as one JSON document: `docs/contract.json`.

    python -m delve.contract > docs/contract.json

A test fails when the committed file differs from what this produces, so every change a
client can see shows up as a diff of that file (and should bump `API_REVISION`). The app
repository keeps a copy and checks its models against it.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from delve import API_REVISION, protocol
from delve.brief import brief_payload
from delve.db.models import Approval, Card, Conversation, Message, Run, Schedule, ToolStep
from delve.llama import Props
from delve.schedules import schedule_out
from delve.serialize import (
    approval_out,
    card_out,
    conversation_out,
    message_out,
    run_out,
    step_out,
)
from delve.stats import GpuBucket, RunRow, stats_out
from delve.system import Parts, gpu_out, hermes_issues, system_out

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
    card = Card(id="k", v=1, type="water", severity="info", ts=_T, origin="script",
                payload={}, received_at=_T)  # fmt: skip
    system = _keys(
        system_out(
            Parts(checked_at=_T, hermes_ok=True, props=Props("m", "q", 1, 1),
                  llama_unit={"active_state": "active"}, llama_healthy=True, gpus=[])
        )
    )  # fmt: skip
    system["hermes"]["issues"] = "hermes_issue[]"
    system["gpus"] = "gpu[]|null"
    issue = hermes_issues({"readiness": {"checks": {"disk": {"status": "degraded"}}}})[0]
    sched = Schedule(
        id=uuid.UUID(int=1), kind="prompt", name="n", cron="30 7 * * *", prompt="p",
        conversation_mode="new", thinking=False, enabled=True, created_at=_T, updated_at=_T,
    )  # fmt: skip
    brief = brief_payload(
        now=_T, host={"uptime_s": 1, "booted_at": 1},
        incidents=[{"id": "i", "state": "detected"}],
        updates=card, water=card,
        system={"llama": {"status": "running"}, "hermes": {"status": "ok"}},
    )  # fmt: skip
    brief_keys = _keys(brief)
    brief_keys["water"] = {k: True for k in sorted(brief["water"])} if brief["water"] else True
    brief_keys["incidents"] = "brief_incident[]"
    brief_keys["missing"] = "string[]"
    return {
        "conversation": _keys(conversation_out(conv)),
        "message": _keys(message_out(msg)),
        "run": _keys(run_out(run)),
        "open_run": _keys({**run_out(run), "position": 0}),
        "approval": _keys(approval_out(appr)),
        "tool_step": {**_keys(step_out(step)), "approval": "approval|null"},
        # payload: per type, plan 5 (water: station, level_cm, measured_at, …)
        "card": {**_keys(card_out(card)), "payload": "object"},
        # B3: GET /api/v1/system and the system.updated event
        "system": system,
        "gpu": _keys(gpu_out({})),
        "hermes_issue": _keys(issue),
        # B4: GET /api/v1/schedules items, schedule.updated
        "schedule": _keys(schedule_out(sched)),
        # B4: the `payload` of a `brief` card (water: null when there was no reading)
        "brief_payload": brief_keys,
        "brief_incident": _keys(brief["incidents"][0]),
        # B5: GET /api/v1/stats (points: one per bucket, v null without data)
        "stats": _stats_keys(),
        "stats_series": _keys({k: v for k, v in _stats()["generation"].items() if k != "points"})
        | {"points": "stats_point[]"},
        "stats_point": _keys(_stats()["generation"]["points"][0]),
        "stats_day": _keys(_stats()["tokens_per_day"]["days"][0]),
        "stats_gpu": {
            **_keys({"index": 0, "name": "n"}),
            "power": "stats_series",
            "temp": "stats_series",
        },
    }


def _stats() -> dict[str, Any]:
    run = RunRow(status="completed", queued_at=_T, started_at=_T, ended_at=_T, usage={},
                 perf={})  # fmt: skip
    gpu = GpuBucket(index=0, name="n", temp=[None], power=[None], last_temp=None,
                    last_power=None)  # fmt: skip
    return stats_out(
        range_key="24h", now=_T, runs=[run], latencies=[], queued_now=0, gpus=[gpu],
        gpu_edges=[_T, _T], sampling={"interval_s": 60, "ok": True, "error": None,
                                      "last_at": None},
    )  # fmt: skip


def _stats_keys() -> dict[str, Any]:
    out: dict[str, Any] = _keys(_stats())
    for name in ("generation", "prefill", "queue", "approval_latency", "cache_hit"):
        out[name] = "stats_series"
    out["tokens_per_day"] = {"days": "stats_day[]", "value": True}
    out["gpus"] = "stats_gpu[]"
    return out


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
