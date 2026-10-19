"""The app-facing contract: docs/contract.json, protocol checks, open runs, meta."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import delve
from delve import API_REVISION, contract, protocol
from delve.app import create_app
from delve.config import Settings
from delve.db.models import Conversation, Run
from delve.events import EventHub

from .conftest import ROOT
from .test_api import auth


def test_contract_file_is_current() -> None:
    committed = (ROOT / "docs" / "contract.json").read_text()
    assert committed == contract.render(), (
        "docs/contract.json is stale: run `python -m delve.contract > docs/contract.json` "
        "(and bump API_REVISION if a client can see the change)"
    )


def test_contract_covers_every_route() -> None:
    paths = {(r["method"], r["path"]) for r in contract.contract()["rest"]}
    assert ("GET", "/api/v1/runs/open") in paths and ("WS", "/api/v1/ws") in paths
    assert contract.contract()["api_revision"] == API_REVISION


def test_hub_checks_declared_events(monkeypatch: pytest.MonkeyPatch) -> None:
    hub = EventHub(validate=True)
    ok = {"conversation_id": "c", "run_id": "r", "status": "running"}
    hub.publish("run.updated", ok)
    with pytest.raises(protocol.ProtocolViolation, match="unexpected"):
        hub.publish("run.updated", {**ok, "extra": 1})
    with pytest.raises(protocol.ProtocolViolation, match="missing"):
        hub.publish("run.updated", {"run_id": "r"})
    with pytest.raises(protocol.ProtocolViolation, match="undeclared"):
        hub.publish("run.exploded", {})
    monkeypatch.setattr(protocol, "STRICT", False)  # in production: logged, still sent
    env = hub.publish("run.exploded", {})
    assert env.type == "run.exploded"
    EventHub().publish("anything", {"x": 1})  # unvalidated hubs (unit tests) accept anything


def test_revision_read_from_release_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = "4f35ea1" + "0" * 33
    (tmp_path / "venv").mkdir()
    monkeypatch.setattr(sys, "prefix", str(tmp_path / "venv"))
    assert delve._read_revision() is None
    (tmp_path / "REVISION").write_text(sha + "\n")
    assert delve._read_revision() == sha
    (tmp_path / "REVISION").write_text("not a sha\n")
    assert delve._read_revision() is None


@pytest.mark.pg
def test_meta_reports_revisions(client: TestClient) -> None:
    meta = client.get("/api/v1/meta").json()
    assert meta["api_revision"] == API_REVISION and "revision" in meta


@pytest.fixture
def idle_client(env: pytest.MonkeyPatch, migrated_url: str) -> Iterator[TestClient]:
    """The app without its run worker: queued and active rows stay as the test wrote them
    (the worker would start or recover them)."""
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    with TestClient(create_app(Settings(), run_worker=False)) as c:  # type: ignore[call-arg]
        yield c


@pytest.mark.pg
async def test_open_runs_in_queue_order(
    idle_client: TestClient, token: str, db: async_sessionmaker[AsyncSession]
) -> None:
    client = idle_client
    t0 = datetime(2026, 10, 19, 20, 0, tzinfo=UTC)
    async with db() as s:
        s.add_all([Conversation(id="c_a"), Conversation(id="c_b")])
        await s.flush()

        def run(rid: str, conv: str, status: str, minutes: int) -> Run:
            return Run(
                run_id=rid, conversation_id=conv, idempotency_key=uuid.uuid4(), status=status,
                input="x", thinking=False, origin="app", queued_at=t0 + timedelta(minutes=minutes),
            )  # fmt: skip

        s.add_all(
            [
                run("run_done", "c_a", "completed", 0),
                run("q_late", "c_a", "queued", 9),
                run("q_tie_b", "c_b", "queued", 5),
                run("q_tie_a", "c_a", "queued", 5),
                run("run_live", "c_b", "waiting_for_approval", 7),  # active even if queued later
            ]
        )
        await s.commit()
    r = client.get("/api/v1/runs/open", headers=auth(token))
    assert r.status_code == 200
    rows = r.json()["data"]
    assert [(x["run_id"], x["position"]) for x in rows] == [
        ("run_live", 0), ("q_tie_a", 1), ("q_tie_b", 2), ("q_late", 3),
    ]  # fmt: skip
    assert set(rows[0]) == set(contract.shapes()["open_run"])
    assert client.get("/api/v1/runs/open").status_code == 401
