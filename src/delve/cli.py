"""Command line: `delve-backend [serve]`, `delve-backend pair`,
`delve-backend token {create,list,revoke}` and `delve-backend helper <verb>`.

The token commands only need the database (DELVE_DATABASE_URL), like Alembic, so they
work without the Hermes key in the environment. `helper` talks to the privileged helper's
socket (DELVE_HELPER_SOCKET) exactly as the server does - `sudo delve helper gpu.status`
checks the socket, its group and the helper in one go.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, InterfaceError, OperationalError, ProgrammingError

from delve.auth import create_device, create_pairing_code, format_code, revoke_device
from delve.db.models import Device
from delve.db.session import make_engine, make_sessionmaker

DEFAULT_DB_URL = "postgresql+asyncpg:///delve?host=/run/postgresql"
HELPER_VERBS = (
    "llama.status",
    "hermes.status",
    "gpu.status",
    "host.status",
    "agent.incidents",
    "llama.start",
    "llama.stop",
)


def _db_url() -> str:
    return os.environ.get("DELVE_DATABASE_URL", DEFAULT_DB_URL)


async def _token_create(name: str) -> int:
    engine = make_engine(_db_url())
    try:
        async with make_sessionmaker(engine)() as session:
            device, token = await create_device(session, name)
            await session.commit()
    finally:
        await engine.dispose()
    print(f"device: {device.id}  name: {device.name}", file=sys.stderr)
    print("token (shown once, store it in the app):", file=sys.stderr)
    print(token)
    return 0


async def _pair(name: str | None, ttl_min: int) -> int:
    engine = make_engine(_db_url())
    try:
        async with make_sessionmaker(engine)() as session:
            code, expires = await create_pairing_code(
                session, name=name, ttl=timedelta(minutes=ttl_min)
            )
            await session.commit()
    finally:
        await engine.dispose()
    local = expires.astimezone().strftime("%H:%M:%S")
    print(f"pairing code, valid until {local} (one use; replaces earlier codes):", file=sys.stderr)
    print(format_code(code))
    return 0


async def _token_list() -> int:
    engine = make_engine(_db_url())
    try:
        async with make_sessionmaker(engine)() as session:
            rows = (
                (await session.execute(select(Device).order_by(Device.created_at))).scalars().all()
            )
    finally:
        await engine.dispose()
    for d in rows:
        seen = d.last_seen_at.isoformat(timespec="seconds") if d.last_seen_at else "-"
        state = "revoked" if d.revoked else "active"
        print(f"{d.id}  {state:7}  last_seen={seen}  {d.name}")
    return 0


async def _token_revoke(device_id: str) -> int:
    try:
        did = uuid.UUID(device_id)
    except ValueError:
        print(f"not a device id: {device_id}", file=sys.stderr)
        return 2
    engine = make_engine(_db_url())
    try:
        async with make_sessionmaker(engine)() as session:
            ok = await revoke_device(session, did)
            await session.commit()
    finally:
        await engine.dispose()
    print("revoked" if ok else "no active device with that id", file=sys.stderr)
    return 0 if ok else 1


def _serve() -> int:
    import uvicorn

    from delve.app import create_app
    from delve.config import get_settings
    from delve.log import configure_logging

    settings = get_settings()
    configure_logging(settings.log_level)
    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_config=None,
        proxy_headers=False,
        server_header=False,
        ws_ping_interval=None,  # the app sends its own keepalives (plan 6.7)
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="delve-backend")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("serve", help="run the HTTP/WebSocket server (default)")
    pair = sub.add_parser("pair", help="print a one-time code for pairing a phone")
    pair.add_argument("--name", help="device name (default: what the app sends)")
    pair.add_argument("--ttl", type=int, default=10, choices=range(1, 61), metavar="MINUTES")
    token = sub.add_parser("token", help="manage device tokens")
    tsub = token.add_subparsers(dest="action", required=True)
    create = tsub.add_parser("create", help="create a device and print its token once")
    create.add_argument("--name", required=True)
    tsub.add_parser("list", help="list devices")
    revoke = tsub.add_parser("revoke", help="revoke a device")
    revoke.add_argument("device_id")
    helper = sub.add_parser("helper", help="ask the privileged helper (as the backend would)")
    helper.add_argument("verb", choices=HELPER_VERBS)
    return parser


def _db_commands(args: argparse.Namespace) -> int:
    if args.command == "pair":
        try:
            return asyncio.run(_pair(args.name, args.ttl))
        except ValueError as exc:
            print(exc, file=sys.stderr)
            return 2
    if args.action == "create":
        return asyncio.run(_token_create(args.name))
    if args.action == "list":
        return asyncio.run(_token_list())
    return asyncio.run(_token_revoke(args.device_id))


def _db_name() -> str:
    try:
        return make_url(_db_url()).database or "?"
    except ArgumentError:
        return "?"


async def _helper(verb: str) -> int:
    from delve.brief import INCIDENTS_TIMEOUT_S
    from delve.system import CONTROL_TIMEOUT_S, STATUS_TIMEOUT_S, HelperClient, HelperFailed

    path = Path(os.environ.get("DELVE_HELPER_SOCKET", "/run/delve-helper.sock"))
    timeout = CONTROL_TIMEOUT_S if verb in ("llama.start", "llama.stop") else STATUS_TIMEOUT_S
    if verb == "agent.incidents":
        timeout = INCIDENTS_TIMEOUT_S
    try:
        data = await HelperClient(path).call(verb, timeout_s=timeout)
    except HelperFailed as exc:
        print(f"helper: {exc.code}: {exc.message}", file=sys.stderr)
        return 1
    except Exception as exc:  # HelperUnavailable and anything unexpected, one line
        print(f"helper not reachable at {path}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(data, indent=1))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command in (None, "serve"):
        return _serve()
    if args.command == "helper":
        return asyncio.run(_helper(args.verb))
    try:
        return _db_commands(args)
    except ProgrammingError as exc:
        if "does not exist" not in str(exc.orig):
            raise
        print(
            f"database '{_db_name()}' is not migrated: run `alembic upgrade head` "
            "(with the same DELVE_DATABASE_URL)",
            file=sys.stderr,
        )
        return 3
    except (OperationalError, InterfaceError, OSError) as exc:
        print(f"cannot reach database '{_db_name()}': {type(exc).__name__}", file=sys.stderr)
        return 3
