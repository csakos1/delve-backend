"""Device tokens (plan 6.8, N28).

A token is shown once when it is created; only its SHA-256 is stored in `devices`.
Pairing with a one-time code (plan 12/8) will create tokens through the same functions.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from delve.db.models import Device

TOKEN_PREFIX = "dlv_"  # noqa: S105  # a prefix, not a secret
# last_seen_at is a hint, not an audit log: write it at most this often per device
LAST_SEEN_RESOLUTION = timedelta(minutes=1)


def new_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class DeviceIdentity:
    id: uuid.UUID
    name: str


async def create_device(session: AsyncSession, name: str) -> tuple[Device, str]:
    """Insert a device and return it with its plaintext token (never stored)."""
    name = name.strip()
    if not name or len(name) > 64:
        raise ValueError("device name must be 1-64 characters")
    token = new_token()
    device = Device(id=uuid.uuid4(), name=name, token_sha256=token_hash(token))
    session.add(device)
    await session.flush()
    return device, token


async def authenticate(session: AsyncSession, token: str | None) -> DeviceIdentity | None:
    """Return the device for a valid, non-revoked token; None otherwise."""
    if not token or not token.startswith(TOKEN_PREFIX) or len(token) > 128:
        return None
    digest = token_hash(token)
    device = (
        await session.execute(select(Device).where(Device.token_sha256 == digest))
    ).scalar_one_or_none()
    # the lookup is by hash; compare again in constant time for good measure
    if device is None or device.revoked or not hmac.compare_digest(device.token_sha256, digest):
        return None
    now = datetime.now(UTC)
    if device.last_seen_at is None or now - device.last_seen_at > LAST_SEEN_RESOLUTION:
        await session.execute(update(Device).where(Device.id == device.id).values(last_seen_at=now))
        await session.commit()
    return DeviceIdentity(id=device.id, name=device.name)


async def revoke_device(session: AsyncSession, device_id: uuid.UUID) -> bool:
    result = await session.execute(
        update(Device).where(Device.id == device_id, ~Device.revoked).values(revoked=True)
    )
    return bool(getattr(result, "rowcount", 0))
