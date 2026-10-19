"""Device tokens (plan 6.8, N28).

A token is shown once when it is created; only its SHA-256 is stored in `devices`.

Pairing (plan 12/8, decision 161): `delve-backend pair` prints an 8-digit one-time code
(10 minutes); the app sends it to `POST /api/v1/devices/register` and receives a token.
A new code replaces any unused one, and only the code's SHA-256 is stored.
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

from delve.db.models import Device, PairingCode

PAIRING_DIGITS = 8
PAIRING_TTL = timedelta(minutes=10)
FCM_TOKEN_MAX = 4096
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
        update(Device)
        .where(Device.id == device_id, ~Device.revoked)
        .values(revoked=True, fcm_token=None)  # a revoked phone gets no more pushes
    )
    return bool(getattr(result, "rowcount", 0))


def normalize_code(code: str) -> str | None:
    """Digits only ("1234-5678", "1234 5678" are fine); None if not 8 digits."""
    digits = "".join(c for c in code if c.isdigit())
    if len(digits) != PAIRING_DIGITS or len(code) > 32:
        return None
    return digits


def format_code(code: str) -> str:
    return f"{code[:4]}-{code[4:]}"


async def create_pairing_code(
    session: AsyncSession, *, name: str | None = None, ttl: timedelta = PAIRING_TTL
) -> tuple[str, datetime]:
    """A fresh code; unused earlier codes stop working. Returns (code, expires_at)."""
    if name is not None:
        name = name.strip()
        if not name or len(name) > 64:
            raise ValueError("device name must be 1-64 characters")
    now = datetime.now(UTC)
    await session.execute(
        update(PairingCode)
        .where(PairingCode.used_at.is_(None), PairingCode.expires_at > now)
        .values(expires_at=now)
    )
    code = "".join(secrets.choice("0123456789") for _ in range(PAIRING_DIGITS))
    expires = now + ttl
    session.add(
        PairingCode(id=uuid.uuid4(), code_sha256=token_hash(code), name=name, expires_at=expires)
    )
    await session.flush()
    return code, expires


async def redeem_pairing_code(
    session: AsyncSession, code: str, name: str | None
) -> tuple[Device, str] | None:
    """Use a valid code once: create the device and return it with its token.

    The caller commits. Unknown, used and expired codes all return None.
    """
    digits = normalize_code(code)
    if digits is None:
        return None
    pc = (
        await session.execute(
            select(PairingCode)
            .where(
                PairingCode.code_sha256 == token_hash(digits),
                PairingCode.used_at.is_(None),
                PairingCode.expires_at > datetime.now(UTC),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if pc is None:
        return None
    device, token = await create_device(session, pc.name or (name or "").strip() or "phone")
    pc.used_at, pc.device_id = datetime.now(UTC), device.id
    return device, token


async def set_fcm_token(session: AsyncSession, device_id: uuid.UUID, token: str | None) -> None:
    if token is not None and (not token.strip() or len(token) > FCM_TOKEN_MAX):
        raise ValueError(f"FCM token must be 1-{FCM_TOKEN_MAX} characters")
    await session.execute(update(Device).where(Device.id == device_id).values(fcm_token=token))
