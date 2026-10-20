"""agentcards – közös kártya-sémák (plugin + no-agent scriptek). Csak stdlib."""
from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta, timezone

SCHEMA_VERSION = 1
SEVERITIES = ("info", "warn", "alert")
ORIGINS = ("agent", "script")


class CardValidationError(ValueError):
    def __init__(self, errors):
        super().__init__("; ".join(errors))
        self.errors = list(errors)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _num(v, name, lo, hi, errs, required):
    if v is None:
        if required:
            errs.append(f"{name}: required")
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        errs.append(f"{name}: must be a finite number")
        return None
    if not lo <= v <= hi:
        errs.append(f"{name}: out of range [{lo}, {hi}]")
        return None
    return round(float(v), 1)


def _str(v, name, max_len, errs, required):
    if v is None or (isinstance(v, str) and not v.strip()):
        if required:
            errs.append(f"{name}: required (non-empty string)")
        return None
    if not isinstance(v, str):
        errs.append(f"{name}: must be a string")
        return None
    v = v.strip()
    if len(v) > max_len:
        errs.append(f"{name}: max {max_len} chars")
        return None
    return v


def _ts(v, name, errs, now):
    s = _str(v, name, 40, errs, True)
    if s is None:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        errs.append(f"{name}: not ISO 8601")
        return None
    if dt.tzinfo is None:
        errs.append(f"{name}: timezone required (e.g. Z or +02:00)")
        return None
    if dt > now + timedelta(minutes=15):
        errs.append(f"{name}: in the future")
        return None
    if dt < now - timedelta(days=30):
        errs.append(f"{name}: older than 30 days")
        return None
    return _iso_z(dt)


# --- water --------------------------------------------------------------------
WATER_FIELDS = ("station", "level_cm", "measured_at", "trend_cm_24h",
                "lake_avg_cm", "lake_avg_trend_cm_24h",
                "threshold_cm", "data_source", "note")


def validate_water(p, now):
    if not isinstance(p, dict):
        raise CardValidationError(["payload: must be an object"])
    errs = []
    unknown = sorted(set(p) - set(WATER_FIELDS))
    if unknown:
        errs.append(f"unknown fields: {', '.join(map(str, unknown))}")
    out = {
        "station": _str(p.get("station"), "station", 64, errs, True),
        "level_cm": _num(p.get("level_cm"), "level_cm", -500, 1500, errs, True),
        "measured_at": _ts(p.get("measured_at"), "measured_at", errs, now),
        "trend_cm_24h": _num(p.get("trend_cm_24h"), "trend_cm_24h", -300, 300, errs, False),
        "lake_avg_cm": _num(p.get("lake_avg_cm"), "lake_avg_cm", -500, 1500, errs, False),
        "lake_avg_trend_cm_24h": _num(p.get("lake_avg_trend_cm_24h"), "lake_avg_trend_cm_24h",
                                      -300, 300, errs, False),
        "threshold_cm": _num(p.get("threshold_cm"), "threshold_cm", -500, 1500, errs, False),
        "data_source": _str(p.get("data_source"), "data_source", 120, errs, False),
        "note": _str(p.get("note"), "note", 280, errs, False),
    }
    if errs:
        raise CardValidationError(errs)
    return {k: v for k, v in out.items() if v is not None}


VALIDATORS = {"water": validate_water}


def build_event(card_type, payload, severity, origin, session_id=None, now=None):
    now = now or datetime.now(timezone.utc)
    if card_type not in VALIDATORS:
        raise CardValidationError([f"type: unknown card type {card_type!r}"])
    errs = []
    if severity not in SEVERITIES:
        errs.append(f"severity: must be one of {', '.join(SEVERITIES)}")
    if origin not in ORIGINS:
        errs.append(f"origin: must be one of {', '.join(ORIGINS)}")
    clean = None
    try:
        clean = VALIDATORS[card_type](payload, now)
    except CardValidationError as e:
        errs.extend(e.errors)
    if errs:
        raise CardValidationError(errs)
    ev = {"v": SCHEMA_VERSION, "id": uuid.uuid4().hex, "type": card_type,
          "severity": severity, "ts": _iso_z(now), "origin": origin, "payload": clean}
    if session_id:
        ev["session_id"] = str(session_id)[:128]
    return ev


# --- LLM-facing tool schemas -------------------------------------------------------
PUBLISH_WATER_CARD_SCHEMA = {
    "name": "publish_water_card",
    "description": (
        "Publish a water-level card to the user's phone dashboard (Lake Balaton / harbour gauge). "
        "Use ONLY values you obtained from a tool output in this session (a measurement or a "
        "script result) - never estimate or invent numbers. Publishing runs nothing on the system. "
        "On a validation error, fix the listed fields once; do not retry more than once."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "severity": {"type": "string", "enum": list(SEVERITIES),
                         "description": "info = routine update, warn = approaching threshold, alert = below threshold / action needed"},
            "station": {"type": "string", "description": "Gauge / station name, e.g. Siófok"},
            "level_cm": {"type": "number", "description": "Water level in cm (gauge reading)"},
            "measured_at": {"type": "string",
                            "description": "Measurement time, ISO 8601 with timezone, e.g. 2026-10-16T17:00:00Z"},
            "trend_cm_24h": {"type": "number", "description": "Change over the last 24 h in cm (negative = falling)"},
            "lake_avg_cm": {"type": "number", "description": "Lake Balaton average water level in cm (official lake-wide average, same gauge datum)"},
            "lake_avg_trend_cm_24h": {"type": "number", "description": "Change of the lake-wide average over the last 24 h in cm (negative = falling)"},
            "threshold_cm": {"type": "number", "description": "Alert threshold in cm"},
            "data_source": {"type": "string", "description": "Where the value came from"},
            "note": {"type": "string", "description": "Short note for the user (max 280 chars)"},
        },
        "required": ["severity", "station", "level_cm", "measured_at"],
        "additionalProperties": False,
    },
}
