"""agentcards – közös kártya-sémák (plugin + no-agent scriptek). Csak stdlib."""
from __future__ import annotations

import math
import re
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


# --- updates (csak a no-agent script publikálja, LLM-tool nincs hozzá) -------------
UPDATES_FIELDS = ("count", "packages", "checked_at", "data_source")
UPDATES_MAX_PACKAGES = 20
_PKG_CHARS = set("abcdefghijklmnopqrstuvwxyz0123456789@._+-")


def validate_updates(p, now):
    if not isinstance(p, dict):
        raise CardValidationError(["payload: must be an object"])
    errs = []
    unknown = sorted(set(p) - set(UPDATES_FIELDS))
    if unknown:
        errs.append(f"unknown fields: {', '.join(map(str, unknown))}")
    count = p.get("count")
    if isinstance(count, bool) or not isinstance(count, int):
        errs.append("count: required integer")
        count = None
    elif not 0 <= count <= 100000:
        errs.append("count: out of range [0, 100000]")
        count = None
    packages = p.get("packages", [])
    clean_pkgs = []
    if not isinstance(packages, list):
        errs.append("packages: must be a list")
    elif len(packages) > UPDATES_MAX_PACKAGES:
        errs.append(f"packages: max {UPDATES_MAX_PACKAGES} names")
    else:
        for name in packages:
            if (not isinstance(name, str) or not 0 < len(name) <= 128
                    or not set(name.lower()) <= _PKG_CHARS):
                errs.append("packages: names must be pacman package names")
                break
            clean_pkgs.append(name)
    if count is not None and len(clean_pkgs) > count:
        errs.append("packages: more names than count")
    out = {
        "count": count,
        "packages": clean_pkgs,
        "checked_at": _ts(p.get("checked_at"), "checked_at", errs, now),
        "data_source": _str(p.get("data_source"), "data_source", 120, errs, False),
    }
    if errs:
        raise CardValidationError(errs)
    return {k: v for k, v in out.items() if v is not None}


# --- watch_* (csak a watchlib publikálja: figyelők, Delve W) -----------------------
WATCH_SOURCES = ("hardverapro", "cinemacity", "page")
WATCH_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{1,39}")
WATCH_ITEM_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
WATCH_HASH_RE = re.compile(r"[0-9a-f]{16}")
WATCH_MAX_WORDS = 10
WATCH_MAX_PRICE = 1_000_000_000


def _bool(v, name, errs, required, default=None):
    if v is None:
        if required:
            errs.append(f"{name}: required boolean")
        return default
    if not isinstance(v, bool):
        errs.append(f"{name}: must be a boolean")
        return None
    return v


def _int(v, name, lo, hi, errs, required):
    if v is None:
        if required:
            errs.append(f"{name}: required integer")
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        errs.append(f"{name}: must be an integer")
        return None
    if not lo <= v <= hi:
        errs.append(f"{name}: out of range [{lo}, {hi}]")
        return None
    return v


def _match(v, name, rx, max_len, errs, required):
    s = _str(v, name, max_len, errs, required)
    if s is not None and not rx.fullmatch(s):
        errs.append(f"{name}: invalid format")
        return None
    return s


def _url(v, name, errs, required):
    s = _str(v, name, 600, errs, required)
    if s is not None and not re.fullmatch(r"https?://[^\s\"'<>]+", s):
        errs.append(f"{name}: must be an http(s) URL")
        return None
    return s


def _words(v, name, errs):
    if v is None:
        return []
    if not isinstance(v, list) or len(v) > WATCH_MAX_WORDS:
        errs.append(f"{name}: list of at most {WATCH_MAX_WORDS} words")
        return None
    out = []
    for w in v:
        if not isinstance(w, str) or not 0 < len(w.strip()) <= 40:
            errs.append(f"{name}: each entry is a non-empty string of at most 40 chars")
            return None
        out.append(w.strip())
    return out


def _unknown(p, fields, errs):
    unknown = sorted(set(p) - set(fields))
    if unknown:
        errs.append(f"unknown fields: {', '.join(map(str, unknown))}")


WATCH_DEF_FIELDS = ("slug", "name", "source", "url", "include", "any", "exclude",
                    "min_price", "max_price", "criteria", "fetch_details",
                    "stop_after_hit", "notify_existing", "schedule", "def_hash")


def validate_watch_def(p, now):
    if not isinstance(p, dict):
        raise CardValidationError(["payload: must be an object"])
    errs = []
    _unknown(p, WATCH_DEF_FIELDS, errs)
    source = _str(p.get("source"), "source", 20, errs, True)
    if source is not None and source not in WATCH_SOURCES:
        errs.append(f"source: must be one of {', '.join(WATCH_SOURCES)}")
    out = {
        "slug": _match(p.get("slug"), "slug", WATCH_SLUG_RE, 40, errs, True),
        "name": _str(p.get("name"), "name", 80, errs, True),
        "source": source,
        "url": _url(p.get("url"), "url", errs, True),
        "include": _words(p.get("include"), "include", errs),
        "any": _words(p.get("any"), "any", errs),
        "exclude": _words(p.get("exclude"), "exclude", errs),
        "min_price": _int(p.get("min_price"), "min_price", 0, WATCH_MAX_PRICE, errs, False),
        "max_price": _int(p.get("max_price"), "max_price", 0, WATCH_MAX_PRICE, errs, False),
        "criteria": _str(p.get("criteria"), "criteria", 500, errs, False),
        "fetch_details": _bool(p.get("fetch_details"), "fetch_details", errs, False, False),
        "stop_after_hit": _bool(p.get("stop_after_hit"), "stop_after_hit", errs, False, False),
        "notify_existing": _bool(p.get("notify_existing"), "notify_existing", errs, False, False),
        "schedule": _str(p.get("schedule"), "schedule", 60, errs, True),
        "def_hash": _match(p.get("def_hash"), "def_hash", WATCH_HASH_RE, 16, errs, True),
    }
    if errs:
        raise CardValidationError(errs)
    return {k: v for k, v in out.items() if v is not None}


WATCH_RUN_FIELDS = ("slug", "status", "checked_at", "items", "matched", "new", "candidates",
                    "baseline", "error_code", "error", "duration_ms", "final_url")
WATCH_RUN_STATUSES = ("ok", "error")


def validate_watch_run(p, now):
    if not isinstance(p, dict):
        raise CardValidationError(["payload: must be an object"])
    errs = []
    _unknown(p, WATCH_RUN_FIELDS, errs)
    status = _str(p.get("status"), "status", 10, errs, True)
    if status is not None and status not in WATCH_RUN_STATUSES:
        errs.append("status: must be ok or error")
    out = {
        "slug": _match(p.get("slug"), "slug", WATCH_SLUG_RE, 40, errs, True),
        "status": status,
        "checked_at": _ts(p.get("checked_at"), "checked_at", errs, now),
        "items": _int(p.get("items"), "items", 0, 100000, errs, status == "ok"),
        "matched": _int(p.get("matched"), "matched", 0, 100000, errs, status == "ok"),
        "new": _int(p.get("new"), "new", 0, 100000, errs, status == "ok"),
        "candidates": _int(p.get("candidates"), "candidates", 0, 1000, errs, status == "ok"),
        "baseline": _bool(p.get("baseline"), "baseline", errs, False, False),
        "error_code": _match(p.get("error_code"), "error_code", re.compile(r"[a-z0-9_]{1,40}"),
                             40, errs, status == "error"),
        "error": _str(p.get("error"), "error", 300, errs, False),
        "duration_ms": _int(p.get("duration_ms"), "duration_ms", 0, 3_600_000, errs, False),
        "final_url": _url(p.get("final_url"), "final_url", errs, False),
    }
    if errs:
        raise CardValidationError(errs)
    return {k: v for k, v in out.items() if v is not None}


WATCH_CANDIDATE_FIELDS = ("slug", "item_id", "title", "url", "price", "price_value",
                          "location", "posted", "excerpt", "reserved", "change")
# change: an item announced earlier came back in a new state ("available": no longer reserved)
WATCH_CHANGES = ("available",)


def validate_watch_candidate(p, now):
    if not isinstance(p, dict):
        raise CardValidationError(["payload: must be an object"])
    errs = []
    _unknown(p, WATCH_CANDIDATE_FIELDS, errs)
    out = {
        "slug": _match(p.get("slug"), "slug", WATCH_SLUG_RE, 40, errs, True),
        "item_id": _match(p.get("item_id"), "item_id", WATCH_ITEM_ID_RE, 64, errs, True),
        "title": _str(p.get("title"), "title", 300, errs, True),
        "url": _url(p.get("url"), "url", errs, True),
        "price": _str(p.get("price"), "price", 60, errs, False),
        "price_value": _int(p.get("price_value"), "price_value", 0, WATCH_MAX_PRICE, errs, False),
        "location": _str(p.get("location"), "location", 120, errs, False),
        "posted": _str(p.get("posted"), "posted", 60, errs, False),
        "excerpt": _str(p.get("excerpt"), "excerpt", 1500, errs, False),
        "reserved": _bool(p.get("reserved"), "reserved", errs, False),
        "change": _str(p.get("change"), "change", 20, errs, False),
    }
    if out["change"] is not None and out["change"] not in WATCH_CHANGES:
        errs.append(f"change: must be one of {', '.join(WATCH_CHANGES)}")
    if errs:
        raise CardValidationError(errs)
    return {k: v for k, v in out.items() if v is not None}


VALIDATORS = {"water": validate_water, "updates": validate_updates,
              "watch_def": validate_watch_def, "watch_run": validate_watch_run,
              "watch_candidate": validate_watch_candidate}


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
