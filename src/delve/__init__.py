"""Delve backend."""

import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    __version__ = version("delve-backend")
except PackageNotFoundError:  # running from a source tree without installation
    __version__ = "0.0.0+src"

# The app-facing contract (docs/contract.json). Bumped on every change a client can see;
# additive changes keep old clients working, so the app checks `api_revision >= N`.
API_REVISION = 6


def _read_revision() -> str | None:
    """The git sha of an installed release (deploy/install.sh writes
    /opt/delve/releases/<sha>/REVISION next to the venv). Read once at import: the
    `current` symlink may point to a newer release before this process restarts."""
    try:
        text = (Path(sys.prefix).resolve().parent / "REVISION").read_text().strip()
    except OSError:
        return None
    return text if re.fullmatch(r"[0-9a-f]{7,40}", text) else None


REVISION = _read_revision()

__all__ = ["API_REVISION", "REVISION", "__version__"]
