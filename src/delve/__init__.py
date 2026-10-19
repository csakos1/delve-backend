"""Delve backend."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("delve-backend")
except PackageNotFoundError:  # running from a source tree without installation
    __version__ = "0.0.0+src"

__all__ = ["__version__"]
