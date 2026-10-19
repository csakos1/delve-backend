"""`delve-backend` / `python -m delve`: see delve.cli."""

from __future__ import annotations

import sys

from delve.cli import main as _main


def main() -> None:
    sys.exit(_main())


if __name__ == "__main__":
    main()
