"""`delve-backend` / `python -m delve`: run the HTTP server bound to DELVE_BIND."""

from __future__ import annotations

import uvicorn

from delve.app import create_app
from delve.config import get_settings
from delve.log import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    uvicorn.run(
        create_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_config=None,
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
