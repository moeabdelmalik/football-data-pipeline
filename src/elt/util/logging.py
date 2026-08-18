"""One place to configure logging, so every entrypoint logs identically."""

from __future__ import annotations

import logging
import sys


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-22s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,  # Airflow captures stdout into the task log
        force=True,
    )
    # requests/urllib3 are extremely chatty at DEBUG; keep our own logs readable.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
