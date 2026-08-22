"""Run the ingestion service with graceful SIGINT and SIGTERM shutdown."""

from __future__ import annotations

import asyncio
import logging
import signal

from hydromancer_eda.config import get_settings
from hydromancer_eda.ingestion.allfills import AllFillsIngestionService


async def _main() -> None:
    """Configure signal handling and run the allFills ingestion service."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await AllFillsIngestionService(get_settings()).run_forever(stop_event)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main())
