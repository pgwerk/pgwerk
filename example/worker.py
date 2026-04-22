"""
Starts a worker that processes jobs from the default queue.

    cd example
    python worker.py
"""

import os
import asyncio
import logging

from pgwerk import Werk
from pgwerk import AsyncWorker
from pgwerk import configure_logging


configure_logging(logger_name="")

logger = logging.getLogger(__name__)
CONCURRENCY = 10
QUEUES = ["default", "email", "media", "billing"]

app = Werk(
    os.environ.get("PGWERK_DSN", "postgresql://werk:wrk@localhost/wrk"),
    max_pool_size=CONCURRENCY + 5,  # listen loop holds 1 conn permanently; leave headroom for concurrent acks
)


async def main() -> None:
    async with app:
        worker = AsyncWorker(app=app, queues=QUEUES, concurrency=CONCURRENCY)
        logger.info("Worker started on queues=%s concurrency=%d. Press Ctrl+C to stop.", QUEUES, CONCURRENCY)
        await worker.run()


asyncio.run(main())
