from __future__ import annotations

import asyncio

import click

from .utils import load_app
from ..logging import configure_logging


@click.command()
@click.argument("app")
@click.option("--queues", "-q", default="default", show_default=True, help="Comma-separated list of queues to consume.")
@click.option("--concurrency", "-c", default=10, show_default=True, help="Maximum number of concurrent jobs.")
@click.option(
    "--worker-type",
    "-w",
    type=click.Choice(["async", "thread", "process", "fork"]),
    default="async",
    show_default=True,
    help="Worker execution model.",
)
@click.option(
    "--log-level",
    "-l",
    default="INFO",
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.option(
    "--log-format",
    default="text",
    show_default=True,
    type=click.Choice(["text", "json"], case_sensitive=False),
)
@click.option("--no-color", is_flag=True, default=False, help="Disable colored log output.")
def worker(
    app: str, queues: str, concurrency: int, worker_type: str, log_level: str, log_format: str, no_color: bool
) -> None:
    """Start a worker process.

    APP is the Wrk instance to use, e.g. ``myapp.tasks:wrk``
    """
    configure_logging(level=log_level, format=log_format, color=False if no_color else None)
    wrk_app = load_app(app)
    queue_list = [q.strip() for q in queues.split(",") if q.strip()]

    async def _run() -> None:
        async with wrk_app:
            await wrk_app.run(queues=queue_list, concurrency=concurrency, worker_type=worker_type)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
