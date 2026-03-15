from __future__ import annotations

import asyncio

import click

from .utils import load_app
from ..logging import configure_logging


@click.command()
@click.argument("scheduler")
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
def cron(scheduler: str, log_level: str, log_format: str, no_color: bool) -> None:
    """Start a cron scheduler process.

    SCHEDULER is the CronScheduler instance to use, e.g. ``myapp.tasks:scheduler``
    """
    configure_logging(level=log_level, format=log_format, color=False if no_color else None)
    cron_scheduler = load_app(scheduler)

    async def _run() -> None:
        async with cron_scheduler.app:
            await cron_scheduler.run()

    asyncio.run(_run())
