from __future__ import annotations

import click

from .api import api
from .cron import cron
from .dashboard import dashboard
from .info import info
from .jobs import jobs
from .purge import purge
from .slowest import slowest
from .stats import stats
from .throughput import throughput
from .worker import worker


@click.group()
def cli() -> None:
    """wrk — postgres-backed job queue."""


cli.add_command(api)
cli.add_command(cron)
cli.add_command(worker)
cli.add_command(stats)
cli.add_command(jobs)
cli.add_command(throughput)
cli.add_command(slowest)
cli.add_command(dashboard)
cli.add_command(info)
cli.add_command(purge)


def main() -> None:
    cli()
