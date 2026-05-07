from __future__ import annotations

import asyncio

import click

import psycopg

from ..config import WerkConfig
from ..database import DatabaseManager


@click.command()
@click.option("--dsn", envvar="PGWERK_DSN", required=True, help="Postgres connection string.")
@click.option("--schema", default=None, envvar="PGWERK_SCHEMA", help="Postgres schema for wrk tables.")
@click.option("--prefix", default=None, envvar="PGWERK_PREFIX", help="Table-name prefix.")
def migrate(dsn: str, schema: str | None, prefix: str | None) -> None:
    """Create or migrate the wrk schema."""

    async def _run() -> None:
        defaults = WerkConfig()
        db = DatabaseManager(schema or defaults.schema, prefix or defaults.prefix)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await db.migrate(conn)
        click.echo("Migration complete.")

    asyncio.run(_run())
