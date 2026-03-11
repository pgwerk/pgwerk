from __future__ import annotations

import asyncio

import click

from .utils import load_app


@click.command()
@click.argument("app")
@click.option("--status", "-s", default="complete,failed,aborted", help="Comma-separated statuses to delete.")
@click.option("--queue", "-q", default=None, help="Limit to a specific queue.")
@click.confirmation_option(prompt="Purge matching jobs?")
def purge(app: str, status: str, queue: str | None) -> None:
    """Delete finished jobs from the database."""
    from psycopg.sql import SQL

    wrk_app = load_app(app)
    statuses = [s.strip() for s in status.split(",")]

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    SQL("""
                        DELETE FROM {jobs}
                        WHERE status = ANY(%(statuses)s)
                        {where}
                        RETURNING id
                    """).format(
                        jobs=wrk_app._t["jobs"],
                        where=SQL("AND queue = %(q)s") if queue else SQL(""),
                    ),
                    {"statuses": statuses, "q": queue} if queue else {"statuses": statuses},
                )
                deleted = len(await cur.fetchall())
            click.echo(f"Purged {deleted} job(s).")

    asyncio.run(_run())
