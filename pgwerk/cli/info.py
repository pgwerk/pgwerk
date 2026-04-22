from __future__ import annotations

import asyncio

import click

from .utils import load_app


@click.command()
@click.argument("app")
@click.option("--queue", "-q", default=None, help="Filter by queue name.")
def info(app: str, queue: str | None) -> None:
    """Print queue statistics and active workers."""
    from psycopg.sql import SQL

    from ..commons import JobStatus

    wrk_app = load_app(app)

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    SQL("""
                        SELECT status, count(*)
                        FROM {jobs}
                        {where}
                        GROUP BY status
                        ORDER BY status
                    """).format(
                        jobs=wrk_app._t["jobs"],
                        where=SQL("WHERE queue = %(q)s") if queue else SQL(""),
                    ),
                    {"q": queue} if queue else {},
                )
                counts = dict(await cur.fetchall())

                await cur.execute(
                    SQL("""
                        SELECT name, queue, heartbeat_at
                        FROM {worker}
                        WHERE status = 'active'
                        ORDER BY name
                    """).format(worker=wrk_app._t["worker"]),
                )
                workers = await cur.fetchall()

            click.echo("Jobs:")
            for s in JobStatus:
                click.echo(f"  {s.value:<12} {counts.get(s.value, 0)}")

            click.echo(f"\nWorkers ({len(workers)} active):")
            for name, wqueue, hb in workers:
                click.echo(f"  {name}  queues={wqueue}  heartbeat={hb}")

    asyncio.run(_run())
