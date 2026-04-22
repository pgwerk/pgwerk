from __future__ import annotations

import asyncio

import click

from .utils import load_app
from .utils import short_func
from .utils import parse_since
from .utils import fmt_duration
from .utils import require_rich


@click.command()
@click.argument("app")
@click.option("--queue", "-q", default=None, help="Filter by queue.")
@click.option("--limit", "-n", default=10, show_default=True, help="Number of functions to show.")
@click.option("--since", default="24h", show_default=True, help="Time window e.g. 24h, 7d, 30m.")
def slowest(app: str, queue: str | None, limit: int, since: str) -> None:
    """Show slowest job functions by average execution time."""
    from psycopg.sql import SQL

    console = require_rich()
    import rich.box as box

    from rich.text import Text
    from rich.table import Table

    wrk_app = load_app(app)
    since_dt = parse_since(since)

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    SQL("""
                        SELECT
                            function,
                            count(*) AS runs,
                            avg(EXTRACT(EPOCH FROM (completed_at - started_at)))::float AS avg_secs,
                            percentile_cont(0.5) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                            ) AS p50_secs,
                            percentile_cont(0.95) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                            ) AS p95_secs,
                            max(EXTRACT(EPOCH FROM (completed_at - started_at)))::float AS max_secs,
                            count(*) FILTER (WHERE status = 'failed') AS failures
                        FROM {jobs}
                        WHERE status IN ('complete', 'failed')
                          AND started_at IS NOT NULL
                          AND completed_at IS NOT NULL
                          AND completed_at > %(since)s
                          {where}
                        GROUP BY function
                        ORDER BY avg_secs DESC NULLS LAST
                        LIMIT %(limit)s
                    """).format(
                        jobs=wrk_app._t["jobs"],
                        where=SQL("AND queue = %(queue)s") if queue else SQL(""),
                    ),
                    {"since": since_dt, "limit": limit, "queue": queue}
                    if queue
                    else {"since": since_dt, "limit": limit},
                )
                rows = await cur.fetchall()

        if not rows:
            console.print(f"[yellow]No jobs completed in the last {since}.[/yellow]")
            return

        table = Table(
            title=f"Slowest Functions — last {since}{' [' + queue + ']' if queue else ''}",
            box=box.ROUNDED,
            header_style="bold dim",
        )
        table.add_column("#", justify="right", min_width=3)
        table.add_column("Function", min_width=20)
        table.add_column("Runs", justify="right")
        table.add_column("Avg", justify="right", min_width=8)
        table.add_column("p50", justify="right", min_width=8)
        table.add_column("p95", justify="right", min_width=8)
        table.add_column("Max", justify="right", min_width=8)
        table.add_column("Failures", justify="right")

        for i, (func, runs, avg_s, p50_s, p95_s, max_s, failures) in enumerate(rows, 1):
            avg_style = "green" if avg_s < 1 else ("yellow" if avg_s < 10 else "bright_red")
            p95_style = "yellow" if p95_s and p95_s > 5 else ("bright_red" if p95_s and p95_s > 30 else "")
            fail_style = "bright_red" if failures and int(failures) > 0 else "dim"
            table.add_row(
                str(i),
                short_func(func),
                str(runs),
                Text(fmt_duration(avg_s), style=avg_style),
                fmt_duration(p50_s),
                Text(fmt_duration(p95_s), style=p95_style),
                Text(fmt_duration(max_s), style="bright_red" if max_s and max_s > 30 else ""),
                Text(str(int(failures)), style=fail_style),
            )
        console.print(table)

    asyncio.run(_run())
