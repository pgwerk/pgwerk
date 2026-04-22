from __future__ import annotations

import asyncio

import click

from .utils import STATUS_STYLES
from .utils import fmt_age
from .utils import load_app
from .utils import short_func
from .utils import fmt_duration
from .utils import require_rich


@click.command()
@click.argument("app")
@click.option("--queue", "-q", default=None, help="Filter by queue.")
@click.option("--status", "-s", default=None, help="Filter by status (comma-separated).")
@click.option("--limit", "-n", default=20, show_default=True, help="Max rows.")
@click.option("--offset", default=0, help="Row offset for pagination.")
def jobs(app: str, queue: str | None, status: str | None, limit: int, offset: int) -> None:
    """List recent jobs in a formatted table."""
    from psycopg.sql import SQL

    console = require_rich()
    import rich.box as box

    from rich.text import Text
    from rich.table import Table

    wrk_app = load_app(app)
    statuses = [s.strip() for s in status.split(",")] if status else None

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            async with pool.connection() as conn, conn.cursor() as cur:
                conditions = []
                params: dict = {"limit": limit, "offset": offset}
                if queue:
                    conditions.append(SQL("queue = %(queue)s"))
                    params["queue"] = queue
                if statuses:
                    conditions.append(SQL("status = ANY(%(statuses)s)"))
                    params["statuses"] = statuses
                where_clause = SQL("WHERE ") + SQL(" AND ").join(conditions) if conditions else SQL("")

                await cur.execute(
                    SQL("""
                        SELECT id, function, queue, status, priority,
                               attempts, max_attempts, enqueued_at,
                               started_at, completed_at, worker_id, error
                        FROM {jobs}
                        {where}
                        ORDER BY enqueued_at DESC
                        LIMIT %(limit)s OFFSET %(offset)s
                    """).format(jobs=wrk_app._t["jobs"], where=where_clause),
                    params,
                )
                rows = await cur.fetchall()

        title = f"Jobs — {queue or 'all queues'}"
        if statuses:
            title += f" [{', '.join(statuses)}]"

        table = Table(title=title, box=box.ROUNDED, header_style="bold dim", show_lines=False)
        table.add_column("ID", max_width=10)
        table.add_column("Function", max_width=28)
        table.add_column("Queue", max_width=12)
        table.add_column("Status", min_width=10)
        table.add_column("Tries", justify="right")
        table.add_column("Enqueued", min_width=10)
        table.add_column("Duration", justify="right", min_width=8)
        table.add_column("Error", max_width=30)

        for row in rows:
            (
                job_id,
                func,
                q,
                s,
                _priority,
                attempts,
                max_attempts,
                enqueued_at,
                started_at,
                completed_at,
                _worker_id,
                error,
            ) = row
            style = STATUS_STYLES.get(s, "white")
            short_id = str(job_id)[:8] + "…"
            duration = None
            if started_at and completed_at:
                duration = (completed_at - started_at).total_seconds()
            err_display = ""
            if error:
                first_line = error.split("\n")[0]
                err_display = first_line[:28] + ("…" if len(first_line) > 28 else "")

            table.add_row(
                Text(short_id, style="dim"),
                short_func(func),
                q or "—",
                Text(s, style=f"bold {style}"),
                f"{attempts}/{max_attempts}",
                fmt_age(enqueued_at),
                Text(fmt_duration(duration), style="dim"),
                Text(err_display, style="dim red") if err_display else "",
            )

        console.print(table)
        if len(rows) == limit:
            console.print(f"[dim]Showing {limit} rows. Use --offset {offset + limit} for next page.[/dim]")

    asyncio.run(_run())
