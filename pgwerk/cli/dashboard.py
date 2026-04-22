from __future__ import annotations

import asyncio

from datetime import datetime

import click

from .utils import ALL_STATUSES
from .utils import STATUS_STYLES
from .utils import bar
from .utils import fmt_age
from .utils import load_app
from .utils import short_func
from .utils import fmt_duration
from .utils import require_rich


@click.command()
@click.argument("app")
@click.option("--interval", "-i", default=5, show_default=True, help="Refresh interval in seconds.")
@click.option("--queue", "-q", default=None, help="Filter by queue.")
def dashboard(app: str, interval: int, queue: str | None) -> None:
    """Live auto-refreshing analytics dashboard."""
    from psycopg.sql import SQL

    console = require_rich()
    import rich.box as box

    from rich.live import Live
    from rich.text import Text
    from rich.panel import Panel
    from rich.table import Table
    from rich.columns import Columns
    from rich.console import Group

    wrk_app = load_app(app)

    async def _fetch(pool, t):
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    SELECT status, count(*)
                    FROM {jobs}
                    {where}
                    GROUP BY status
                """).format(
                    jobs=t["jobs"],
                    where=SQL("WHERE queue = %(q)s") if queue else SQL(""),
                ),
                {"q": queue} if queue else {},
            )
            counts = dict(await cur.fetchall())

            await cur.execute(
                SQL("""
                    SELECT name, queue, metadata, heartbeat_at
                    FROM {worker}
                    WHERE status = 'active'
                    ORDER BY name
                """).format(worker=t["worker"]),
            )
            workers = await cur.fetchall()

            await cur.execute(
                SQL("""
                    SELECT function, queue, status, enqueued_at, started_at, completed_at
                    FROM {jobs}
                    {where}
                    ORDER BY enqueued_at DESC
                    LIMIT 12
                """).format(
                    jobs=t["jobs"],
                    where=SQL("WHERE queue = %(q)s") if queue else SQL(""),
                ),
                {"q": queue} if queue else {},
            )
            recent = await cur.fetchall()

        return counts, workers, recent

    def _build(counts, workers, recent, last_refresh):
        total = sum(counts.values())

        status_table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim", padding=(0, 1))
        status_table.add_column("Status", min_width=10)
        status_table.add_column("Count", justify="right", min_width=6)
        status_table.add_column("Bar", min_width=20)
        for status in ALL_STATUSES:
            count = counts.get(status, 0)
            style = STATUS_STYLES.get(status, "white")
            status_table.add_row(
                Text(status, style=f"bold {style}"),
                Text(str(count), style=style),
                Text(bar(count, total, 18), style=style),
            )
        status_table.add_section()
        status_table.add_row(Text("total", style="bold"), Text(str(total), style="bold"), "")

        worker_table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim", padding=(0, 1))
        worker_table.add_column("Worker", min_width=14)
        worker_table.add_column("Queue")
        worker_table.add_column("Heartbeat", justify="right")
        for name, wqueue, _meta, hb in workers:
            worker_table.add_row(name or "—", wqueue or "—", fmt_age(hb))
        if not workers:
            worker_table.add_row(Text("no active workers", style="dim"), "", "")

        recent_table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim", padding=(0, 1))
        recent_table.add_column("Function", max_width=24)
        recent_table.add_column("Queue", max_width=10)
        recent_table.add_column("Status", min_width=10)
        recent_table.add_column("Age", justify="right")
        recent_table.add_column("Duration", justify="right")
        for func, q, s, enqueued_at, started_at, completed_at in recent:
            style = STATUS_STYLES.get(s, "white")
            duration = None
            if started_at and completed_at:
                duration = (completed_at - started_at).total_seconds()
            recent_table.add_row(
                short_func(func),
                q or "—",
                Text(s, style=f"bold {style}"),
                fmt_age(enqueued_at),
                fmt_duration(duration),
            )

        top = Columns(
            [
                Panel(status_table, title="[bold]Job Status[/bold]", border_style="dim"),
                Panel(worker_table, title=f"[bold]Workers ({len(workers)} active)[/bold]", border_style="dim"),
            ],
            expand=True,
        )
        bottom = Panel(recent_table, title="[bold]Recent Jobs[/bold]", border_style="dim")
        footer = Text(
            f" Refreshing every {interval}s · Last: {last_refresh} · Ctrl+C to exit",
            style="dim",
        )
        return Group(top, bottom, footer)

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            counts, workers, recent = await _fetch(pool, wrk_app._t)
            last_refresh = datetime.now().strftime("%H:%M:%S")

            with Live(
                _build(counts, workers, recent, last_refresh), console=console, refresh_per_second=2, screen=True
            ) as live:
                try:
                    while True:
                        await asyncio.sleep(interval)
                        counts, workers, recent = await _fetch(pool, wrk_app._t)
                        last_refresh = datetime.now().strftime("%H:%M:%S")
                        live.update(_build(counts, workers, recent, last_refresh))
                except (KeyboardInterrupt, asyncio.CancelledError):
                    pass

    asyncio.run(_run())
