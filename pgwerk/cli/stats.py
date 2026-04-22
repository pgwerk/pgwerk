from __future__ import annotations

import asyncio

import click

from .utils import ALL_STATUSES
from .utils import STATUS_STYLES
from .utils import bar
from .utils import fmt_age
from .utils import load_app
from .utils import require_rich


@click.command()
@click.argument("app")
@click.option("--queue", "-q", default=None, help="Filter by queue name.")
def stats(app: str, queue: str | None) -> None:
    """Show queue statistics with visual breakdown."""
    from psycopg.sql import SQL

    console = require_rich()
    import rich.box as box

    from rich.text import Text
    from rich.table import Table

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
                    """).format(
                        jobs=wrk_app._t["jobs"],
                        where=SQL("WHERE queue = %(q)s") if queue else SQL(""),
                    ),
                    {"q": queue} if queue else {},
                )
                counts = dict(await cur.fetchall())

                if not queue:
                    await cur.execute(
                        SQL("""
                            SELECT queue, status, count(*)
                            FROM {jobs}
                            GROUP BY queue, status
                            ORDER BY queue, status
                        """).format(jobs=wrk_app._t["jobs"]),
                    )
                    queue_rows = await cur.fetchall()
                else:
                    queue_rows = []

                await cur.execute(
                    SQL("""
                        SELECT name, queue, metadata, heartbeat_at
                        FROM {worker}
                        WHERE status = 'active'
                        ORDER BY name
                    """).format(worker=wrk_app._t["worker"]),
                )
                workers = await cur.fetchall()

        console.rule("[bold]wrk stats[/bold]")

        total = sum(counts.values())
        status_table = Table(
            title=f"Job Status — {queue or 'all queues'}",
            box=box.ROUNDED,
            header_style="bold dim",
            show_header=True,
        )
        status_table.add_column("Status", min_width=10)
        status_table.add_column("Count", justify="right", min_width=7)
        status_table.add_column("Distribution", min_width=32)
        status_table.add_column("%", justify="right", min_width=6)

        for status in ALL_STATUSES:
            count = counts.get(status, 0)
            style = STATUS_STYLES.get(status, "white")
            pct = f"{count / total * 100:.1f}" if total else "—"
            status_table.add_row(
                Text(status, style=f"bold {style}"),
                Text(str(count), style=style),
                Text(bar(count, total, 28), style=style),
                pct,
            )
        status_table.add_section()
        status_table.add_row(Text("total", style="bold"), Text(str(total), style="bold"), "", "")
        console.print(status_table)

        if queue_rows:
            queues_data: dict[str, dict[str, int]] = {}
            for q, s, c in queue_rows:
                queues_data.setdefault(q, {})[s] = int(c)

            qtable = Table(title="By Queue", box=box.ROUNDED, header_style="bold dim")
            qtable.add_column("Queue", min_width=12)
            for s in ["scheduled", "queued", "active", "waiting", "complete", "failed", "aborted"]:
                qtable.add_column(s.capitalize(), justify="right")

            for q_name, q_counts in sorted(queues_data.items()):
                qtable.add_row(
                    q_name,
                    Text(str(q_counts.get("scheduled", 0)), style="cyan"),
                    Text(str(q_counts.get("queued", 0)), style="bright_blue"),
                    Text(str(q_counts.get("active", 0)), style="bright_green"),
                    Text(str(q_counts.get("waiting", 0)), style="yellow"),
                    Text(str(q_counts.get("complete", 0)), style="green"),
                    Text(str(q_counts.get("failed", 0)), style="bright_red"),
                    Text(str(q_counts.get("aborted", 0)), style="orange1"),
                )
            console.print(qtable)

        wtable = Table(title=f"Workers ({len(workers)} active)", box=box.ROUNDED, header_style="bold dim")
        wtable.add_column("Name", min_width=16)
        wtable.add_column("Queue")
        wtable.add_column("Concurrency", justify="right")
        wtable.add_column("Last Heartbeat")

        for name, wqueue, meta, hb in workers:
            concurrency = str(meta.get("concurrency", "?")) if isinstance(meta, dict) else "?"
            wtable.add_row(name or "—", wqueue or "—", concurrency, fmt_age(hb))

        if not workers:
            wtable.add_row("[dim]no active workers[/dim]", "", "", "")

        console.print(wtable)

    asyncio.run(_run())
