from __future__ import annotations

import asyncio

from datetime import datetime
from datetime import timezone
from datetime import timedelta

import click

from .utils import load_app
from .utils import require_rich


@click.command()
@click.argument("app")
@click.option("--hours", default=24, show_default=True, help="Lookback window in hours.")
@click.option("--queue", "-q", default=None, help="Filter by queue.")
def throughput(app: str, hours: int, queue: str | None) -> None:
    """Chart job throughput over time (requires plotext)."""
    try:
        import plotext as plt
    except ImportError:
        raise click.ClickException("This command requires 'plotext'. Install with: pip install 'wrk[analytics]'")

    from psycopg.sql import SQL

    console = require_rich()
    import rich.box as box

    from rich.text import Text
    from rich.table import Table

    wrk_app = load_app(app)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    async def _run() -> None:
        async with wrk_app:
            pool = wrk_app._pool_or_raise()
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    SQL("""
                        SELECT
                            date_trunc('hour', completed_at) AS bucket,
                            count(*) FILTER (WHERE status = 'complete') AS completed,
                            count(*) FILTER (WHERE status = 'failed') AS failed
                        FROM {jobs}
                        WHERE completed_at IS NOT NULL
                          AND completed_at > %(cutoff)s
                          {where}
                        GROUP BY bucket
                        ORDER BY bucket
                    """).format(
                        jobs=wrk_app._t["jobs"],
                        where=SQL("AND queue = %(queue)s") if queue else SQL(""),
                    ),
                    {"cutoff": cutoff, "queue": queue} if queue else {"cutoff": cutoff},
                )
                rows = await cur.fetchall()

        if not rows:
            console.print("[yellow]No data in the selected time window.[/yellow]")
            return

        labels = [r[0].strftime("%H:%M") for r in rows]
        completed = [int(r[1]) for r in rows]
        failed = [int(r[2]) for r in rows]

        plt.clf()
        plt.theme("dark")
        plt.title(f"Job Throughput — last {hours}h{' [' + queue + ']' if queue else ''}")
        plt.plot(labels, completed, label="completed", color="green")
        if any(f > 0 for f in failed):
            plt.plot(labels, failed, label="failed", color="red")
        plt.xlabel("Hour (UTC)")
        plt.ylabel("Jobs")
        plt.plotsize(min(100, len(labels) * 4 + 20), 20)
        plt.show()

        total_complete = sum(completed)
        total_failed = sum(failed)
        total = total_complete + total_failed
        failure_rate = f"{total_failed / total * 100:.1f}%" if total else "—"
        peak_hour = labels[completed.index(max(completed))] if completed and max(completed) > 0 else "—"
        avg_per_hour = f"{total / len(rows):.1f}" if rows else "—"

        summary = Table(title="Summary", box=box.SIMPLE, header_style="bold dim")
        summary.add_column("Metric")
        summary.add_column("Value", justify="right")
        summary.add_row("Window", f"last {hours}h")
        summary.add_row("Total completed", Text(str(total_complete), style="green"))
        summary.add_row("Total failed", Text(str(total_failed), style="bright_red" if total_failed else "dim"))
        summary.add_row("Failure rate", Text(failure_rate, style="bright_red" if total_failed else "green"))
        summary.add_row("Avg / hour", avg_per_hour)
        summary.add_row("Peak hour", peak_hour)
        console.print(summary)

    asyncio.run(_run())
