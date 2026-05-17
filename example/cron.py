"""
Runs a Scheduler with a handful of example schedules.

    cd example
    python cron.py

The scheduler registers four schedules and runs until Ctrl+C.
Jobs are picked up by the normal worker (worker.py).
"""

import os
import asyncio
import logging

from pgwerk import Werk
from pgwerk import Scheduler
from pgwerk import configure_logging

from example.tasks import heartbeat_ping
from example.tasks import flush_metrics
from example.tasks import cleanup_expired_sessions
from example.tasks import daily_report


configure_logging(logger_name="")

logger = logging.getLogger(__name__)

app = Werk(os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk"))
scheduler = Scheduler(app, on_unregistered="pause")

# Runs every 30 seconds — handy for watching the dashboard update quickly.
scheduler.register(heartbeat_ping, interval=30, _queue="default")

# Runs every 60 seconds — simulates a metrics flush.
scheduler.register(flush_metrics, interval=60, _queue="default")

# Runs every 5 minutes — simulates a stale-session cleanup.
scheduler.register(cleanup_expired_sessions, interval=300, _queue="default", kwargs={"older_than_days": 30})

# Runs on a cron expression — every day at 08:00 UTC.
scheduler.register(daily_report, cron="0 8 * * *", _queue="default", _timeout=300)


async def main() -> None:
    logger.info(
        "Scheduler starting — %d schedules registered. Press Ctrl+C to stop.",
        len(scheduler._pending),
    )
    async with app:
        await scheduler.run()


asyncio.run(main())
