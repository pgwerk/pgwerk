---
title: Scheduler — pgwerk Postgres job queue
description: Schedule recurring background jobs in Python using pgwerk's Scheduler. Supports cron expressions and fixed intervals with distributed, lock-free scheduling.
keywords: python cron job postgres, recurring background jobs python, scheduled tasks python, pgwerk scheduler
---

# Scheduler

`Scheduler` enqueues recurring jobs on a fixed interval or a cron expression. Schedule definitions are persisted in `_pgwerk_schedules` — multiple scheduler processes share load via `SELECT … FOR UPDATE SKIP LOCKED` with no primary/standby coordination required.

## Basic usage

```python
import asyncio
from pgwerk import Werk, AsyncWorker, Scheduler

app = Werk("postgresql://user:pass@localhost/mydb")
scheduler = Scheduler(app)

@scheduler.register(cron="0 9 * * *", _queue="reports")
async def daily_report():
    ...

@scheduler.register(interval=300)
async def cleanup():
    ...

async def main():
    async with app:
        worker = AsyncWorker(app=app)
        await asyncio.gather(worker.run(), scheduler.run())

asyncio.run(main())
```

`register()` stages schedules in-process. They are upserted into the database when `run()` (or `sync()`) is called.

## Cron expressions

Cron expressions require the `croniter` package:

```bash
pip install "pgwerk[cron]"
# or
pip install croniter
```

Standard five-field cron syntax is supported:

```
┌───────── minute (0-59)
│ ┌─────── hour (0-23)
│ │ ┌───── day of month (1-31)
│ │ │ ┌─── month (1-12)
│ │ │ │ ┌─ day of week (0-6, Sunday=0)
│ │ │ │ │
* * * * *
```

```python
@scheduler.register(cron="*/15 * * * *")   # every 15 minutes
async def poll():
    ...

@scheduler.register(cron="0 0 * * 1")      # every Monday at midnight
async def weekly_digest():
    ...
```

## `register()` options

All options are `_`-prefixed to avoid collisions with function kwargs passed via `args`/`kwargs`:

| Option | Default | Description |
|---|---|---|
| `cron` | — | Cron expression (mutually exclusive with `interval`) |
| `interval` | — | Seconds between runs (mutually exclusive with `cron`) |
| `_queue` | `"default"` | Target queue for enqueued jobs |
| `_name` | `module.qualname` | Unique schedule name |
| `args` | `[]` | Positional arguments forwarded to the handler |
| `kwargs` | `{}` | Keyword arguments forwarded to the handler |
| `_timeout` | `None` | Job timeout in seconds |
| `_result_ttl` | `None` | Seconds to retain completed job rows |
| `_failure_ttl` | `None` | Seconds to retain failed job rows |
| `_meta` | `None` | Metadata dict attached to every enqueued job |

## Imperative scheduling

For dynamic registration after startup, use the async methods. These write to the database immediately and are safe to call while the scheduler is running.

```python
# By cron expression
await scheduler.cron("0 9 * * *", send_report, _queue="reports")
await scheduler.cron("*/5 * * * *", poll_feed, feed_id=42, _timeout=30)

# By interval
await scheduler.interval(3600, sync_data, _queue="etl")
await scheduler.interval(300, cleanup, older_than_days=7, _timeout=60)

# Full control — upserts and recomputes next_run_at from the policy
await scheduler.schedule(send_report, cron="0 10 * * *")

# Start the first run at a specific time
from datetime import datetime, timezone
await scheduler.schedule_at(send_report, datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc), interval=86400)

# Start the first run after a delay
await scheduler.schedule_in(3600, send_report, interval=86400)
```

## Reconciliation on startup

The `on_unregistered` parameter controls what happens to schedules that exist in the database but were not registered by the current process at startup:

```python
# "pause" (default) — safe for rolling deploys; pauses orphaned schedules
scheduler = Scheduler(app, on_unregistered="pause")

# "keep" — DB is the source of truth; code has no effect on unregistered schedules
scheduler = Scheduler(app, on_unregistered="keep")

# "delete" — removes schedules not registered by this process (use with care)
scheduler = Scheduler(app, on_unregistered="delete")
```

## Managing schedules

```python
# Update a schedule in place
await scheduler.update("myapp.tasks.cleanup", interval=600, _timeout=120)

# Pause / resume
await scheduler.pause("myapp.tasks.cleanup")
await scheduler.resume("myapp.tasks.cleanup")

# Force a schedule due immediately (fires on next tick)
await scheduler.trigger("myapp.tasks.cleanup")

# Delete a schedule row
await scheduler.delete("myapp.tasks.cleanup")
```

## Introspection

```python
# All schedules
schedules = await scheduler.list_schedules()
for s in schedules:
    print(s.name, s.next_run_at, s.paused)

# Single schedule
s = await scheduler.get("myapp.tasks.cleanup")
```

## Manual sync

Push staged registrations without calling `run()`:

```python
inserted, updated, reconciled = await scheduler.sync()
```

## Distributed scheduling

Multiple processes can run `Scheduler.run()` against the same Postgres instance simultaneously. Each picks up due schedules via `SELECT … FOR UPDATE SKIP LOCKED`, so work is distributed naturally without any primary election or advisory lock.

## Deduplication

Each enqueue uses an idempotency key derived from the schedule name and tick timestamp. If a schedule fires before its previous job has been consumed, the duplicate is silently dropped.
