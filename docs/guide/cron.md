# Cron

`CronScheduler` enqueues recurring jobs on a fixed interval or a cron expression. A Postgres advisory lock ensures only one scheduler instance is active at a time — competing instances enter standby and automatically promote if the primary's connection drops.

## Basic usage

```python
from pgwerk import Wrk, AsyncWorker, CronScheduler

app = Wrk("postgresql://user:pass@localhost/mydb")

async def daily_report():
    ...

async def cleanup():
    ...

scheduler = CronScheduler(app)

# Cron expression: every day at 09:00 UTC
scheduler.register(daily_report, cron="0 9 * * *")

# Fixed interval: every 5 minutes
scheduler.register(cleanup, interval=300)

async with app:
    worker = AsyncWorker(app=app)
    await asyncio.gather(worker.run(), scheduler.run())
```

## Cron expressions

Cron expressions require the `croniter` package:

```bash
pip install "wrk[cron]"
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
scheduler.register(my_func, cron="*/15 * * * *")   # every 15 minutes
scheduler.register(my_func, cron="0 0 * * 1")      # every Monday at midnight
scheduler.register(my_func, cron="30 8 1 * *")     # 1st of each month at 08:30
```

## CronJob objects

For more control, construct a `CronJob` directly:

```python
from pgwerk import CronJob

job = CronJob(
    func=daily_report,
    queue="reports",
    cron="0 9 * * *",
    timeout=300,
    result_ttl=86400,
    failure_ttl=604800,
    meta={"source": "scheduler"},
)
scheduler.register(job)
```

`CronJob` parameters:

| Parameter | Description |
|---|---|
| `func` | The callable to enqueue |
| `queue` | Target queue (default `"default"`) |
| `args` / `kwargs` | Arguments forwarded to the callable |
| `interval` | Seconds between runs (mutually exclusive with `cron`) |
| `cron` | Cron expression (mutually exclusive with `interval`) |
| `timeout` | Job timeout in seconds |
| `result_ttl` | Seconds to retain completed job rows |
| `failure_ttl` | Seconds to retain failed job rows |
| `meta` | Metadata dict attached to every enqueued job |
| `name` | Unique scheduler name (defaults to `module.qualname`) |
| `paused` | Start in paused state |

## Pausing and resuming

```python
scheduler.pause("myapp.tasks.cleanup")
scheduler.resume("myapp.tasks.cleanup")
```

Paused jobs remain registered but are not enqueued until resumed.

## Dynamic registration

Jobs can be added and removed while the scheduler is running:

```python
new_job = scheduler.register(new_func, interval=60)
scheduler.unregister("myapp.tasks.old_func")
```

## Introspection

```python
# All registered jobs
for name, cron_job in scheduler.jobs.items():
    print(name, cron_job.next_run_at)

# Single job
job = scheduler.get("myapp.tasks.cleanup")

# Count
print(len(scheduler))

# Membership
print("myapp.tasks.cleanup" in scheduler)
```

## Distributed scheduling

When multiple processes run `CronScheduler.run()` against the same Postgres instance, only one acquires the advisory lock and becomes the primary. The others poll every `cron_standby_retry_interval` seconds (default 30) and promote automatically if the primary's session ends.

This means you can run a `CronScheduler` alongside every worker process without configuring a separate scheduler process — failover is automatic.

## Deduplication

`CronScheduler` uses an idempotency key derived from the job name and the tick timestamp. If a tick fires before the previous enqueue has been consumed, the duplicate is silently dropped.
