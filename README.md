# PGWerk

[![CI](https://github.com/pgwerk/pgwerk/actions/workflows/ci.yml/badge.svg)](https://github.com/pgwerk/pgwerk/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgwerk?v=2)](https://pypi.org/project/pgwerk/)
[![Python](https://img.shields.io/pypi/pyversions/pgwerk?v=2)](https://pypi.org/project/pgwerk/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-pgwerk.github.io/pgwerk-blue)](https://pgwerk.github.io/pgwerk)

A Postgres-backed job queue. Durable, visible, transactional.

Jobs are rows. Workers poll with `SELECT … FOR UPDATE SKIP LOCKED`. No external broker, no sidecar, just your existing Postgres instance. The schema is created automatically on first connect.

![PGWerk dashboard overview](docs/assets/01-ss-home.png)

---

## Python

### Install

```bash
pip install pgwerk

# Cron support (optional)
pip install "pgwerk[cron]"
```

Requires Python 3.11+ and a Postgres 14+ database.

### Quickstart

Define your app and handlers:

```python
from pgwerk import Werk, Context

app = Werk("postgresql://user:pass@localhost/mydb")

# Call connect() once at startup; disconnect() at shutdown.
# async with app: is shorthand for the same pair.
await app.connect()

async def send_email(to: str):
    ...  # plain handler — ctx is optional

async def send_email_with_context(ctx: Context, to: str):
    ...  # opt in by naming/typing the first parameter as ctx
```

Enqueue jobs from anywhere in your application:

```python
await app.enqueue(send_email, to="user@example.com")
```

Run a worker in a separate process:

```python
import asyncio
from pgwerk import AsyncWorker

async def main():
    worker = AsyncWorker(app=app, queues=["default"], concurrency=10)
    await worker.run()

asyncio.run(main())
```

Handlers are identified by their dotted import path (`myapp.tasks.send_email`). The function itself is passed to `enqueue`; `werk` records its path and imports it on the worker side. `ctx` is injected when the first parameter is named `ctx` or annotated as `Context`.

### Enqueueing

```python
from pgwerk import Retry, Repeat, Dependency
from datetime import datetime, timezone, timedelta

# Basic
await app.enqueue(my_func, x=1)

# With options
await app.enqueue(
    my_func,
    x=1,
    _queue="high",
    _priority=10,
    _delay=30,                         # seconds from now
    _at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    _retry=Retry(max=3, intervals=[10, 60, 300]),  # total attempts, including the first
    _timeout=120,                      # fail after 2 minutes
    _heartbeat=30,                     # worker auto-renews while the job is running
    _key="unique-key",                 # idempotency key — duplicate enqueues are silently dropped
    _group="user:42",                  # at most one active job per group
    _meta={"source": "api"},
    _on_success=notify_done,
    _on_failure=alert_team,
)

# Repeat a job N more times after the first run
await app.enqueue(cleanup, _repeat=Repeat(times=5, interval=3600))

# Depend on another job finishing first
job_a = await app.enqueue(step_one)
await app.enqueue(step_two, _depends_on=job_a)          # waits for job_a
await app.enqueue(step_two, _depends_on=Dependency(job_a, allow_failure=True))

# Bulk enqueue in one round-trip
from pgwerk import EnqueueParams
await app.enqueue_many([
    EnqueueParams(func=my_func, kwargs={"n": i}, queue="bulk") for i in range(100)
])
```

### Workers

```python
from pgwerk import AsyncWorker, ThreadWorker, ProcessWorker, ForkWorker

# Asyncio (default — best for I/O-bound work)
worker = AsyncWorker(app=app, queues=["default", "high"], concurrency=20)

# Thread pool (CPU-light work, blocking libraries)
worker = ThreadWorker(app=app, concurrency=8)

# Process pool (CPU-bound work, true parallelism)
worker = ProcessWorker(app=app, concurrency=4)

# Fork per job (maximum isolation)
worker = ForkWorker(app=app, concurrency=4)

await worker.run()
```

Workers register themselves in the database, send periodic heartbeats, auto-touch jobs that opt into `_heartbeat`, and use `LISTEN/NOTIFY` for instant wake-up when jobs are enqueued.

### Cron

```python
from pgwerk import CronScheduler, CronJob

scheduler = CronScheduler(app)
scheduler.register(CronJob(func=my_func, cron="*/15 * * * *"))  # every 15 min
scheduler.register(CronJob(func=other_func, interval=3600))      # every hour

async with app:
    await scheduler.run()
```

`CronScheduler` uses a Postgres advisory lock so only one process runs the scheduler at a time. Requires `croniter` for cron expressions.

### Serializers

```python
from pgwerk import Werk, PickleSerializer

app = Werk(dsn, serializer=PickleSerializer())  # default is JSONSerializer
```

The configured serializer is used for job payloads, job results, and execution results.

### Job inspection

```python
job = await app.get_job(job_id)
executions = await app.get_executions(job_id)
await app.cancel_job(job_id)
```

### CLI

```bash
# Start a worker
werk worker myapp.tasks:app --queues default,high --concurrency 10 --worker-type async

# Show queue statistics and active workers
werk info myapp.tasks:app

# Delete finished jobs
werk purge myapp.tasks:app --status complete,failed
```

`APP` is a `module:attribute` path to a `Werk` instance.

---

## Schema

All tables are prefixed (default `_pgwerk_`) and optionally placed in a named schema. Migrations run automatically on connect using an advisory lock to prevent races across multiple processes starting simultaneously.

| Table | Purpose |
|---|---|
| `_pgwerk_worker` | Registered workers + heartbeat tracking |
| `_pgwerk_jobs` | Job queue — all state lives here |
| `_pgwerk_worker_jobs` | Active claim tracking |
| `_pgwerk_jobs_executions` | Per-attempt execution history |
| `_pgwerk_job_deps` | Job dependency graph |


### Job lifecycle

```
queued → active → complete
                ↘ failed   (retries exhausted)
       → waiting           (blocked on dependencies, Python only)
       → aborted           (cancelled before execution)
```

---

## License

MIT
