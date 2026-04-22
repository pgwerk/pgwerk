# Quickstart

## Install

```bash
pip install wrk
```

Requires Python 3.11+ and Postgres 14+. For cron expression support, add the optional extra:

```bash
pip install "wrk[cron]"
```

## Connect

Create a `Wrk` instance with your Postgres DSN. Call `connect()` once at startup and `disconnect()` at shutdown. The async context manager is shorthand for the same pair:

```python
from wrk import Wrk

app = Wrk("postgresql://user:pass@localhost/mydb")

# Explicit lifecycle
await app.connect()
# ... your app ...
await app.disconnect()

# Context manager (recommended)
async with app:
    ...
```

`connect()` is idempotent — calling it multiple times is safe. On first connect it runs schema migrations using a Postgres advisory lock, so multiple processes starting simultaneously will not race.

## Define handlers

Handlers are plain async (or sync) functions. `wrk` records their dotted import path and imports them on the worker side when a job runs.

```python
async def send_email(to: str, subject: str) -> None:
    ...

async def resize_image(path: str, width: int) -> str:
    ...
```

### Execution context

Handlers can optionally receive an execution context as their first argument. `wrk` injects it automatically when the first parameter is named `ctx` or annotated as `Context`:

```python
from wrk import Context

async def send_email(ctx: Context, to: str) -> None:
    print(f"Job {ctx.job.id} on worker {ctx.worker.name}")
    ...
```

`Context` carries the connected `app`, the `worker`, the `job` being executed, and any `exception` raised (available in `after_process` hooks).

## Enqueue a job

Call `enqueue` from anywhere — web handlers, background tasks, other jobs:

```python
await app.enqueue(send_email, to="user@example.com", subject="Hello")
```

`enqueue` returns the inserted `Job` object, or `None` when an idempotency key collision is detected.

## Run a worker

Workers dequeue and execute jobs. Run one in a separate process or alongside your application:

```python
import asyncio
from wrk import AsyncWorker

async def main():
    worker = AsyncWorker(app=app, queues=["default"], concurrency=10)
    await worker.run()

asyncio.run(main())
```

Or start one from the CLI:

```bash
wrk worker myapp.tasks:app --queues default --concurrency 10
```

`APP` is a `module:attribute` path to your `Wrk` instance.

## Minimal end-to-end example

```python
import asyncio
from wrk import Wrk, AsyncWorker

app = Wrk("postgresql://user:pass@localhost/mydb")

async def greet(name: str) -> str:
    return f"Hello, {name}!"

async def main():
    async with app:
        await app.enqueue(greet, name="world")
        worker = AsyncWorker(app=app, queues=["default"], concurrency=5)
        await worker.run()

asyncio.run(main())
```
