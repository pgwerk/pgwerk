# Workers

Workers dequeue jobs from Postgres and execute them. `werk` ships with four worker types, all sharing the same base behaviour.

## Worker types

### AsyncWorker

The default. Runs handlers as coroutines on a single asyncio event loop. Best for I/O-bound work (HTTP calls, database queries, file I/O).

```python
from pgwerk import AsyncWorker

worker = AsyncWorker(app=app, queues=["default"], concurrency=20)
await worker.run()
```

### ThreadWorker

Runs each handler in a thread-pool executor. Suitable for blocking libraries that are not async-aware.

```python
from pgwerk import ThreadWorker

worker = ThreadWorker(app=app, concurrency=8)
await worker.run()
```

### ProcessWorker

Runs handlers in a process-pool executor. Use this for CPU-bound work that benefits from true parallelism (bypasses the GIL).

```python
from pgwerk import ProcessWorker

worker = ProcessWorker(app=app, concurrency=4)
await worker.run()
```

### ForkWorker

Forks a new process for each individual job. Provides maximum isolation — a crashing job cannot affect the worker process. A `SIGTERM` grace period allows the child process to finish before `SIGKILL` is sent.

```python
from pgwerk import ForkWorker

worker = ForkWorker(app=app, concurrency=4)
await worker.run()
```

## Configuration

All workers share these constructor parameters:

| Parameter | Default | Description |
|---|---|---|
| `app` | required | Connected `Werk` instance |
| `queues` | `["default"]` | Queue names to consume |
| `concurrency` | `10` | Max jobs processed simultaneously |
| `heartbeat_interval` | `10` | Seconds between worker heartbeats |
| `poll_interval` | `5.0` | Seconds between database polls when idle |
| `dequeue_strategy` | `Priority` | How queues are ordered when dequeuing |
| `burst` | `False` | Exit once the queue is empty |
| `before_process` | `[]` | Hooks called before each job starts |
| `after_process` | `[]` | Hooks called after each job finishes |
| `sweep_interval` | `60.0` | Seconds between maintenance sweeps |
| `abort_interval` | `1.0` | Seconds between abort-signal checks |
| `shutdown_timeout` | `30.0` | Seconds to wait for in-flight jobs during shutdown |

## Dequeue strategies

Control how the worker picks jobs from multiple queues:

```python
from pgwerk import AsyncWorker, DequeueStrategy

# Priority (default) — higher-priority jobs run first across all queues
worker = AsyncWorker(app=app, queues=["high", "low"], dequeue_strategy=DequeueStrategy.Priority)

# Round-robin — cycle through queues evenly
worker = AsyncWorker(app=app, queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.RoundRobin)

# Random — pick a queue at random each poll
worker = AsyncWorker(app=app, queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.Random)
```

## Burst mode

The worker exits as soon as the queue drains instead of waiting for new work:

```python
worker = AsyncWorker(app=app, burst=True)
await worker.run()
# returns when the queue is empty
```

## Lifecycle hooks

Run code before or after every job:

```python
from pgwerk import Context

async def log_start(ctx: Context) -> None:
    print(f"Starting {ctx.job.function} [{ctx.job.id}]")

async def log_end(ctx: Context) -> None:
    if ctx.exception:
        print(f"Failed: {ctx.exception}")
    else:
        print(f"Completed [{ctx.job.id}]")

worker = AsyncWorker(
    app=app,
    before_process=[log_start],
    after_process=[log_end],
)

# Or add them later
worker.add_before_process(log_start)
worker.add_after_process(log_end)
```

Hooks receive a `Context` object. In `after_process` hooks, `ctx.exception` is set if the job raised.

## Exception handlers

Push handlers onto a stack to intercept job failures:

```python
async def report_error(job, exc):
    sentry_sdk.capture_exception(exc)

worker.push_exception_handler(report_error)
```

Handlers are called in reverse push order (most recently pushed first). Use `pop_exception_handler()` to remove the top handler.

## Signals and shutdown

Workers install handlers for `SIGTERM` and `SIGINT`. On receipt, the worker stops accepting new jobs and waits up to `shutdown_timeout` seconds for in-flight jobs to finish before cancelling them.

## Running alongside a web server

Workers can run in the same process as an ASGI/WSGI application using background tasks or a separate asyncio task:

```python
import asyncio
from pgwerk import AsyncWorker

async def main():
    async with app:
        worker = AsyncWorker(app=app)
        await asyncio.gather(
            run_web_server(),
            worker.run(),
        )
```
