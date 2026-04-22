# Workers

## BaseWorker

Abstract base class shared by all worker implementations. Provides the polling loop, dequeue logic, ack/nack handling, heartbeating, LISTEN/NOTIFY integration, sweep, and graceful shutdown.

### Constructor

```python
BaseWorker(
    app: Werk,
    queues: list[str] | None = None,
    concurrency: int = 10,
    heartbeat_interval: int | None = None,
    poll_interval: float | None = None,
    dequeue_strategy: DequeueStrategy = DequeueStrategy.Priority,
    burst: bool = False,
    before_process: list[Callable] | None = None,
    after_process: list[Callable] | None = None,
    sweep_interval: float | None = None,
    abort_interval: float | None = None,
    shutdown_timeout: float | None = None,
)
```

### Methods

#### `run() → None`

Start the worker. Registers the worker in the database, starts side tasks (heartbeat, listen, abort, sweep loops), processes jobs until shutdown, then deregisters.

#### `add_before_process(hook) → None`

Append a hook called before each job starts. Hooks receive a `Context` argument.

#### `add_after_process(hook) → None`

Append a hook called after each job finishes (whether it succeeded or failed).

#### `push_exception_handler(handler) → None`

Push a handler onto the exception stack. Handlers are called in reverse push order when a job raises. Signature: `handler(job: Job, exc: Exception) -> None`.

#### `pop_exception_handler() → Callable`

Remove and return the top exception handler. Raises `IndexError` if the stack is empty.

---

## AsyncWorker

Runs handlers as coroutines on the asyncio event loop. Best for I/O-bound work.

```python
from pgwerk import AsyncWorker

worker = AsyncWorker(app=app, queues=["default", "high"], concurrency=20)
await worker.run()
```

---

## ThreadWorker

Runs each handler in a thread-pool executor. The event loop remains unblocked. Use for blocking code that cannot be made async.

```python
from pgwerk import ThreadWorker

worker = ThreadWorker(app=app, concurrency=8)
await worker.run()
```

---

## ProcessWorker

Runs handlers in a process-pool executor. Provides true CPU parallelism by bypassing the GIL.

```python
from pgwerk import ProcessWorker

worker = ProcessWorker(app=app, concurrency=4)
await worker.run()
```

---

## ForkWorker

Forks a new process for each job. Provides maximum isolation — a crashing job cannot corrupt the worker process. A `SIGTERM` grace period (`sigterm_grace`) is applied before `SIGKILL`.

```python
from pgwerk import ForkWorker

worker = ForkWorker(app=app, concurrency=4)
await worker.run()
```

---

## DequeueStrategy

Controls how queues are ordered when selecting jobs.

| Value | Behaviour |
|---|---|
| `DequeueStrategy.Priority` | Higher-priority jobs run first across all queues |
| `DequeueStrategy.RoundRobin` | Cycle through queues in turn |
| `DequeueStrategy.Random` | Pick a queue at random each poll |

```python
from pgwerk import AsyncWorker, DequeueStrategy

worker = AsyncWorker(
    app=app,
    queues=["a", "b", "c"],
    dequeue_strategy=DequeueStrategy.RoundRobin,
)
```
