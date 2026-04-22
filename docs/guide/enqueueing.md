# Enqueueing

## Basic enqueue

Pass the callable and its arguments:

```python
await app.enqueue(send_email, to="user@example.com", subject="Hello")
```

Positional arguments are also supported:

```python
await app.enqueue(process_image, "/tmp/photo.jpg", 800)
```

`enqueue` returns the inserted `Job` object, or `None` when an idempotency key collision is detected.

## Control options

All control options are keyword arguments prefixed with `_`. Everything else is forwarded to the handler as its payload.

### Queue and priority

```python
await app.enqueue(my_func, x=1, _queue="high", _priority=10)
```

Higher `_priority` values run first within the same queue. The default queue is `"default"` and the default priority is `0`.

### Scheduling

```python
from datetime import datetime, timezone

# Run 30 seconds from now
await app.enqueue(my_func, _delay=30)

# Run at an absolute time
await app.enqueue(my_func, _at=datetime(2025, 6, 1, 9, 0, tzinfo=timezone.utc))
```

### Retries

Pass an integer for a simple max-attempt count, or a `Retry` object for fine-grained control:

```python
from wrk import Retry

# Retry up to 3 times total (including the first attempt)
await app.enqueue(my_func, _retry=3)

# Custom back-off: wait 10s, then 60s, then 300s between retries
await app.enqueue(my_func, _retry=Retry(max=4, intervals=[10, 60, 300]))
```

`Retry.intervals` can also be a single integer for a uniform delay between all retries.

### Timeout

Cancel the job if it does not finish within the given number of seconds:

```python
await app.enqueue(my_func, _timeout=120)
```

### Heartbeat

For long-running jobs, instruct the worker to periodically renew the job's active timestamp so the sweep does not reap it:

```python
await app.enqueue(my_func, _heartbeat=30)  # renew every 30 seconds
```

### Idempotency key

Duplicate enqueues with the same key are silently dropped:

```python
await app.enqueue(send_invoice, invoice_id=42, _key="invoice:42")
```

### Concurrency group

At most one job from the same group can be active at a time:

```python
await app.enqueue(sync_user, user_id=99, _group="user:99")
```

### Metadata

Attach arbitrary data to the job for inspection or callbacks:

```python
await app.enqueue(my_func, _meta={"source": "api", "request_id": "abc"})
```

### TTL and expiry

```python
# Expire an unstarted job after 60 seconds
await app.enqueue(my_func, _ttl=60)

# Keep the completed-job row for 1 hour
await app.enqueue(my_func, _result_ttl=3600)

# Keep the failed-job row for 24 hours
await app.enqueue(my_func, _failure_ttl=86400)
```

### Failure mode

By default, failed jobs are kept in the database (`failure_mode="hold"`) so you can inspect and retry them. Set `failure_mode="delete"` to remove the row on terminal failure:

```python
await app.enqueue(my_func, _failure_mode="delete")
```

## Callbacks

Register functions to be called when a job completes, fails, or is stopped:

```python
from wrk import Callback

async def on_done(ctx):
    print(f"Job {ctx.job.id} finished")

async def on_error(ctx):
    print(f"Job {ctx.job.id} failed: {ctx.job.error}")

await app.enqueue(
    my_func,
    _on_success=on_done,
    _on_failure=on_error,
    _on_stopped=on_error,
)

# With a per-callback timeout
await app.enqueue(my_func, _on_success=Callback(func=on_done, timeout=10))
```

## Repeating jobs

Re-enqueue a job automatically after each successful run:

```python
from wrk import Repeat

# Run 6 times total (first + 5 repeats), waiting 1 hour between each
await app.enqueue(cleanup, _repeat=Repeat(times=5, interval=3600))

# Custom per-run delays
await app.enqueue(cleanup, _repeat=Repeat(times=3, intervals=[60, 300, 3600]))
```

## Dependencies

Jobs can wait for one or more upstream jobs before they become eligible:

```python
from wrk import Dependency

job_a = await app.enqueue(step_one)
job_b = await app.enqueue(step_two, _depends_on=job_a)

# Allow the dependent job to run even if job_a fails
job_b = await app.enqueue(step_two, _depends_on=Dependency(job_a, allow_failure=True))

# Multiple dependencies
job_c = await app.enqueue(step_three, _depends_on=[job_a, job_b])
```

The dependent job enters `waiting` status and is promoted to `queued` once all its dependencies reach a terminal state.

## Bulk enqueue

Insert multiple jobs in a single round-trip:

```python
from wrk import EnqueueParams

await app.enqueue_many([
    EnqueueParams(func=process, kwargs={"item_id": i}, queue="bulk")
    for i in range(1000)
])
```

`enqueue_many` returns a list of `Job | None` in the same order as the input specs.

## Transactional enqueue

Enqueue a job inside an existing database transaction by passing `_conn`:

```python
async with pool.connection() as conn:
    await conn.execute("INSERT INTO orders ...", ...)
    await app.enqueue(fulfill_order, order_id=123, _conn=conn)
    # both the INSERT and the enqueue commit or roll back together
```

## Wait for a result

`apply` enqueues a job and blocks until it finishes, returning its result:

```python
result = await app.apply(greet, name="world", timeout=30)
# result == "Hello, world!"
```

`map` does the same for multiple inputs in parallel:

```python
results = await app.map(
    process_item,
    [{"item_id": 1}, {"item_id": 2}, {"item_id": 3}],
    timeout=60,
)
```
