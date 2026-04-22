# Schemas

## Job

Represents a job row in the database.

| Field | Type | Description |
|---|---|---|
| `id` | `str` | UUID |
| `function` | `str` | Dotted import path of the handler |
| `queue` | `str` | Queue name |
| `status` | `JobStatus` | Current status |
| `priority` | `int` | Higher values run first |
| `attempts` | `int` | Number of execution attempts so far |
| `max_attempts` | `int` | Maximum allowed attempts |
| `scheduled_at` | `datetime` | When the job becomes eligible to run |
| `enqueued_at` | `datetime` | When the job was inserted |
| `key` | `str \| None` | Idempotency key |
| `group_key` | `str \| None` | Concurrency group |
| `payload` | `dict \| None` | Deserialized handler arguments |
| `result` | `Any` | Return value of the handler (set on completion) |
| `error` | `str \| None` | Traceback string (set on failure) |
| `timeout_secs` | `int \| None` | Job timeout in seconds |
| `heartbeat_secs` | `int \| None` | Heartbeat renewal interval |
| `started_at` | `datetime \| None` | When the current attempt started |
| `completed_at` | `datetime \| None` | When the job reached a terminal state |
| `touched_at` | `datetime \| None` | Last heartbeat timestamp |
| `expires_at` | `datetime \| None` | Row expiry time |
| `worker_id` | `str \| None` | ID of the worker processing the job |
| `meta` | `dict \| None` | Metadata attached at enqueue time |
| `result_ttl` | `int \| None` | Seconds to retain completed rows |
| `failure_ttl` | `int \| None` | Seconds to retain failed rows |
| `ttl` | `int \| None` | Seconds until an unstarted job expires |
| `on_success` | `str \| None` | Dotted path to success callback |
| `on_failure` | `str \| None` | Dotted path to failure callback |
| `on_stopped` | `str \| None` | Dotted path to stopped callback |
| `retry_intervals` | `list[int] \| None` | Per-attempt retry delays |
| `repeat_remaining` | `int \| None` | Remaining repeat iterations |
| `repeat_interval_secs` | `int \| None` | Uniform repeat interval |
| `repeat_intervals` | `list[int] \| None` | Per-run repeat delays |
| `cron_name` | `str \| None` | Cron schedule that created this job |
| `failure_mode` | `str` | `"hold"` or `"delete"` |

---

## JobStatus

```python
class JobStatus(str, enum.Enum):
    Scheduled = "scheduled"   # waiting for scheduled_at
    Queued    = "queued"      # eligible for dequeue
    Waiting   = "waiting"     # blocked on dependencies
    Active    = "active"      # being executed by a worker
    Aborting  = "aborting"    # abort requested, not yet confirmed
    Complete  = "complete"    # finished successfully
    Failed    = "failed"      # retries exhausted
    Aborted   = "aborted"     # cancelled before or during execution
```

Terminal statuses: `Complete`, `Failed`, `Aborted`.

---

## JobExecution

Represents a single execution attempt.

| Field | Type | Description |
|---|---|---|
| `id` | `str` | UUID |
| `job_id` | `str` | Parent job ID |
| `attempt` | `int` | Attempt number (1-based) |
| `status` | `ExecutionStatus` | Outcome of this attempt |
| `worker_id` | `str \| None` | Worker that ran this attempt |
| `error` | `str \| None` | Traceback if failed |
| `result` | `Any` | Return value if successful |
| `started_at` | `datetime \| None` | When this attempt started |
| `completed_at` | `datetime \| None` | When this attempt ended |

---

## ExecutionStatus

```python
class ExecutionStatus(str, enum.Enum):
    Running  = "running"
    Complete = "complete"
    Failed   = "failed"
    Aborted  = "aborted"
```

---

## Retry

Configure maximum attempts and back-off delays.

```python
from wrk import Retry

# 4 total attempts with custom per-attempt delays
retry = Retry(max=4, intervals=[10, 60, 300])

# 3 attempts with a uniform 30-second delay
retry = Retry(max=3, intervals=30)
```

| Field | Type | Description |
|---|---|---|
| `max` | `int` | Total attempts (including the first). Must be ≥ 1 |
| `intervals` | `int \| list[int]` | Delay in seconds between attempts. A single int for uniform delay; a list for per-attempt delays. The last value is reused when the list is shorter than `max - 1` |

---

## Repeat

Re-enqueue a job after each successful run.

```python
from wrk import Repeat

# Run 6 times total (first + 5 repeats), 1 hour apart
repeat = Repeat(times=5, interval=3600)

# Custom per-run delays
repeat = Repeat(times=3, intervals=[60, 300, 3600])
```

| Field | Type | Description |
|---|---|---|
| `times` | `int` | Additional runs after the first. Must be ≥ 1 |
| `interval` | `int` | Uniform delay in seconds between runs (default `0`) |
| `intervals` | `list[int] \| None` | Per-run delays; overrides `interval` |

---

## Dependency

Declare that a job must wait for an upstream job.

```python
from wrk import Dependency

job_a = await app.enqueue(step_one)
await app.enqueue(step_two, _depends_on=Dependency(job_a, allow_failure=True))
```

| Field | Type | Description |
|---|---|---|
| `job` | `str \| Job` | Upstream job ID or Job object |
| `allow_failure` | `bool` | If `True`, the dependent job still runs even if this dependency fails or is aborted |

---

## Callback

A callback function with an optional timeout.

```python
from wrk import Callback

await app.enqueue(my_func, _on_success=Callback(func=notify, timeout=10))
```

| Field | Type | Description |
|---|---|---|
| `func` | `Callable \| str` | The callback function or its dotted import path |
| `timeout` | `int \| None` | Seconds before the callback is cancelled |

---

## Context

Execution context injected into handlers.

| Field | Type | Description |
|---|---|---|
| `app` | `Wrk` | The connected app instance |
| `worker` | `BaseWorker` | The worker processing this job |
| `job` | `Job` | The job being executed |
| `exception` | `Exception \| None` | Set in `after_process` hooks when the job raised |

---

## EnqueueParams

Specification for a single job in `enqueue_many`.

```python
from wrk import EnqueueParams

await app.enqueue_many([
    EnqueueParams(func=process, kwargs={"id": i}, queue="bulk")
    for i in range(100)
])
```

| Field | Default | Description |
|---|---|---|
| `func` | required | Callable to execute |
| `args` | `()` | Positional arguments |
| `kwargs` | `{}` | Keyword arguments |
| `queue` | `"default"` | Queue name |
| `priority` | `0` | Job priority |
| `delay` | `None` | Seconds from now before eligible |
| `at` | `None` | Absolute scheduled time |
| `retry` | `1` | Max attempts or `Retry` object |
| `timeout` | `None` | Job timeout in seconds |
| `heartbeat` | `None` | Heartbeat interval in seconds |
| `key` | `None` | Idempotency key |
| `group` | `None` | Concurrency group |
| `meta` | `None` | Metadata dict |
| `result_ttl` | `None` | Completed-row retention |
| `failure_ttl` | `None` | Failed-row retention |
| `ttl` | `None` | Unstarted-job expiry |
| `on_success` | `None` | Success callback |
| `on_failure` | `None` | Failure callback |
| `on_stopped` | `None` | Stopped callback |
| `repeat` | `None` | Repeat policy |
| `depends_on` | `None` | Upstream dependencies |
| `failure_mode` | `"hold"` | `"hold"` or `"delete"` |

---

## CronJob

A function registered with `CronScheduler`.

| Field | Type | Description |
|---|---|---|
| `func` | `Callable` | Function to enqueue |
| `queue` | `str` | Target queue (default `"default"`) |
| `args` | `tuple` | Positional arguments |
| `kwargs` | `dict` | Keyword arguments |
| `interval` | `int \| None` | Seconds between runs |
| `cron` | `str \| None` | Cron expression |
| `timeout` | `int \| None` | Job timeout in seconds |
| `result_ttl` | `int \| None` | Completed-row retention |
| `failure_ttl` | `int \| None` | Failed-row retention |
| `meta` | `dict \| None` | Metadata attached to every enqueued job |
| `name` | `str` | Unique name (defaults to `module.qualname`) |
| `paused` | `bool` | Whether this job is currently paused |

---

## FailureMode

```python
class FailureMode(str, enum.Enum):
    Hold   = "hold"    # keep the failed job row (default)
    Delete = "delete"  # remove the row on terminal failure
```
