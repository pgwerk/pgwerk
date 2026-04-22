# App

## Werk

The central object. Holds the connection pool and exposes all job management operations.

### Constructor

```python
Werk(
    dsn: str,
    *,
    config: WerkConfig | dict | None = None,
    schema: str | None = None,
    prefix: str | None = None,
    min_pool_size: int | None = None,
    max_pool_size: int | None = None,
    serializer: Serializer | None = None,
    max_active_secs: int | None = None,
    log_level: int | str | None = None,
    log_format: str | None = None,
    log_color: bool | None = None,
    log_fmt: str | None = None,
)
```

| Parameter | Default | Description |
|---|---|---|
| `dsn` | required | Postgres connection string |
| `config` | `WerkConfig()` | Base configuration; keyword arguments take precedence |
| `schema` | `None` | Postgres schema to qualify all table names |
| `prefix` | `"_pgwerk"` | Prefix applied to every table name |
| `min_pool_size` | `2` | Minimum pooled connections |
| `max_pool_size` | `10` | Maximum pooled connections |
| `serializer` | `JSONSerializer` | Payload/result serializer |
| `max_active_secs` | `3600` | Seconds before an active job is considered stuck by `sweep` |
| `log_level` | `None` | Logging level passed to `configure_logging` |
| `log_format` | `None` | `"text"` or `"json"` |
| `log_color` | `None` | Enable/disable ANSI colour in text logs |
| `log_fmt` | `None` | Custom log format string |

### Lifecycle

#### `connect() → None`

Open the connection pool and run schema migrations. Idempotent — safe to call multiple times. Runs registered `on_startup` hooks after the pool is open.

#### `disconnect() → None`

Run `on_shutdown` hooks and close the connection pool. Idempotent.

#### `async with app`

Calls `connect()` on entry and `disconnect()` on exit.

#### `on_startup(func) → func`

Register a callable to run after the pool opens. Can be used as a decorator.

```python
@app.on_startup
async def init_cache():
    ...
```

#### `on_shutdown(func) → func`

Register a callable to run before the pool closes.

### Enqueueing

#### `enqueue(func, *args, **kwargs) → Job | None`

Enqueue a job. Control options are keyword arguments prefixed with `_`; all other keyword arguments are forwarded to the handler as payload.

Returns the inserted `Job`, or `None` if an idempotency key collision was detected.

| Option | Type | Description |
|---|---|---|
| `_queue` | `str` | Queue name (default `"default"`) |
| `_priority` | `int` | Higher values run first (default `0`) |
| `_delay` | `int` | Seconds from now before the job becomes eligible |
| `_at` | `datetime` | Absolute UTC datetime for scheduled jobs |
| `_retry` | `Retry \| int` | Max attempts or a `Retry` object with back-off |
| `_timeout` | `int` | Seconds before the job is timed out |
| `_heartbeat` | `int` | Heartbeat renewal interval for long-running jobs |
| `_key` | `str` | Idempotency key — duplicate keys are silently dropped |
| `_group` | `str` | Concurrency group name |
| `_conn` | `AsyncConnection` | Existing psycopg connection for transactional enqueue |
| `_meta` | `dict` | Arbitrary metadata stored with the job |
| `_result_ttl` | `int` | Seconds to retain completed job rows |
| `_failure_ttl` | `int` | Seconds to retain failed job rows |
| `_ttl` | `int` | Seconds until an unstarted job expires |
| `_on_success` | `Callback \| Callable \| str` | Callback on successful completion |
| `_on_failure` | `Callback \| Callable \| str` | Callback on failure |
| `_on_stopped` | `Callback \| Callable \| str` | Callback when cancelled/stopped |
| `_repeat` | `Repeat` | Repeat policy for recurring jobs |
| `_depends_on` | `Dependency \| Job \| list` | Upstream jobs that must complete first |
| `_failure_mode` | `str` | `"hold"` (default) or `"delete"` |

#### `enqueue_many(specs, *, _conn=None) → list[Job | None]`

Enqueue multiple jobs in a single batch insert. `specs` is a list of `EnqueueParams` objects. Returns results in the same order; an entry is `None` for duplicate-key collisions.

#### `apply(func, *args, timeout=None, poll_interval=0.5, **enqueue_kwargs) → Any`

Enqueue a job and block until its result is available. Raises `JobError` if the job fails or is aborted.

#### `map(func, iter_kwargs, *, timeout=None, poll_interval=0.5, return_exceptions=False, **shared_enqueue_kwargs) → list[Any]`

Enqueue `func` for each dict in `iter_kwargs` and collect all results. With `return_exceptions=True`, failed jobs produce a `JobError` in the results list instead of raising.

#### `wait_for(job_id, *, timeout=None, poll_interval=2.0) → Job`

Block until a job reaches a terminal state. Uses `LISTEN/NOTIFY` for instant wake-up with polling as fallback.

### Job management

#### `get_job(job_id) → Job`

Fetch a single job by ID. Raises `JobNotFound` if missing.

#### `list_jobs(queue, status, worker_id, search, limit, offset) → list[Job]`

List jobs with optional filters.

#### `cancel_job(job_id) → bool`

Cancel a queued or scheduled job. Returns `True` if found and cancelled.

#### `requeue_job(job_id) → bool`

Reset a completed or failed job back to `queued`.

#### `abort_job(job_id) → bool`

Request an active job to stop and mark it `aborted`.

#### `delete_job(job_id) → None`

Permanently delete a job row.

#### `touch_job(job_id) → None`

Extend the heartbeat deadline for a long-running job.

#### `get_executions(job_id) → list[JobExecution]`

Fetch the per-attempt execution history for a job.

#### `get_job_dependencies(job_id) → list[str]`

Return the IDs of jobs that must complete before the given job can run.

#### `sweep() → list[str]`

Detect stuck active jobs (no heartbeat within `max_active_secs`) and requeue them. Returns the list of requeued job IDs.

#### `bulk_requeue_jobs(queue, function_name) → int`

Requeue multiple jobs at once, optionally filtered by queue or function name.

#### `bulk_cancel_jobs(queue) → int`

Cancel multiple queued jobs at once.

#### `purge_jobs(statuses, older_than_days) → int`

Delete job rows by status and age.

### Maintenance

#### `vacuum() → None`

Run `VACUUM ANALYZE` on all werk tables.

#### `truncate() → None`

Truncate all werk tables. Useful in tests.

### Lifecycle hooks

#### `register_before_enqueue(callback) → None`

Register a callback invoked with each `Job` just before it is inserted.

#### `unregister_before_enqueue(callback) → None`

Remove a previously registered before-enqueue callback.

---

## WerkConfig

Centralised configuration. Keyword arguments to `Werk()` take precedence over values in a `WerkConfig` instance.

```python
from pgwerk import Werk, WerkConfig

config = WerkConfig(
    prefix="_jobs",
    max_pool_size=20,
    sweep_interval=30.0,
)
app = Werk(dsn, config=config)
```

| Attribute | Default | Description |
|---|---|---|
| `schema` | `None` | Postgres schema for table qualification |
| `prefix` | `"_pgwerk"` | Table name prefix |
| `min_pool_size` | `2` | Min connections in pool |
| `max_pool_size` | `10` | Max connections in pool |
| `max_active_secs` | `3600` | Stuck-job threshold for sweep |
| `heartbeat_interval` | `10` | Worker heartbeat cadence (seconds) |
| `poll_interval` | `5.0` | Idle poll cadence (seconds) |
| `abort_interval` | `1.0` | Abort-signal check cadence (seconds) |
| `sweep_interval` | `60.0` | Maintenance sweep cadence (seconds) |
| `shutdown_timeout` | `30.0` | Graceful shutdown timeout (seconds) |
| `sigterm_grace` | `5` | ForkWorker SIGTERM→SIGKILL grace (seconds) |
| `cron_standby_retry_interval` | `30.0` | Standby scheduler retry cadence (seconds) |
| `ephemeral_tables` | `False` | Use UNLOGGED tables for worker/worker_jobs |
