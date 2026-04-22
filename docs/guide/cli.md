# CLI

The `werk` command-line tool lets you start workers, inspect queues, and manage jobs without writing Python scripts.

## APP argument

Most commands take an `APP` argument — a `module:attribute` path to a `Werk` instance:

```bash
werkworker myapp.tasks:app
werkinfo myapp.tasks:app
```

The module is imported at runtime, so the `Werk` instance is initialised exactly as it would be in your application.

## Commands

### `werk worker`

Start a worker process.

```bash
werkworker APP [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--queues`, `-q` | `default` | Comma-separated list of queues to consume |
| `--concurrency`, `-c` | `10` | Maximum concurrent jobs |
| `--worker-type`, `-w` | `async` | `async`, `thread`, `process`, or `fork` |
| `--log-level`, `-l` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `--log-format` | `text` | `text` or `json` |
| `--no-color` | off | Disable ANSI colour in log output |

```bash
# Async worker on two queues
werkworker myapp.tasks:app --queues default,high --concurrency 20

# Thread worker with JSON logging
werkworker myapp.tasks:app --worker-type thread --log-format json

# Process worker for CPU-bound work
werkworker myapp.tasks:app --worker-type process --concurrency 4
```

### `werk info`

Print queue statistics, active workers, and server information.

```bash
werkinfo APP
```

### `werk jobs`

List recent jobs in a formatted table.

```bash
werkjobs APP [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--queue`, `-q` | all | Filter by queue name |
| `--status`, `-s` | all | Comma-separated status filter |
| `--limit`, `-n` | `20` | Maximum rows to return |
| `--offset` | `0` | Row offset for pagination |

```bash
# Show failed jobs in the high queue
werkjobs myapp.tasks:app --queue high --status failed

# Paginate
werkjobs myapp.tasks:app --limit 50 --offset 50
```

### `werk stats`

Show queue depth and throughput statistics.

```bash
werkstats APP
```

### `werk throughput`

Display a throughput chart over the last N minutes.

```bash
werkthroughput APP
```

### `werk slowest`

List the slowest jobs by execution duration.

```bash
werkslowest APP
```

### `werk cron`

Show registered cron jobs and their next scheduled run time.

```bash
werkcron APP
```

### `werk purge`

Delete jobs by status, optionally filtered to jobs older than N days.

```bash
werkpurge APP [OPTIONS]
```

| Option | Description |
|---|---|
| `--status` | Comma-separated statuses to purge (e.g. `complete,failed`) |
| `--older-than` | Only delete jobs older than this many days |

```bash
# Delete all completed jobs
werkpurge myapp.tasks:app --status complete

# Delete failed jobs older than 7 days
werkpurge myapp.tasks:app --status failed --older-than 7
```

### `werk dashboard`

Open an interactive terminal dashboard showing live queue and worker metrics (requires the `analytics` optional extra).

```bash
pip install "pgwerk[analytics]"
werkdashboard myapp.tasks:app
```

### `werk api`

Start the REST API server (requires the `litestar` optional extra).

```bash
pip install "pgwerk[api]"
werkapi myapp.tasks:app
```
