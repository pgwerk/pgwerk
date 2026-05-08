# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- `CronScheduler` now registers itself in `_pgwerk_worker` with `role = 'scheduler'`, runs a heartbeat loop, and deregisters on shutdown — giving the same observability as job workers
- `role` column on `_pgwerk_worker` (`'worker'` or `'scheduler'`, default `'worker'`) to distinguish job workers from scheduler instances; existing databases are migrated automatically (schema version 5)

## [0.1.9] - 2026-05-07

### Added

- `werk migrate` CLI command — runs schema migrations standalone with a dedicated DSN, without starting a worker or API server
- `auto_migrate` flag on `Werk` (default `True`) — set to `False` to skip auto-migration at connect time when running migrations separately

### Changed

- Default schema changed from `public` (search_path) to `pgwerk` — tables are now isolated in a dedicated schema out of the box
- Production security recommendation added to the README: create a Postgres role scoped to the `pgwerk` schema to prevent cross-schema access

## [0.1.8] - 2026-05-07

### Added

- `werk api` now accepts `--api-token` and `--auth` flags to enable token-based authentication on the REST API

### Changed

- Added `mypy` config with strict type checking
- REST API module split into focused files (`deps.py`, `handlers.py`, `routes.py`, `spa.py`, `exporter.py`) instead of a single monolithic `app.py`
- Config management centralized through `pgwerk/config.py` — workers, CLI commands, and the API all read from one place
- Database repository layer now uses a protocol-based driver abstraction (`AsyncConnection` / `AsyncCursor` in `pgwerk/connection.py`), decoupling it from psycopg specifics

## [0.1.7] - 2026-05-07

### Added

- `sync=True` on `enqueue` / `enqueue_many` — jobs can now be executed synchronously in the calling thread without spinning up a worker

### Removed

- `psycopg_pool` dependency dropped — the library manages a plain single connection, removing a heavy transitive dependency and simplifying the connection lifecycle

## [0.1.6] - 2026-05-07

### Changed

- DDL column ordering: timestamp columns (`enqueued_at`, `started_at`) are now the first column in each table, making the schema easier to read

## [0.1.5] - 2026-05-07

### Fixed

- Dashboard is now correctly bundled in the PyPI wheel — `werk api` serves the UI as expected after `pip install`

## [0.1.4] - 2026-05-07

### Fixed

- Dashboard static files are now correctly bundled into the PyPI wheel — `werk api` serves the UI as expected after a plain `pip install`

## [0.1.3] - 2026-05-05

### Fixed

- `import_fn` no longer swallows `ModuleNotFoundError` raised by a dependency of the target module — the real error is re-raised immediately instead of being replaced with a generic "Couldn't import" message

## [0.1.2] - 2026-05-05

### Fixed

- `JSONSerializer` now handles `UUID`, `datetime`, `date`, `time`, `Decimal`, and `Enum` without raising `TypeError` — values are coerced to their string/primitive equivalents (same behaviour as Celery/kombu)

## [0.1.0] - 2026-04-22

### Added

- `Werk` app class — connect/disconnect, enqueue, enqueue_many, get_job, get_executions, cancel_job, sweep
- `AsyncWorker`, `ThreadWorker`, `ProcessWorker`, `ForkWorker` — four concurrency models
- `LISTEN/NOTIFY` wake-up so workers react instantly to new jobs
- `SELECT … FOR UPDATE SKIP LOCKED` dequeue with Priority, RoundRobin, and Random strategies
- `Retry` — configurable max attempts and per-interval back-off delays
- `Repeat` — re-enqueue a job N more times after each successful run
- `Dependency` / DAG — jobs that wait for one or more upstream jobs to complete
- Idempotency keys (`_key`) — duplicate enqueues silently dropped
- Group keys (`_group`) — at most one active job per group at a time
- `_heartbeat` — worker auto-renews long-running jobs to prevent sweep reaping
- `CronScheduler` + `CronJob` — cron-expression and interval-based recurring jobs, with Postgres advisory lock so only one scheduler runs at a time
- `JSONSerializer` (default) and `PickleSerializer`
- Auto-migration on `connect()` with advisory lock to prevent races
- `before_process` / `after_process` hooks on workers
- `on_success` / `on_failure` / `on_stopped` callbacks per job
- `failure_mode=delete` — terminal failures remove the row instead of marking it failed
- `result_ttl` / `failure_ttl` — automatic expiry of completed / failed rows
- `burst` mode — worker exits once the queue drains
- `BaseWorker.push_exception_handler` / `pop_exception_handler` stack
- `werk` CLI — `worker`, `info`, `purge` sub-commands
- REST API (optional `litestar` extra) — job inspection and queue stats
- Prometheus metrics exporter (optional `prometheus-client` extra)
- `werk info` dashboard using `rich` + `plotext` (optional `analytics` extra)
