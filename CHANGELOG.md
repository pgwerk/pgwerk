# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
