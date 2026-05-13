# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.15] - 2026-05-13

### Added

- Dashboard `/jobs` screen: queue filter is now a multi-select checkbox dropdown, so individual queues can be toggled in and out of the list view (empty selection means "all queues")
- `GET /api/jobs` `queue` query param accepts a comma-separated list of queue names; the repo-level `list_jobs(queue=...)` now also takes `Sequence[str]` and translates it to a `queue = ANY(...)` filter

---

## [0.1.14] - 2026-05-11

### Fixed

- `WerkExporter.collect()` crashed on every cycle with `UndefinedColumn: column "last_heartbeat" does not exist` — the worker-count query now correctly references `heartbeat_at`
- Grafana dashboard: "Workers Online" stat always showed 0 because the query filtered on `expires_at > NOW()`, but `expires_at` is only set when a worker stops (active workers have `NULL`). Query now uses `heartbeat_at > NOW() - INTERVAL '60 seconds'`
- Grafana dashboard: "Success Rate" returned `No data` instead of 100 % when no failed jobs existed in the time window — wrapped in `COALESCE` to default to 100
- Grafana dashboard: "Jobs by Queue and Status" and "Top Functions by Failure Count" bar charts rendered with the wrong axis — added explicit `xField` so Grafana treats `queue` / `function` as the category axis

### Added

- `WerkConfig.from_env()` — reconstruct a full config from `PGWERK_*` environment variables (covers all fields)
- `WerkConfig.to_env()` — write the current config back to `PGWERK_*` environment variables; used by the `werk api` CLI so uvicorn's factory-reload mode picks up the correct config
- Integration test `tests/integration/test_exporter.py` — covers `WerkExporter.collect()` against a real Postgres instance, catching SQL column-name regressions that mypy cannot see

### Changed

- `werk api` now always passes `"pgwerk.api.app:create_app"` as a factory string to uvicorn (with `factory=True`), calling `config.to_env()` first so the factory reconstructs the identical config on every reload

---

## [0.1.13] - 2026-05-11

### Changed

- Docker image now includes the `exporter` extra (`prometheus-client`) alongside `api`

---

## [0.1.12] - 2026-05-08

### Added

- `WerkConfig.min_compatible_db_version` and `max_compatible_db_version` class constants — declare the schema-version range this build can safely talk to. `connect()` now validates the live DB version against this range and raises `SchemaVersionMismatch` on mismatch, regardless of `auto_migrate`. Default range is `min = schema_version`, `max = None` (unbounded forward) — matches expand-contract migrations
- `SchemaVersionMismatch` exception (under `pgwerk.exceptions`) for clear failure when the DB is older than the code requires or newer than the code supports

### Changed

- `Werk` no longer requires `connect()` to access the queue. Repositories are constructed lazily on first access, so `Werk(dsn).enqueue(...)` works from any process (e.g. an external worker enqueueing into pgwerk) as long as migrations have been run by some process. `connect()` remains the entry point for migrations, the in-process sync worker, and `on_startup` hooks
- The API/dashboard server (`pgwerk.api`) now constructs `Werk(..., auto_migrate=False)`. The API is no longer the owner of schema migrations — run `werk migrate` (or boot a worker, which still auto-migrates by default) to upgrade. The API will refuse to start with a clear error if it sees an incompatible DB version

## [0.1.11] - 2026-05-08

### Added

- `_pgwerk_schedules` table — recurring-job definitions are first-class rows keyed by `name`, with a new `Schedule` dataclass and `ScheduleRepository`
- `on_unregistered` policy on `CronScheduler` (default `"pause"`, also `"keep"` and `"delete"`) for reconciling orphan schedules at startup
- `CronScheduler` registers in `_pgwerk_worker` with `role = 'scheduler'` (new `role` column)
- `CronScheduler.schedule()`, `schedule_at()`, `schedule_in()` — imperative registration that upserts to the DB immediately; safe to call after `run()` has started, and `_at` / `_in` variants anchor the first run at an explicit time
- `Werk.enqueue_at(dt, func, ...)` and `Werk.enqueue_in(delay, func, ...)` — thin wrappers over `enqueue()` for one-shot deferred jobs
- `GET /api/schedules` — list every registered schedule row (including never-fired ones)
- `GET /api/schedules/stats` — per-schedule aggregate job statistics (the previous list endpoint's shape)
- `GET /api/schedules/{name}` — fetch a single schedule row
- `POST /api/jobs` now returns `404` (instead of `500`) when `schedule_name` references a schedule that doesn't exist

### Changed

- Scheduler coordination uses `FOR NO KEY UPDATE SKIP LOCKED` on `_pgwerk_schedules` instead of advisory-lock primary/standby
- Renamed `jobs.cron_name` to `jobs.schedule_name` with a FK to `schedules(name)` (`ON DELETE SET NULL`, `ON UPDATE CASCADE`). The migration adds the FK with `NOT VALID` so existing rows are not scanned — pre-migration `cron_name` values that point to nothing become orphan labels. Run `ALTER TABLE <prefix>_jobs VALIDATE CONSTRAINT <prefix>_jobs_schedule_name_fkey` after backfilling or cleaning up to promote it
- `CronScheduler.register()` stages in memory; the DB is written by `sync()` (auto-called by `run()`)
- `POST /api/schedules/{name}/trigger` now returns the updated schedule row instead of the previously-enqueued job (triggering advances `next_run_at`; the next scheduler tick does the enqueue)

### Removed

- `CronJob` (replaced by `Schedule`), advisory-lock primary/standby machinery, and `cron_standby_retry_interval` config
- `/api/cron/*` routes (replaced by `/api/schedules/*`)

## [0.1.10] - 2026-05-07

### Changed

- `sync=True` execution no longer registers a pseudo-worker in `_pgwerk_worker` or claims a row in `_pgwerk_worker_jobs` — sync runs stay fully in-process and only record an execution row with `worker_id = NULL`

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
