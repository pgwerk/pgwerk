# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
