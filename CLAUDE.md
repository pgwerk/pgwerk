# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**wrk** is a Postgres-backed job queue library. It auto-migrates a shared schema on first connect using advisory locks to prevent races.

## Commands

### Python

```bash
cd python

# Install (dev)
uv sync --extra dev

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/unit/test_job.py

# Run integration tests (requires Postgres — see below)
WRK_TEST_DSN="postgresql://wrk:wrk@localhost/wrk" uv run pytest tests/integration/

# Lint
uv run ruff check .
uv run ruff format .
```


## Integration test database

Integration tests expect a Postgres instance at `postgresql://wrk:wrk@localhost/wrk` (override with `WRK_TEST_DSN`). Tables are auto-created by `Wrk.connect()` / `app.connect()` and truncated between tests via the `app` fixture in `tests/integration/conftest.py`.

## Architecture

### Schema (both implementations)

Four tables, prefixed (default `_wrk_`), optionally schema-qualified:

| Table | Purpose |
|---|---|
| `_wrk_worker` | Registered worker instances + heartbeat |
| `_wrk_jobs` | The job queue — all state lives here |
| `_wrk_worker_jobs` | Claim tracking (worker ↔ job) |
| `_wrk_jobs_executions` | Per-attempt execution history |
| `_wrk_job_deps` | Job dependency graph (Python only) |

Dequeue uses `SELECT … FOR UPDATE SKIP LOCKED` inside a transaction for safe concurrent polling. Workers also use `LISTEN/NOTIFY` for instant wake-up on enqueue.

### Python worker hierarchy

`BaseWorker` (polling loop, dequeue, ack/nack) is subclassed by:
- `AsyncWorker` — asyncio, runs handlers as coroutines
- `ThreadWorker` — runs handlers in a thread pool
- `ProcessWorker` — runs handlers in a process pool
- `ForkWorker` — forks per job

### Python-only features vs Go

The Python implementation adds: `Retry` (exponential back-off intervals), `Repeat` (recurring jobs), `Callback` (on_success/on_failure hooks), `Dependency` (DAG dependencies), `CronScheduler`, pluggable `Serializer` (JSON/Pickle), `heartbeat_secs` for long-running jobs, and a `CLI` (`wrk` command via Click).

The Go implementation is a leaner subset: enqueue, get, cancel, and a single goroutine-based `Worker`.

### Configuration

Both use an options/builder pattern. Python: `Wrk(dsn, prefix=..., schema=..., serializer=...)`. Go: `New(dsn, WithSchema(...), WithPrefix(...), WithPoolSize(...))`. Table names are constructed at init time and stored — never interpolated at query time except through the pre-built identifiers.
