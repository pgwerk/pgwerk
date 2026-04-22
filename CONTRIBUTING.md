# Contributing

Thanks for taking the time. Here's how to get up and running and what to expect when you open a PR.

## Setup

You need Python 3.11+ and a Postgres instance for integration tests.

```bash
# Clone
git clone https://github.com/ccrvlh/wrk
cd wrk

# Install with dev extras
uv sync --extra dev

# Start Postgres (Docker example)
docker run -d \
  --name wrk-postgres \
  -e POSTGRES_USER=wrk \
  -e POSTGRES_PASSWORD=wrk \
  -e POSTGRES_DB=wrk_test \
  -p 5432:5432 \
  postgres:16
```

## Running tests

```bash
# Unit tests (no Postgres needed)
uv run pytest tests/unit/

# Integration tests
PGWERK_TEST_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk_test" uv run pytest tests/integration/

# All tests
PGWERK_TEST_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk_test" uv run pytest

# Single file
uv run pytest tests/unit/test_job.py -v
```

## Linting

```bash
uv run ruff check .
uv run ruff format .
```

CI runs both on every push and pull request.

## Opening a PR

- Keep PRs focused — one concern per PR makes review faster.
- Add or update tests for any changed behaviour.
- Integration tests live in `tests/integration/`; unit tests in `tests/unit/`.
- Update `CHANGELOG.md` under `[Unreleased]` with a brief description of your change.
- CI must pass before merge.

## Project structure

```
pgwerk/              source package
  app.py          Wrk app (connect, enqueue, inspect)
  worker/         BaseWorker + AsyncWorker / ThreadWorker / ProcessWorker / ForkWorker
  repos.py        Database repository layer (all SQL lives here)
  database.py     Migration runner
  schemas.py      Pydantic / dataclass models
  cron.py         CronScheduler + CronJob
  cli/            Click CLI (wrk worker / info / purge)
  api/            Optional Litestar REST API
  exporter/       Optional Prometheus metrics
tests/
  unit/           Pure-Python unit tests, no DB required
  integration/    Full-stack tests against a live Postgres instance
```

## Reporting bugs

Open an issue with:
1. What you expected to happen.
2. What actually happened (include logs where relevant).
3. A minimal reproduction — ideally a single `pytest` test or a short script.
