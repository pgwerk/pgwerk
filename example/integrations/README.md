# Framework integrations

Each file is a self-contained, runnable example of using PGWerk from a web
framework. They all enqueue a `send_welcome_email` job as part of a user signup.

The interesting question for a Postgres-backed queue is: **does the job survive
or vanish with the surrounding database transaction?** PGWerk's `enqueue` accepts
a `_conn=` argument and runs the `INSERT` (and the wake-up `NOTIFY`) on *that*
connection — never committing it itself. So if you hand it the same connection
your ORM is using, the job commits or rolls back atomically with your data.

PGWerk's `enqueue` is `async`. That shapes what each framework can promise:

| Framework | Stack | Guarantee | How |
|---|---|---|---|
| FastAPI  | async + async SQLAlchemy (psycopg) | **Same transaction** | Pass SQLAlchemy's raw psycopg `AsyncConnection` as `_conn` |
| Litestar | async + async SQLAlchemy (psycopg) | **Same transaction** | Same as FastAPI |
| Django   | sync ORM | **Enqueue-on-commit** | `transaction.on_commit(...)` — sent only if the tx commits |
| Flask    | sync (Flask-SQLAlchemy) | **Enqueue-on-commit** | SQLAlchemy `after_commit` event |

Why the split: a sync ORM holds a *sync* psycopg connection, which can't be
handed to an `async` enqueue. Rather than pretend, the sync examples use the
"enqueue only after commit" pattern. You still never get an orphaned job (a job
for a row that rolled back); you just don't share the literal transaction.

## Run any of them

```bash
createdb pgwerk            # once
export PGWERK_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk"

uv run uvicorn example.integrations.fastapi_app:app --reload     # FastAPI
uv run litestar --app example.integrations.litestar_app:app run  # Litestar
uv run python example/integrations/flask_app.py                  # Flask
# Django: see the header of django_app.py

# In another terminal, run a worker to actually process the jobs:
uv run werk worker --dsn "$PGWERK_DSN"
```

Then `POST /signup` (with `{"email": "a@b.com"}`) and watch the worker log the
email. To see the transactional guarantee, force an error after `enqueue` but
before commit — the user row *and* the job both disappear.
