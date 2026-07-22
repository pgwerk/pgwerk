---
title: pgwerk — Postgres-backed job queue for Python
description: Run background jobs in Python using only Postgres. No Redis, no broker. pgwerk uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent dequeue.
keywords: postgres job queue python, background jobs python, celery alternative no redis, postgresql task queue, asyncio worker, transactional job queue
---

# pgwerk

A Postgres-backed job queue for Python. Jobs are rows. Workers dequeue with `SELECT … FOR UPDATE SKIP LOCKED`. No Redis, no RabbitMQ, no sidecar — just your existing Postgres instance.

## Install

```bash
pip install pgwerk
```

Python 3.11+ and Postgres 14+. For cron expression support:

```bash
pip install "pgwerk[cron]"
```

## Quickstart

```python
from pgwerk import Werk, AsyncWorker
import asyncio

app = Werk("postgresql://user:pass@localhost/mydb")

async def send_email(to: str) -> None:
    ...

async def main():
    async with app:
        await app.enqueue(send_email, to="user@example.com")
        worker = AsyncWorker(app=app, queues=["default"], concurrency=10)
        await worker.run()

asyncio.run(main())
```

See the [Quickstart guide](guide/quickstart.md) for the full picture.

## What's different

pgwerk dequeues with `SELECT … FOR UPDATE SKIP LOCKED` — a pattern Postgres has supported since 9.5. That gives you a few properties you don't get from Redis-backed queues:

- **Transactional enqueue** — enqueue inside an open transaction; if it rolls back, the job never exists.
- **SQL visibility** — jobs are rows in `_pgwerk_jobs`. Query them with psql, pgAdmin, or anything else.
- **No broker** — your existing Postgres instance is the queue. Nothing new to run or monitor.
- **LISTEN/NOTIFY** — workers wake up immediately on enqueue instead of waiting for the next poll cycle.

## Limitations

Postgres is not a message broker. At high dequeue rates with many concurrent workers, lock contention on the jobs table becomes the bottleneck. If you're processing millions of jobs per second, a dedicated broker will outperform it.

pgwerk requires Python 3.11+ and psycopg3. It does not support other database drivers, earlier Python versions, or cross-language workers. It is also much newer than Celery or RQ — if you need something battle-tested at scale for years, factor that in.

---

[Quickstart](guide/quickstart.md) · [Workers](guide/workers.md) · [Enqueueing](guide/enqueueing.md) · [Cron](guide/cron.md)