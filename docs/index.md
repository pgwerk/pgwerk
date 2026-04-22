# wrk

A Postgres-backed job queue. Durable, visible, transactional.

Jobs are rows. Workers poll with `SELECT … FOR UPDATE SKIP LOCKED`. No external broker, no sidecar — just your existing Postgres instance.

## Install

```bash
pip install wrk
```

## Quickstart

```python
from pgwerk import Wrk, AsyncWorker

app = Wrk("postgresql://user:pass@localhost/mydb")

async def send_email(to: str):
    ...

await app.connect()
await app.enqueue(send_email, to="user@example.com")
```

Run a worker:

```python
worker = AsyncWorker(app=app, queues=["default"], concurrency=10)
await worker.run()
```

See the [Guide](guide/quickstart.md) for the full picture.
