"""Integration tests for worker registration and heartbeat."""

from __future__ import annotations

import asyncio

from psycopg.sql import SQL

from pgwerk.worker import AsyncWorker

from .tasks import noop
from .conftest import make_worker


class TestWorkerLifecycle:
    async def test_worker_registers_in_db(self, app):
        """Worker inserts a row in the worker table and marks it stopped after run."""
        await app.enqueue(noop)
        worker = make_worker(app)
        await worker.run()

        pool = app._pool_or_raise()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT status FROM {worker} WHERE id = %(id)s").format(worker=app._t["worker"]),
                {"id": worker.id},
            )
            row = await cur.fetchone()

        assert row is not None
        assert row[0] == "stopped"

    async def test_worker_heartbeat_updates_db(self, app):
        """Worker heartbeat loop updates heartbeat_at in the worker table."""
        worker = AsyncWorker(
            app=app,
            queues=["default"],
            burst=False,
            heartbeat_interval=0.05,
            poll_interval=0.05,
            sweep_interval=9999,
            abort_interval=0.05,
        )
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.3)
        worker._request_shutdown()
        await asyncio.wait_for(task, timeout=5.0)

        pool = app._pool_or_raise()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT heartbeat_at FROM {worker} WHERE id = %(id)s").format(worker=app._t["worker"]),
                {"id": worker.id},
            )
            row = await cur.fetchone()

        assert row is not None
        assert row[0] is not None, "heartbeat_at should be set after worker ran"

    async def test_multiple_workers_all_register(self, app):
        """Every worker in a concurrent pool registers its own DB row."""
        workers = [make_worker(app, burst=False) for _ in range(3)]
        tasks = [asyncio.create_task(w.run()) for w in workers]

        await asyncio.sleep(0.3)

        pool = app._pool_or_raise()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT COUNT(*) FROM {worker}").format(worker=app._t["worker"]),
            )
            row = await cur.fetchone()

        for worker in workers:
            worker._request_shutdown()
        await asyncio.gather(*tasks)

        assert row[0] >= 3
