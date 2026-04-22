"""Integration tests for job heartbeat functionality."""

from __future__ import annotations

import logging

import pytest

from psycopg.sql import SQL

from .tasks import noop
from .tasks import slow_async
from .tasks import clear_callback_log
from .conftest import make_worker

from tests.commons import JobStatus


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestJobHeartbeat:
    async def test_touch_updates_timestamp(self, app):
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET status='active', touched_at='2000-01-01' WHERE id=%s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        await app.touch_job(job.id)
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id=%s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            row = await cur.fetchone()
        assert row[0].year > 2000

    async def test_job_heartbeat_updates_touched_at(self, app):
        """A long-running job with heartbeat_secs should update touched_at periodically."""
        job = await app.enqueue(slow_async, seconds=0.4, _heartbeat=1)
        pool = app._pool_or_raise()

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id = %s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            before = (await cur.fetchone())[0]

        await make_worker(app, sweep_interval=9999).run()

        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id = %s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            after = (await cur.fetchone())[0]

        assert after is not None
        if before is not None:
            assert after > before

    async def test_job_heartbeat_error_does_not_kill_job(self, app, caplog):
        """Heartbeat loop errors are swallowed, not fatal."""
        job = await app.enqueue(slow_async, seconds=0.2, _heartbeat=1)

        call_count = 0
        original = app.touch_job

        async def flaky_touch(_):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("heartbeat error")

        app.touch_job = flaky_touch

        with caplog.at_level(logging.WARNING, logger="wrk.worker.base"):
            await make_worker(app).run()

        app.touch_job = original
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert any("job heartbeat error" in r.message for r in caplog.records)
