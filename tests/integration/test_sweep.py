"""Integration tests for sweep functionality."""

from __future__ import annotations

import pytest

from psycopg.sql import SQL

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import clear_callback_log


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestSweep:
    async def test_sweep_requeues_stuck_active_job(self, app):
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept
        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued

    async def test_sweep_fails_job_when_attempts_exhausted(self, app):
        job = await app.enqueue(noop, _retry=1)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', attempts = 1, max_attempts = 1,
                        started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept
        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Failed

    async def test_sweep_returns_empty_when_nothing_stuck(self, app):
        await app.enqueue(noop)
        swept = await app.sweep()
        assert swept == []

    async def test_sweep_respects_heartbeat_secs(self, app):
        job = await app.enqueue(noop, _heartbeat=30)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW() - INTERVAL '1 minute',
                        touched_at = NOW() - INTERVAL '1 minute', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept

    async def test_sweep_ignores_fresh_active_job(self, app):
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW(), worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id not in swept

    async def test_sweep_skips_job_with_fresh_heartbeat(self, app):
        job = await app.enqueue(noop, _heartbeat=60)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW()
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id not in swept
        alive = await app.get_job(job.id)
        assert alive.status == JobStatus.Active

    async def test_sweep_recovers_stale_heartbeat_job(self, app):
        job = await app.enqueue(noop, _heartbeat=5, _retry=2)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW() - INTERVAL '30 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id in swept
        requeued = await app.get_job(job.id)
        assert requeued.status == JobStatus.Queued

    async def test_sweep_fails_stale_job_at_max_attempts(self, app):
        job = await app.enqueue(noop, _heartbeat=5, _retry=1)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        max_attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW() - INTERVAL '30 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id in swept
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

    async def test_sweep_skips_no_heartbeat_fresh_job(self, app):
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '5 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id not in swept
