"""Integration tests for group_key serial enforcement."""

from __future__ import annotations

from psycopg.sql import SQL

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import track_conc
from .tasks import _conc_state
from .tasks import reset_conc_tracker
from .conftest import make_worker


class TestGroupKey:
    async def test_active_group_job_blocks_sibling(self, app):
        """While a same-group job is ACTIVE, the worker cannot dequeue its sibling.

        The group_key check filters on status = 'active'. This prevents cross-batch
        claims — it does not serialize jobs within a single dequeue batch.
        """
        job_a = await app.enqueue(noop, _group="g1")
        job_b = await app.enqueue(noop, _group="g1")

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', worker_id = gen_random_uuid(), attempts = 1, started_at = NOW()
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job_a.id},
            )

        await make_worker(app).run()

        assert (await app.get_job(job_b.id)).status == JobStatus.Queued

    async def test_group_job_runnable_after_sibling_completes(self, app):
        """Once the active same-group job finishes, the sibling is dequeued normally."""
        job_a = await app.enqueue(noop, _group="g2")
        job_b = await app.enqueue(noop, _group="g2")

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'complete', worker_id = NULL, completed_at = NOW(), attempts = 1
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job_a.id},
            )

        await make_worker(app).run()

        assert (await app.get_job(job_b.id)).status == JobStatus.Complete

    async def test_different_groups_run_concurrently(self, app):
        """Jobs with distinct group_keys can execute in parallel."""
        reset_conc_tracker()
        for i in range(6):
            await app.enqueue(track_conc, seconds=0.1, _group=f"group-{i}")

        await make_worker(app, concurrency=10).run()

        assert _conc_state["max"] > 1

    async def test_ungrouped_jobs_not_blocked_by_grouped(self, app):
        """Jobs without a group_key are not affected by group serialization."""
        for _ in range(3):
            await app.enqueue(noop, _group="some-group")
        for _ in range(3):
            await app.enqueue(noop)

        await make_worker(app, concurrency=10).run()

        jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in jobs)

    async def test_group_enforcement_is_per_queue(self, app):
        """Group enforcement only looks at jobs in the same queues the worker listens to.
        An active job in queue-A with group G does not block a job in queue-B with group G."""
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    INSERT INTO {jobs} (function, queue, group_key, status, worker_id, attempts, started_at, max_attempts)
                    VALUES ('tests.integration.tasks.noop', 'q-a', 'shared-group', 'active',
                            gen_random_uuid(), 1, NOW(), 1)
                """).format(jobs=app._t["jobs"]),
            )

        job_b = await app.enqueue(noop, _queue="q-b", _group="shared-group")

        await make_worker(app, queues=["q-b"]).run()

        assert (await app.get_job(job_b.id)).status == JobStatus.Complete
