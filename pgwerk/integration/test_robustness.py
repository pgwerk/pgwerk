"""Integration tests for jobs with unresolvable function paths."""

from __future__ import annotations

from psycopg.sql import SQL
from psycopg.sql import Identifier

from wrk.commons import JobStatus

from .tasks import noop
from .conftest import make_worker


class TestMissingFunction:
    async def _insert_raw_job(self, app, function: str) -> str:
        """Insert a job with an arbitrary function path, bypassing enqueue()."""
        pool = app._pool_or_raise()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    INSERT INTO {jobs} (function, queue, status, scheduled_at, max_attempts)
                    VALUES (%(fn)s, 'default', 'queued', NOW(), 1)
                    RETURNING id::text
                """).format(jobs=app._t["jobs"]),
                {"fn": function},
            )
            row = await cur.fetchone()
            job_id = row[0]
            await conn.execute(
                SQL("NOTIFY {ch}").format(ch=Identifier(f"{app.prefix}:default")),
            )
        return job_id

    async def test_nonexistent_module_fails_job(self, app):
        """A job whose module cannot be imported is nacked as FAILED."""
        job_id = await self._insert_raw_job(app, "nonexistent.module.some_function")
        await make_worker(app).run()

        done = await app.get_job(job_id)
        assert done.status == JobStatus.Failed
        assert done.error is not None

    async def test_nonexistent_attribute_fails_job(self, app):
        """A job whose attribute doesn't exist in a real module is nacked as FAILED."""
        job_id = await self._insert_raw_job(app, "tests.integration.tasks.this_does_not_exist")
        await make_worker(app).run()

        done = await app.get_job(job_id)
        assert done.status == JobStatus.Failed
        assert done.error is not None

    async def test_bad_function_does_not_crash_worker(self, app):
        """A bad job function fails the job but the worker continues to process others."""
        bad_id = await self._insert_raw_job(app, "no.such.module.fn")
        good_job = await app.enqueue(noop)

        await make_worker(app).run()

        assert (await app.get_job(bad_id)).status == JobStatus.Failed
        assert (await app.get_job(good_job.id)).status == JobStatus.Complete

    async def test_bad_function_retried_then_failed(self, app):
        """Worker retries a bad-function job until max_attempts, then marks FAILED."""
        pool = app._pool_or_raise()
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    INSERT INTO {jobs} (function, queue, status, scheduled_at, max_attempts)
                    VALUES ('bad.module.fn', 'default', 'queued', NOW(), 3)
                    RETURNING id::text
                """).format(jobs=app._t["jobs"]),
                {"fn": "bad.module.fn"},
            )
            row = await cur.fetchone()
            job_id = row[0]
            await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{app.prefix}:default")))

        await make_worker(app).run()

        done = await app.get_job(job_id)
        assert done.status == JobStatus.Failed
        assert done.attempts == 3
