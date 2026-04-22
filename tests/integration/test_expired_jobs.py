"""Integration tests for job TTL and expiry."""

from __future__ import annotations

from psycopg.sql import SQL

from .tasks import noop
from .tasks import fail_always
from .conftest import make_worker

from tests.commons import JobStatus


class TestExpiredJobs:
    async def test_expired_queued_job_not_dequeued(self, app):
        """A job whose expires_at is in the past is skipped by the dequeue query."""
        job = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET expires_at = NOW() - INTERVAL '1 second' WHERE id = %(id)s").format(
                    jobs=app._t["jobs"]
                ),
                {"id": job.id},
            )

        await make_worker(app).run()

        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued, "Expired job should not have been dequeued"

    async def test_failure_ttl_sets_expires_at_on_failed_job(self, app):
        """After terminal failure, expires_at reflects failure_ttl."""
        job = await app.enqueue(fail_always, _retry=1, _failure_ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert done.expires_at is not None

    async def test_result_ttl_sets_expires_at_on_complete_job(self, app):
        """After successful completion, expires_at reflects result_ttl."""
        job = await app.enqueue(noop, _result_ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.expires_at is not None

    async def test_no_ttl_leaves_expires_at_null_on_success(self, app):
        """Without result_ttl, completed jobs have no expiry."""
        job = await app.enqueue(noop)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.expires_at is None

    async def test_non_expired_job_is_dequeued_normally(self, app):
        """A job with a future expires_at is dequeued and processed normally."""
        job = await app.enqueue(noop, _ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
