"""Integration tests for requeue behaviour."""

from __future__ import annotations

from pgwerk.commons import JobStatus

from .conftest import make_worker
from .tasks import fail_always, fail_once


class TestRequeue:
    async def test_requeue_preserves_attempts_and_bumps_max(self, app):
        """After requeue, attempts is kept and max_attempts grows to allow another run."""
        job = await app.enqueue(fail_always, _retry=0)
        await make_worker(app).run()

        failed = await app.get_job(job.id)
        assert failed.status == JobStatus.Failed
        assert failed.attempts == 1

        await app.requeue_job(job.id)

        requeued = await app.get_job(job.id)
        assert requeued.status == JobStatus.Queued
        assert requeued.attempts == 1
        assert requeued.max_attempts == 2

    async def test_requeue_execution_count_continues(self, app):
        """The second run after requeue records attempt 2, not attempt 1."""
        job = await app.enqueue(fail_always, _retry=0)
        await make_worker(app).run()

        await app.requeue_job(job.id)
        await make_worker(app).run()

        execs = await app.get_executions(job.id)
        assert len(execs) == 2
        attempts = sorted(e.attempt for e in execs)
        assert attempts == [1, 2]

    async def test_requeue_lets_job_succeed(self, app):
        """A job that fails once can succeed after requeue (using fail_once)."""
        job = await app.enqueue(fail_once, _retry=0)
        await make_worker(app).run()

        failed = await app.get_job(job.id)
        assert failed.status == JobStatus.Failed

        await app.requeue_job(job.id)
        await make_worker(app).run()

        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.attempts == 2

    async def test_bulk_requeue_preserves_attempts(self, app):
        """bulk_requeue applies the same attempt-preserving logic."""
        job = await app.enqueue(fail_always, _retry=0)
        await make_worker(app).run()

        failed = await app.get_job(job.id)
        assert failed.attempts == 1

        await app.bulk_requeue_jobs()

        requeued = await app.get_job(job.id)
        assert requeued.status == JobStatus.Queued
        assert requeued.attempts == 1
        assert requeued.max_attempts == 2
