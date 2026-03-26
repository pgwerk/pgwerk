"""Integration tests for ThreadWorker."""

from __future__ import annotations

from wrk.worker import ThreadWorker
from wrk.commons import JobStatus

from .tasks import noop
from .tasks import add_pure
from .tasks import fail_pure
from .tasks import slow_pure
from .tasks import async_add_pure


def _thread_worker(app, **kwargs):
    opts = {"concurrency": 10, "burst": True, "poll_interval": 0.05, "sweep_interval": 9999, "abort_interval": 0.05}
    opts.update(kwargs)
    return ThreadWorker(app=app, queues=["default"], **opts)


class TestThreadWorker:
    async def test_sync_function_executes(self, app):
        job = await app.enqueue(slow_pure, seconds=0.05)
        await _thread_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "done"

    async def test_async_function_executes(self, app):
        job = await app.enqueue(async_add_pure, 3, 4)
        await _thread_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 7

    async def test_ctx_function_executes(self, app):
        """ThreadWorker supports context injection unlike Process/ForkWorker."""
        job = await app.enqueue(noop)
        await _thread_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete

    async def test_failure_nacked(self, app):
        job = await app.enqueue(fail_pure, _retry=1)
        await _thread_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "intentional failure" in done.error

    async def test_multiple_jobs(self, app):
        for i in range(5):
            await app.enqueue(add_pure, i, i * 2)
        await _thread_worker(app).run()
        jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in jobs)

    async def test_timeout_enforced(self, app):
        """Sync function running too long is timed out and job is failed."""
        job = await app.enqueue(slow_pure, seconds=2.0, _timeout=1, _retry=1)
        await _thread_worker(app, concurrency=1).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

    async def test_retry_after_failure(self, app):
        from .tasks import fail_once

        job = await app.enqueue(fail_once, _retry=3)
        await _thread_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "recovered"
