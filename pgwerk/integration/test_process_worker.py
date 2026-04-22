"""Integration tests for ProcessWorker."""

from __future__ import annotations

from pgwerk.worker import ProcessWorker
from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import add_pure
from .tasks import fail_pure
from .tasks import slow_pure
from .tasks import async_add_pure


def _process_worker(app, **kwargs):
    opts = {"concurrency": 4, "burst": True, "poll_interval": 0.05, "sweep_interval": 9999, "abort_interval": 0.05}
    opts.update(kwargs)
    return ProcessWorker(app=app, queues=["default"], **opts)


class TestProcessWorker:
    async def test_sync_function_executes(self, app):
        job = await app.enqueue(add_pure, 5, 6)
        await _process_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 11

    async def test_async_function_executes(self, app):
        job = await app.enqueue(async_add_pure, 10, 20)
        await _process_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 30

    async def test_failure_nacked(self, app):
        job = await app.enqueue(fail_pure, _retry=1)
        await _process_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "intentional failure" in done.error

    async def test_context_injection_rejected(self, app):
        """ProcessWorker cannot inject Context — rejects such functions."""
        job = await app.enqueue(noop, _retry=1)
        await _process_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "ProcessWorker" in done.error or "Context" in done.error

    async def test_multiple_jobs_complete(self, app):
        for i in range(4):
            await app.enqueue(add_pure, i, i)
        await _process_worker(app).run()
        jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in jobs)

    async def test_timeout_enforced(self, app):
        """Long-running subprocess is timed out; job is failed."""
        job = await app.enqueue(slow_pure, seconds=5.0, _timeout=1, _retry=1)
        await _process_worker(app, concurrency=1).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
