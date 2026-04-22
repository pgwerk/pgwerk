"""Integration tests for ForkWorker."""

from __future__ import annotations

from pgwerk.worker import ForkWorker
from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import add_pure
from .tasks import fail_pure
from .tasks import slow_pure
from .tasks import crash_process
from .tasks import async_slow_pure


def _fork_worker(app, **kwargs):
    opts = {"concurrency": 4, "burst": True, "poll_interval": 0.05, "sweep_interval": 9999, "abort_interval": 0.05}
    opts.update(kwargs)
    return ForkWorker(app=app, queues=["default"], **opts)


class TestForkWorker:
    async def test_sync_function_executes(self, app):
        job = await app.enqueue(add_pure, 7, 8)
        await _fork_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 15

    async def test_async_function_executes(self, app):
        job = await app.enqueue(async_slow_pure, seconds=0.05)
        await _fork_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == "done"

    async def test_failure_nacked(self, app):
        job = await app.enqueue(fail_pure, _retry=1)
        await _fork_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "intentional failure" in done.error

    async def test_context_injection_rejected(self, app):
        """ForkWorker cannot inject Context — rejects such functions."""
        job = await app.enqueue(noop, _retry=1)
        await _fork_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "ForkWorker" in done.error or "Context" in done.error

    async def test_crash_isolation(self, app):
        """A crashing subprocess fails the job but the worker survives."""
        crash_job = await app.enqueue(crash_process, _retry=1)
        safe_job = await app.enqueue(add_pure, 1, 2)

        await _fork_worker(app).run()

        crashed = await app.get_job(crash_job.id)
        safe = await app.get_job(safe_job.id)

        assert crashed.status == JobStatus.Failed
        assert safe.status == JobStatus.Complete
        assert safe.result == 3

    async def test_process_exitcode_in_error(self, app):
        """Non-zero exit code from subprocess is recorded in job error."""
        job = await app.enqueue(crash_process, _retry=1)
        await _fork_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert done.error is not None

    async def test_timeout_enforced(self, app):
        """ForkWorker terminates the subprocess after timeout_secs."""
        job = await app.enqueue(slow_pure, seconds=30.0, _timeout=1, _retry=1)
        await _fork_worker(app, concurrency=1).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

    async def test_multiple_jobs(self, app):
        for i in range(3):
            await app.enqueue(add_pure, i, i * 3)
        await _fork_worker(app).run()
        jobs = await app.list_jobs(limit=100)
        assert all(j.status == JobStatus.Complete for j in jobs)
