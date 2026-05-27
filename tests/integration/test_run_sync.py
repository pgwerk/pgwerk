"""Integration tests for run_sync and enqueue(_sync=True)."""

from __future__ import annotations

import asyncio

from pgwerk.commons import JobStatus

from .tasks import add
from .tasks import async_add
from .tasks import fail_always
from .tasks import noop
from .conftest import make_worker


class TestRunSync:
    async def test_run_sync_returns_completed_job(self, app):
        done = await app.enqueue(add, 3, 4, _sync=True)
        assert done is not None
        assert done.status == JobStatus.Complete
        assert done.result == 7

    async def test_run_sync_async_function(self, app):
        done = await app.enqueue(async_add, 5, 6, _sync=True)
        assert done is not None
        assert done.status == JobStatus.Complete
        assert done.result == 11

    async def test_run_sync_failure_returns_failed_job(self, app):
        done = await app.enqueue(fail_always, _sync=True)
        assert done is not None
        assert done.status == JobStatus.Failed

    async def test_run_sync_increments_attempts(self, app):
        done = await app.enqueue(noop, _sync=True)
        assert done is not None
        assert done.attempts == 1

    async def test_enqueue_sync_flag_executes_inline(self, app):
        done = await app.enqueue(add, 10, 20, _sync=True)
        assert done is not None
        assert done.status == JobStatus.Complete
        assert done.result == 30

    async def test_enqueue_sync_failure(self, app):
        done = await app.enqueue(fail_always, _sync=True)
        assert done is not None
        assert done.status == JobStatus.Failed


async def _start_worker(app) -> asyncio.Task:
    """Start a continuous background worker and return its task."""
    worker = make_worker(app, burst=False)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)  # let it connect and LISTEN
    return task


async def _stop_worker(task: asyncio.Task) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class TestRunSyncWithConcurrentWorker:
    """Regression tests guarding against a background worker stealing a _sync=True job.

    A _sync=True job is inserted directly as 'active' and owned by the sync worker, so it
    never enters the 'queued' state a polling or LISTEN-ing background worker could
    dequeue. These tests run a background worker on the same queue while enqueuing sync
    jobs to ensure none are stolen or run twice.
    """

    async def test_sync_flag_not_stolen_by_background_worker(self, app):
        task = await _start_worker(app)
        try:
            done = await app.enqueue(add, 1, 2, _sync=True)
        finally:
            await _stop_worker(task)

        assert done is not None
        assert done.status == JobStatus.Complete
        assert done.result == 3

    async def test_sync_failure_not_stolen_by_background_worker(self, app):
        task = await _start_worker(app)
        try:
            done = await app.enqueue(fail_always, _sync=True)
        finally:
            await _stop_worker(task)

        assert done is not None
        assert done.status == JobStatus.Failed

    async def test_multiple_sync_enqueues_with_background_worker(self, app):
        task = await _start_worker(app)
        try:
            results = await asyncio.gather(*[app.enqueue(add, i, i, _sync=True) for i in range(5)])
        finally:
            await _stop_worker(task)

        assert all(r is not None for r in results)
        assert all(r.status == JobStatus.Complete for r in results)
        expected = [i + i for i in range(5)]
        assert [r.result for r in results] == expected

    async def test_sync_job_claimed_by_sync_worker_not_background_worker(self, app):
        """The background worker must not have processed the _sync=True job."""
        task = await _start_worker(app)
        try:
            done = await app.enqueue(noop, _sync=True)
        finally:
            await _stop_worker(task)

        assert done is not None
        assert done.status == JobStatus.Complete
        # Exactly one attempt — the job was never queued, so no background worker
        # could have stolen and re-run it.
        assert done.attempts == 1
