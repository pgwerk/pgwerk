"""Integration tests for run_sync and enqueue(_sync=True)."""

from __future__ import annotations

import asyncio

import pytest

from pgwerk.commons import JobStatus

from .tasks import add
from .tasks import async_add
from .tasks import fail_always
from .tasks import noop
from .conftest import make_worker


class TestRunSync:
    async def test_run_sync_returns_completed_job(self, app):
        job = await app.enqueue(add, 3, 4)
        done = await app.run_sync(job)
        assert done.status == JobStatus.Complete
        assert done.result == 7

    async def test_run_sync_async_function(self, app):
        job = await app.enqueue(async_add, 5, 6)
        done = await app.run_sync(job)
        assert done.status == JobStatus.Complete
        assert done.result == 11

    async def test_run_sync_failure_returns_failed_job(self, app):
        job = await app.enqueue(fail_always)
        done = await app.run_sync(job)
        assert done.status == JobStatus.Failed

    async def test_run_sync_increments_attempts(self, app):
        job = await app.enqueue(noop)
        done = await app.run_sync(job)
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

    async def test_run_sync_raises_when_disconnected(self):
        from pgwerk.app import Werk

        app = Werk("postgresql://pgwerk:pgwerk@localhost/pgwerk_test")
        job_stub = object()
        with pytest.raises(RuntimeError, match="Not connected"):
            await app.run_sync(job_stub)  # type: ignore[arg-type]


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
    """Regression tests for the race where a background worker steals a _sync=True job.

    Root cause: insert() sent NOTIFY, which immediately woke up any LISTEN-ing worker.
    The worker could claim the job between insert and claim_sync, causing claim_sync to
    fail with "not found or not in queued state". Fix: skip NOTIFY when _sync=True.
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
        # Exactly one attempt — if the background worker had stolen and re-tried,
        # attempts could be higher or the claim_sync error would have fired.
        assert done.attempts == 1
