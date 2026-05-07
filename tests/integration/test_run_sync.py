"""Integration tests for run_sync and enqueue(_sync=True)."""

from __future__ import annotations

import pytest

from pgwerk.commons import JobStatus

from .tasks import add
from .tasks import async_add
from .tasks import fail_always
from .tasks import noop


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
