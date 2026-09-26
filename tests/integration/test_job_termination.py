from __future__ import annotations

import asyncio

import pytest

from pgwerk.commons import JobStatus
from pgwerk.commons import ExecutionStatus

from .tasks import noop
from .tasks import on_success
from .tasks import on_failure
from .tasks import on_stopped
from .tasks import slow_async
from .tasks import _callback_log
from .tasks import controlled_async
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestJobTermination:
    async def test_cancel_queued_job(self, app):
        job = await app.enqueue(noop)
        result = await app.cancel_job(job.id)
        assert result is True
        fetched = await app.get_job(job.id)
        assert fetched.status == JobStatus.Aborted

    async def test_cancel_nonexistent_returns_false(self, app):
        result = await app.cancel_job("00000000-0000-0000-0000-000000000000")
        assert result is False

    async def test_cancel_already_complete_returns_false(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        result = await app.cancel_job(job.id)
        assert result is False

    async def test_abort_running_job(self, app, async_job_control):
        job = await app.enqueue(controlled_async, _retry=3)
        worker = make_worker(app)
        worker_task = asyncio.create_task(worker.run())

        try:
            await asyncio.wait_for(async_job_control.started.wait(), timeout=5.0)
            assert (await app.get_job(job.id)).status == JobStatus.Active
            assert await app.abort_job(job.id) is True
            await asyncio.wait_for(async_job_control.cancelled.wait(), timeout=5.0)
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=5.0)

            done = await app.get_job(job.id)
            assert done.status == JobStatus.Aborted
            assert done.worker_id is None
            assert done.completed_at is not None
            assert done.attempts == 1
            executions = await app.get_executions(job.id)
            assert len(executions) == 1
            assert executions[0].status == ExecutionStatus.Aborted
            assert executions[0].completed_at is not None
            assert async_job_control.calls == 1
            assert async_job_control.completed == 0
        finally:
            async_job_control.release.set()
            worker._request_shutdown()
            await asyncio.wait_for(worker_task, timeout=5.0)

    async def test_abort_returns_false_for_non_active_job(self, app):
        job = await app.enqueue(noop)
        result = await app.abort_job(job.id)
        assert result is False

    async def test_abort_triggers_on_stopped_callback(self, app, async_job_control):
        job = await app.enqueue(
            controlled_async, _on_stopped=on_stopped, _on_success=on_success, _on_failure=on_failure
        )
        worker = make_worker(app)
        worker_task = asyncio.create_task(worker.run())

        try:
            await asyncio.wait_for(async_job_control.started.wait(), timeout=5.0)
            assert await app.abort_job(job.id) is True
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=5.0)
            assert _callback_log == [("stopped", job.id)]
        finally:
            async_job_control.release.set()
            worker._request_shutdown()
            await asyncio.wait_for(worker_task, timeout=5.0)

    async def test_job_requeued_when_worker_shutdown_during_execution(self, app):
        """When a worker drains and the drain timeout expires, running jobs are cancelled
        and re-queued (not marked failed), and attempts is decremented back."""
        job = await app.enqueue(slow_async, seconds=2.0, _retry=3)
        worker = make_worker(app, burst=False, poll_interval=0.05, shutdown_timeout=0.1)
        worker_task = asyncio.create_task(worker.run())

        await asyncio.sleep(0.15)

        worker._request_shutdown()
        await asyncio.wait_for(worker_task, timeout=3.0)

        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued
        assert refreshed.attempts == 0
