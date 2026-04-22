from __future__ import annotations

import asyncio

import pytest

from wrk.commons import JobStatus

from .tasks import noop
from .tasks import on_stopped
from .tasks import slow_async
from .tasks import _callback_log
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

    async def test_abort_running_job(self, app):
        job = await app.enqueue(slow_async, seconds=1.0)
        worker = make_worker(app, burst=True, abort_interval=0.05, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run())

        await asyncio.sleep(0.15)
        aborted = await app.abort_job(job.id)
        assert aborted is True

        await asyncio.wait_for(worker_task, timeout=3.0)
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Aborted

    async def test_abort_returns_false_for_non_active_job(self, app):
        job = await app.enqueue(noop)
        result = await app.abort_job(job.id)
        assert result is False

    async def test_abort_triggers_on_stopped_callback(self, app):
        job = await app.enqueue(slow_async, seconds=1.0, _on_stopped=on_stopped)
        worker = make_worker(app, burst=True, abort_interval=0.05, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run())

        await asyncio.sleep(0.15)
        await app.abort_job(job.id)
        await asyncio.wait_for(worker_task, timeout=3.0)

        assert any(ev == "stopped" and jid == job.id for ev, jid in _callback_log)

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
