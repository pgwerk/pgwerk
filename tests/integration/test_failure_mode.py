"""Integration tests for failure mode handling."""

from __future__ import annotations

import asyncio

import pytest

from pgwerk.commons import JobStatus
from pgwerk.exceptions import JobNotFound

from .tasks import on_failure
from .tasks import on_stopped
from .tasks import fail_always
from .tasks import _callback_log
from .tasks import controlled_async
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestFailureMode:
    async def test_failure_mode_delete_removes_job(self, app):
        job = await app.enqueue(fail_always, _retry=1, _failure_mode="delete")
        await make_worker(app).run()
        with pytest.raises(Exception):
            await app.get_job(job.id)

    async def test_failure_mode_delete_calls_on_failure_callback(self, app):
        job = await app.enqueue(fail_always, _retry=1, _failure_mode="delete", _on_failure=on_failure)
        await make_worker(app).run()
        assert any(ev == "failure" and jid == job.id for ev, jid in _callback_log)

    async def test_failure_mode_hold_keeps_job(self, app):
        job = await app.enqueue(fail_always, _retry=1, _failure_mode="hold")
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

    async def test_aborted_failure_mode_delete_calls_on_stopped(self, app, async_job_control):
        job = await app.enqueue(controlled_async, _failure_mode="delete", _on_stopped=on_stopped)
        worker = make_worker(app)
        worker_task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(async_job_control.started.wait(), timeout=5.0)
            assert await app.abort_job(job.id) is True
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=5.0)
            assert _callback_log == [("stopped", job.id)]
            assert async_job_control.cancelled.is_set()
            assert async_job_control.completed == 0
            with pytest.raises(JobNotFound):
                await app.get_job(job.id)
        finally:
            async_job_control.release.set()
            worker._request_shutdown()
            await asyncio.wait_for(worker_task, timeout=5.0)
