"""Integration tests for failure mode handling."""

from __future__ import annotations

import asyncio

import pytest

from .tasks import on_failure
from .tasks import on_stopped
from .tasks import slow_async
from .tasks import fail_always
from .tasks import _callback_log
from .tasks import clear_callback_log
from .conftest import make_worker

from tests.commons import JobStatus


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

    async def test_aborted_failure_mode_delete_calls_on_stopped(self, app):
        job = await app.enqueue(slow_async, seconds=1.0, _failure_mode="delete", _on_stopped=on_stopped)
        worker = make_worker(app, burst=True, abort_interval=0.05, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.15)
        await app.abort_job(job.id)
        await asyncio.wait_for(worker_task, timeout=3.0)
        assert any(ev == "stopped" and jid == job.id for ev, jid in _callback_log)
