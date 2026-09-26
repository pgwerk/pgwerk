"""Integration tests for abort requests racing with acknowledgement."""

from __future__ import annotations

import asyncio

from datetime import timedelta

import pytest

from pgwerk.schemas import Repeat
from pgwerk.commons import JobStatus
from pgwerk.commons import ExecutionStatus
from pgwerk.exceptions import JobNotFound

from .tasks import noop
from .tasks import on_success
from .tasks import on_failure
from .tasks import on_stopped
from .tasks import _callback_log
from .tasks import controlled_async
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestAbortRaces:
    @pytest.mark.parametrize("failure_mode", ["hold", "delete"])
    @pytest.mark.parametrize("poll_abort", [False, True])
    @pytest.mark.parametrize("cancel_ack", [False, True])
    async def test_abort_before_ack_does_not_rerun_handler(
        self, app, async_job_control, monkeypatch, failure_mode, poll_abort, cancel_ack
    ):
        ack_entered = asyncio.Event()
        release_ack = asyncio.Event()
        abort_observed = asyncio.Event()
        release_poll = asyncio.Event()
        async_job_control.release.set()
        job = await app.enqueue(
            controlled_async,
            _retry=1,
            _repeat=Repeat(times=1),
            _failure_mode=failure_mode,
            _failure_ttl=3600,
            _result_ttl=60,
            _on_stopped=on_stopped,
            _on_success=on_success,
            _on_failure=on_failure,
        )
        child = await app.enqueue(noop, _depends_on=job, _queue="dependents")
        worker = make_worker(app, concurrency=1)
        original_ack = worker._ack
        original_get_aborting = app._worker_repo.get_aborting

        async def gated_ack(j, result=None):
            ack_entered.set()
            await release_ack.wait()
            await original_ack(j, result)

        async def observed_get_aborting(job_ids):
            if not poll_abort:
                await release_poll.wait()
            ids = await original_get_aborting(job_ids)
            if job.id in ids:
                abort_observed.set()
            return ids

        monkeypatch.setattr(worker, "_ack", gated_ack)
        monkeypatch.setattr(app._worker_repo, "get_aborting", observed_get_aborting)
        worker_task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(ack_entered.wait(), timeout=5.0)
            assert async_job_control.completed == 1
            assert await app.abort_job(job.id) is True
            if poll_abort:
                await asyncio.wait_for(abort_observed.wait(), timeout=5.0)
            if cancel_ack:
                # A drain timeout cancels the task that owns this execution.
                worker._active_jobs[job.id].cancel()
            release_ack.set()
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=5.0)

            assert async_job_control.calls == 1
            assert async_job_control.completed == 1
            assert _callback_log == [("stopped", job.id)]
            assert (await app.get_job(child.id)).status == JobStatus.Failed
            if failure_mode == "hold":
                done = await app.get_job(job.id)
                assert done.status == JobStatus.Aborted
                assert done.result is None
                assert done.error == "aborted"
                assert done.attempts == 1
                assert done.worker_id is None
                assert done.expires_at >= done.completed_at + timedelta(minutes=59)
                executions = await app.get_executions(job.id)
                assert len(executions) == 1
                assert executions[0].status == ExecutionStatus.Aborted
                assert executions[0].result is None
                assert executions[0].error == "aborted"
                assert executions[0].completed_at is not None
            else:
                with pytest.raises(JobNotFound):
                    await app.get_job(job.id)
            jobs = await app.list_jobs()
            expected_ids = {child.id, job.id} if failure_mode == "hold" else {child.id}
            assert {j.id for j in jobs} == expected_ids  # no repeat was enqueued
        finally:
            release_ack.set()
            release_poll.set()
            worker._request_shutdown()
            await asyncio.wait_for(worker_task, timeout=5.0)

    async def test_abort_after_ack_is_rejected(self, app, async_job_control, monkeypatch):
        ack_committed = asyncio.Event()
        release_ack = asyncio.Event()
        async_job_control.release.set()
        job = await app.enqueue(controlled_async, _on_success=on_success, _on_stopped=on_stopped)
        worker = make_worker(app)
        original_ack = worker._ack

        async def gated_ack(j, result=None):
            await original_ack(j, result)
            ack_committed.set()
            await release_ack.wait()

        monkeypatch.setattr(worker, "_ack", gated_ack)
        worker_task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(ack_committed.wait(), timeout=5.0)
            assert await app.abort_job(job.id) is False
            release_ack.set()
            await asyncio.wait_for(asyncio.shield(worker_task), timeout=5.0)

            done = await app.get_job(job.id)
            assert done.status == JobStatus.Complete
            assert done.result == "done"
            assert async_job_control.calls == 1
            assert _callback_log == [("success", job.id)]
        finally:
            release_ack.set()
            worker._request_shutdown()
            await asyncio.wait_for(worker_task, timeout=5.0)
