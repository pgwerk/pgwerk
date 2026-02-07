"""Integration tests for basic worker execution."""

from __future__ import annotations

import pytest

from wrk.commons import JobStatus
from wrk.serializers import PickleSerializer

from .tasks import Box
from .tasks import add
from .tasks import echo
from .tasks import noop
from .tasks import async_add
from .tasks import async_slow
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_callbacks():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWorkerExecution:
    async def test_sync_function_executes(self, app):
        job = await app.enqueue(add, 3, 4)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 7

    async def test_async_function_executes(self, app):
        job = await app.enqueue(async_add, 5, 6)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.result == 11

    async def test_result_stored_on_job(self, app):
        job = await app.enqueue(add, 10, 20)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.result == 30
        assert done.completed_at is not None

    async def test_attempts_incremented(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.attempts == 1

    async def test_worker_id_cleared_after_completion(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.worker_id is None

    async def test_pickle_serializer_round_trips_payload_and_result(self, app):
        app.serializer = PickleSerializer()
        value = Box(value=7)

        job = await app.enqueue(echo, value=value)
        stored = await app.get_job(job.id)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        execs = await app.get_executions(job.id)

        assert stored.payload == {"args": [], "kwargs": {"value": value}}
        assert done.result == value
        assert execs[0].result == value

    async def test_worker_auto_touches_long_running_jobs(self, app):
        job = await app.enqueue(async_slow, seconds=1.6, _heartbeat=1)

        await make_worker(app, sweep_interval=0.05).run()

        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.error is None

    async def test_execution_record_created(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert len(execs) == 1

    async def test_execution_record_has_result(self, app):
        job = await app.enqueue(add, 2, 3)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert execs[0].result == 5

    async def test_execution_record_has_started_and_completed_at(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert execs[0].started_at is not None
        assert execs[0].completed_at is not None
