"""Integration tests for worker failure handling."""

from __future__ import annotations

import pytest

from wrk.commons import JobStatus

from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_callbacks():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWorkerFailure:
    async def test_failed_job_has_error(self, app):
        job = await app.enqueue(fail_always)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert "intentional failure" in done.error

    async def test_failed_job_has_completed_at(self, app):
        job = await app.enqueue(fail_always)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.completed_at is not None

    async def test_execution_record_on_failure(self, app):
        job = await app.enqueue(fail_always)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert len(execs) == 1
        assert execs[0].status.value == "failed"
        assert "intentional failure" in execs[0].error
