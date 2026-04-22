"""Integration tests for get_executions functionality."""

from __future__ import annotations

import pytest

from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestGetExecutions:
    async def test_get_executions_after_retry(self, app):
        job = await app.enqueue(fail_always, _retry=2)
        await make_worker(app).run()
        execs = await app.get_executions(job.id)
        assert len(execs) == 2
        assert all(e.status.value == "failed" for e in execs)

    async def test_get_executions_empty_before_run(self, app):
        job = await app.enqueue(noop)
        execs = await app.get_executions(job.id)
        assert execs == []
