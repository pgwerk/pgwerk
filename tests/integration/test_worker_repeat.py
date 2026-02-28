"""Integration tests for worker repeat functionality."""

from __future__ import annotations

import pytest

from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_callbacks():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWorkerRepeat:
    async def test_repeat_reenqueues_after_success(self, app):
        from wrk.schemas import Repeat

        await app.enqueue(noop, _repeat=Repeat(times=2))
        await make_worker(app).run()
        jobs = await app.list_jobs()
        noop_jobs = [j for j in jobs if "noop" in j.function]
        assert len(noop_jobs) == 3  # original + 2 repeats

    async def test_failed_job_does_not_repeat(self, app):
        from wrk.schemas import Repeat

        await app.enqueue(fail_always, _repeat=Repeat(times=3))
        await make_worker(app).run()
        jobs = await app.list_jobs()
        assert len(jobs) == 1
