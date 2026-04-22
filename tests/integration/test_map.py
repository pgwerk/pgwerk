"""Integration tests for map functionality."""

from __future__ import annotations

import asyncio

import pytest

from .tasks import add
from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestMap:
    async def test_map_returns_all_results(self, app):
        worker = make_worker(app, burst=True, poll_interval=0.05)
        kwargs_list = [{"x": i, "y": i} for i in range(5)]
        results, _ = await asyncio.gather(
            app.map(add, kwargs_list, poll_interval=0.1, timeout=5.0),
            worker.run(),
        )
        assert results == [0, 2, 4, 6, 8]

    async def test_map_raises_on_first_failure(self, app):
        from tests.exceptions import JobError

        worker = make_worker(app, burst=True, poll_interval=0.05)
        kwargs_list = [{}, {}, {}]
        with pytest.raises(JobError):
            await asyncio.gather(
                app.map(fail_always, kwargs_list, poll_interval=0.1, timeout=3.0),
                worker.run(),
            )

    async def test_map_return_exceptions_collects_errors(self, app):
        from tests.exceptions import JobError

        worker = make_worker(app, burst=True, poll_interval=0.05)
        kwargs_list = [{}, {}, {}]
        results, _ = await asyncio.gather(
            app.map(fail_always, kwargs_list, return_exceptions=True, poll_interval=0.1, timeout=3.0),
            worker.run(),
        )
        assert all(isinstance(r, JobError) for r in results)

    async def test_map_with_timeout_raises(self, app):
        with pytest.raises(asyncio.TimeoutError):
            await app.map(noop, [{}, {}], timeout=0.05, poll_interval=0.02)
