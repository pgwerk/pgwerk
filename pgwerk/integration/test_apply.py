"""Integration tests for apply functionality."""

from __future__ import annotations

import asyncio

import pytest

from .tasks import add
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestApply:
    async def test_apply_returns_result(self, app):
        worker = make_worker(app, burst=True, poll_interval=0.05)
        result, _ = await asyncio.gather(
            app.apply(add, 3, 4, poll_interval=0.1, timeout=3.0),
            worker.run(),
        )
        assert result == 7

    async def test_apply_raises_on_failure(self, app):
        from pgwerk.exceptions import JobError

        worker = make_worker(app, burst=True, poll_interval=0.05)
        with pytest.raises(JobError):
            await asyncio.gather(
                app.apply(fail_always, poll_interval=0.1, timeout=3.0),
                worker.run(),
            )
