"""Integration tests for worker callbacks."""

from __future__ import annotations

import pytest

from .tasks import noop
from .tasks import on_success
from .tasks import on_failure
from .tasks import fail_always
from .tasks import _callback_log
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_callbacks():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWorkerCallbacks:
    async def test_on_success_called(self, app):
        job = await app.enqueue(noop, _on_success=on_success)
        await make_worker(app).run()
        assert any(event == "success" and jid == job.id for event, jid in _callback_log)

    async def test_on_failure_called(self, app):
        job = await app.enqueue(fail_always, _on_failure=on_failure)
        await make_worker(app).run()
        assert any(event == "failure" and jid == job.id for event, jid in _callback_log)

    async def test_on_success_not_called_on_failure(self, app):
        await app.enqueue(fail_always, _on_success=on_success)
        await make_worker(app).run()
        assert not any(event == "success" for event, _ in _callback_log)

    async def test_on_failure_not_called_on_success(self, app):
        await app.enqueue(noop, _on_failure=on_failure)
        await make_worker(app).run()
        assert not any(event == "failure" for event, _ in _callback_log)
