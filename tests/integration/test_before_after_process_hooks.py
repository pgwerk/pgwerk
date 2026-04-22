"""Integration tests for before/after process hooks."""

from __future__ import annotations

import logging

import pytest

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestBeforeAfterProcessHooks:
    async def test_before_process_called(self, app):
        calls = []

        def hook(ctx):
            calls.append(ctx.job.id)

        job = await app.enqueue(noop)
        worker = make_worker(app)
        worker.add_before_process(hook)
        await worker.run()
        assert job.id in calls

    async def test_after_process_called_on_success(self, app):
        calls = []

        async def hook(ctx):
            calls.append(("after", ctx.job.id))

        job = await app.enqueue(noop)
        worker = make_worker(app)
        worker.add_after_process(hook)
        await worker.run()
        assert any(ev == "after" and jid == job.id for ev, jid in calls)

    async def test_after_process_called_on_failure(self, app):
        calls = []

        def hook(ctx):
            calls.append(("after", ctx.job.id, ctx.exception))

        job = await app.enqueue(fail_always)
        worker = make_worker(app)
        worker.add_after_process(hook)
        await worker.run()
        assert any(ev == "after" and jid == job.id for ev, jid, _ in calls)

    async def test_before_process_hook_error_does_not_abort_job(self, app, caplog):
        def bad_hook(ctx):
            raise RuntimeError("hook error")

        job = await app.enqueue(noop)
        worker = make_worker(app)
        worker.add_before_process(bad_hook)
        with caplog.at_level(logging.ERROR, logger="pgwerk.worker.base"):
            await worker.run()
        # Job should still complete despite hook error
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert any("before_process hook error" in r.message for r in caplog.records)

    async def test_after_process_hook_error_is_logged(self, app, caplog):
        def bad_hook(ctx):
            raise RuntimeError("after hook error")

        job = await app.enqueue(noop)
        worker = make_worker(app)
        worker.add_after_process(bad_hook)
        with caplog.at_level(logging.ERROR, logger="pgwerk.worker.base"):
            await worker.run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert any("after_process hook error" in r.message for r in caplog.records)
