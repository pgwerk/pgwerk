"""Integration tests for worker concurrency limits."""

from __future__ import annotations

from .tasks import track_conc
from .tasks import _conc_state
from .tasks import track_sync_conc
from .tasks import _sync_conc_state
from .tasks import reset_conc_tracker
from .tasks import reset_sync_conc_tracker
from .conftest import make_worker

from tests.worker import ThreadWorker


def _thread_worker(app, **kwargs):
    opts = {"concurrency": 10, "burst": True, "poll_interval": 0.05, "sweep_interval": 9999, "abort_interval": 0.05}
    opts.update(kwargs)
    return ThreadWorker(app=app, queues=["default"], **opts)


class TestConcurrencyLimit:
    async def test_async_worker_never_exceeds_concurrency(self, app):
        """AsyncWorker runs at most `concurrency` async coroutines simultaneously."""
        reset_conc_tracker()
        limit = 3
        for _ in range(10):
            await app.enqueue(track_conc, seconds=0.08)

        await make_worker(app, concurrency=limit).run()

        assert _conc_state["max"] <= limit

    async def test_thread_worker_never_exceeds_concurrency(self, app):
        """ThreadWorker runs at most `concurrency` threads simultaneously."""
        reset_sync_conc_tracker()
        limit = 2
        for _ in range(8):
            await app.enqueue(track_sync_conc, seconds=0.1)

        await _thread_worker(app, concurrency=limit).run()

        assert _sync_conc_state["max"] <= limit

    async def test_async_worker_fills_concurrency_slots(self, app):
        """AsyncWorker actually uses concurrency — not always 1 at a time."""
        reset_conc_tracker()
        for _ in range(10):
            await app.enqueue(track_conc, seconds=0.1)

        await make_worker(app, concurrency=5).run()

        assert _conc_state["max"] > 1
