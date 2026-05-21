"""Integration tests for get_queue_depth_history."""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from .tasks import noop


class TestQueueDepthHistory:
    async def test_queued_jobs_appear_in_depth(self, app):
        await app.enqueue(noop)
        rows = await app.get_queue_depth_history(minutes=5)
        latest = rows[-1]
        assert latest["queued"] >= 1

    async def test_scheduled_future_jobs_excluded_from_depth(self, app):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        await app.enqueue(noop, _at=future)
        rows = await app.get_queue_depth_history(minutes=5)
        latest = rows[-1]
        assert latest["queued"] == 0, (
            "future-scheduled jobs must not inflate the queued depth"
        )

    async def test_mixed_queued_and_scheduled(self, app):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        await app.enqueue(noop)
        await app.enqueue(noop, _at=future)
        rows = await app.get_queue_depth_history(minutes=5)
        latest = rows[-1]
        assert latest["queued"] == 1, (
            "only the immediately-queued job should count toward depth"
        )
