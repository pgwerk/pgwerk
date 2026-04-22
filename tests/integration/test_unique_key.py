"""Integration tests for unique job key deduplication."""

from __future__ import annotations

import asyncio

from pgwerk.commons import JobStatus

from .tasks import noop
from .conftest import make_worker


class TestUniqueKey:
    async def test_concurrent_same_key_only_one_inserted(self, app):
        """Concurrent enqueue calls with the same _key produce exactly one job."""
        key = "concurrent-unique-key"
        results = await asyncio.gather(*[app.enqueue(noop, _key=key) for _ in range(8)])
        successful = [r for r in results if r is not None]
        assert len(successful) == 1

    async def test_unique_key_remains_reserved_after_completion(self, app):
        """A completed job's key is still in the DB — the same key cannot be re-enqueued."""
        job1 = await app.enqueue(noop, _key="held-key")
        assert job1 is not None
        await make_worker(app).run()
        assert (await app.get_job(job1.id)).status == JobStatus.Complete

        job2 = await app.enqueue(noop, _key="held-key")
        assert job2 is None
