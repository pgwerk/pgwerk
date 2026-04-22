from __future__ import annotations

from litestar.testing import AsyncTestClient

from wrk.api.app import create_app
from wrk.commons import JobStatus

from .tasks import noop


class TestApiSmoke:
    async def test_create_get_list_and_cancel_job(self, app):
        api = create_app(app)
        async with AsyncTestClient(app=api) as client:
            resp = await client.post(
                "/api/jobs",
                json={"function": "tests.integration.tasks.noop", "queue": "default", "args": [], "kwargs": {}},
            )
            assert resp.status_code == 201
            job = resp.json()

            listed = await client.get("/api/jobs")
            assert listed.status_code == 200
            assert any(item["id"] == job["id"] for item in listed.json())

            fetched = await client.get(f"/api/jobs/{job['id']}")
            assert fetched.status_code == 200
            assert fetched.json()["function"] == "tests.integration.tasks.noop"

            cancelled = await client.post(f"/api/jobs/{job['id']}/cancel")
            assert cancelled.status_code == 201
            assert cancelled.json()["cancelled"] is True

        done = await app.get_job(job["id"])
        assert done.status == JobStatus.Aborted

    async def test_stats_workers_cron_and_sweep_endpoints(self, app):
        cron_seed = await app.enqueue(noop, _cron_name="nightly.noop")

        api = create_app(app)
        async with AsyncTestClient(app=api) as client:
            workers = await client.get("/api/workers")
            assert workers.status_code == 200

            stats = await client.get("/api/stats")
            assert stats.status_code == 200
            assert stats.json()["total_jobs"] >= 1

            cron = await client.get("/api/cron")
            assert cron.status_code == 200
            assert any(item["name"] == "nightly.noop" for item in cron.json())

            trigger = await client.post("/api/cron/nightly.noop/trigger")
            assert trigger.status_code == 201
            assert trigger.json()["function"] == "tests.integration.tasks.noop"

            sweep = await client.post("/api/server/sweep")
            assert sweep.status_code == 201
            assert "swept" in sweep.json()

            server = await client.get("/api/server")
            assert server.status_code == 200
            assert server.json()["tables"]

        original = await app.get_job(cron_seed.id)
        assert original.status == JobStatus.Queued
