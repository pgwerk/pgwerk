from __future__ import annotations

from litestar.testing import AsyncTestClient

from pgwerk.api.app import create_app
from pgwerk.commons import JobStatus
from pgwerk.config import WerkConfig

from .tasks import noop
from .conftest import make_worker


class TestApiSmoke:
    async def test_create_get_list_and_cancel_job(self, app):
        api = create_app(WerkConfig(dsn=app.dsn, prefix=app.prefix))
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

    async def test_stats_workers_schedule_and_sweep_endpoints(self, app):
        # Create the schedule first so the FK from jobs.schedule_name is satisfied.
        from pgwerk.schemas import Schedule

        await app._schedule_repo.insert(
            Schedule(
                name="nightly.noop",
                function="tests.integration.tasks.noop",
                queue="default",
                interval_secs=3600,
            )
        )
        await app.enqueue(noop, _schedule_name="nightly.noop")
        test_worker = make_worker(app)
        await test_worker._register()

        api = create_app(WerkConfig(dsn=app.dsn, prefix=app.prefix))
        async with AsyncTestClient(app=api) as client:
            workers = await client.get("/api/workers")
            assert workers.status_code == 200
            worker_rows = workers.json()
            assert worker_rows
            worker_id = worker_rows[0]["id"]

            worker_detail = await client.get(f"/api/workers/{worker_id}")
            assert worker_detail.status_code == 200
            assert worker_detail.json()["id"] == worker_id

            worker_jobs = await client.get(f"/api/workers/{worker_id}/jobs")
            assert worker_jobs.status_code == 200

            stats = await client.get("/api/stats")
            assert stats.status_code == 200
            assert stats.json()["total_jobs"] >= 1

            schedules = await client.get("/api/schedules")
            assert schedules.status_code == 200
            assert any(item["name"] == "nightly.noop" for item in schedules.json())

            stats = await client.get("/api/schedules/stats")
            assert stats.status_code == 200
            assert any(item["name"] == "nightly.noop" for item in stats.json())

            detail = await client.get("/api/schedules/nightly.noop")
            assert detail.status_code == 200
            assert detail.json()["interval_secs"] == 3600

            detail_missing = await client.get("/api/schedules/does-not-exist")
            assert detail_missing.status_code == 404

            trigger = await client.post("/api/schedules/nightly.noop/trigger")
            assert trigger.status_code == 201
            assert trigger.json()["name"] == "nightly.noop"
            assert trigger.json()["next_run_at"] is not None

            sweep = await client.post("/api/server/sweep")
            assert sweep.status_code == 201
            assert "swept" in sweep.json()

            server = await client.get("/api/server")
            assert server.status_code == 200
            assert server.json()["tables"]

        await test_worker._deregister()

    async def test_enqueue_with_missing_schedule_returns_404(self, app):
        api = create_app(WerkConfig(dsn=app.dsn, prefix=app.prefix))
        async with AsyncTestClient(app=api) as client:
            resp = await client.post(
                "/api/jobs",
                json={
                    "function": "tests.integration.tasks.noop",
                    "queue": "default",
                    "args": [],
                    "kwargs": {},
                    "schedule_name": "does-not-exist",
                },
            )
            assert resp.status_code == 404
            assert "does-not-exist" in resp.json()["detail"]
