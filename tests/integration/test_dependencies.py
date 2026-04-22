from __future__ import annotations

from psycopg.sql import SQL

from .tasks import noop
from .tasks import fail_always
from .conftest import make_worker

from tests.commons import JobStatus
from tests.schemas import Dependency


class TestDependencies:
    async def test_dependent_starts_in_waiting(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        assert child.status == JobStatus.Waiting

    async def test_dependency_recorded(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        deps = await app.get_job_dependencies(child.id)
        assert parent.id in deps

    async def test_dependent_queued_after_parent_completes(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)

        await make_worker(app).run()

        assert (await app.get_job(parent.id)).status == JobStatus.Complete
        assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_multiple_dependents_all_unblocked(self, app):
        parent = await app.enqueue(noop)
        children = [await app.enqueue(noop, _depends_on=parent) for _ in range(3)]

        await make_worker(app).run()

        for child in children:
            assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_chained_dependencies(self, app):
        a = await app.enqueue(noop)
        b = await app.enqueue(noop, _depends_on=a)
        c = await app.enqueue(noop, _depends_on=b)

        await make_worker(app).run()

        assert (await app.get_job(a.id)).status == JobStatus.Complete
        assert (await app.get_job(b.id)).status == JobStatus.Complete
        assert (await app.get_job(c.id)).status == JobStatus.Complete

    async def test_multiple_parents_waits_for_all(self, app):
        p1 = await app.enqueue(noop)
        p2 = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=[p1, p2])
        assert child.status == JobStatus.Waiting

        await make_worker(app).run()

        assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_dependent_fails_when_parent_fails(self, app):
        parent = await app.enqueue(fail_always)
        child = await app.enqueue(noop, _depends_on=parent)

        await make_worker(app).run()

        assert (await app.get_job(parent.id)).status == JobStatus.Failed
        child_done = await app.get_job(child.id)
        assert child_done.status == JobStatus.Failed
        assert "dependency failed" in child_done.error

    async def test_allow_failure_unblocks_dependent(self, app):
        parent = await app.enqueue(fail_always)
        child = await app.enqueue(noop, _depends_on=Dependency(job=parent, allow_failure=True))

        await make_worker(app).run()

        assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_allow_failure_false_blocks_on_failure(self, app):
        parent = await app.enqueue(fail_always)
        child = await app.enqueue(noop, _depends_on=Dependency(job=parent, allow_failure=False))

        await make_worker(app).run()

        assert (await app.get_job(child.id)).status == JobStatus.Failed

    async def test_cancelled_parent_fails_dependent(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)

        await app.cancel_job(parent.id)
        await make_worker(app).run()

        assert (await app.get_job(child.id)).status == JobStatus.Failed

    async def test_mixed_allow_failure(self, app):
        p_fail = await app.enqueue(fail_always)
        p_ok = await app.enqueue(noop)
        child = await app.enqueue(
            noop,
            _depends_on=[
                Dependency(job=p_fail, allow_failure=True),
                Dependency(job=p_ok, allow_failure=False),
            ],
        )

        await make_worker(app).run()

        assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_sweep_failure_settles_waiting_dependents(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = max_attempts,
                        timeout_secs = 1,
                        started_at = NOW() - INTERVAL '10 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": parent.id},
            )

        await app.sweep()

        parent_done = await app.get_job(parent.id)
        child_done = await app.get_job(child.id)
        assert parent_done.status == JobStatus.Failed
        assert child_done.status == JobStatus.Failed
        assert "dependency failed" in child_done.error

    async def test_mutual_dependency_stays_waiting(self, app):
        a = await app.enqueue(noop)
        b = await app.enqueue(noop, _depends_on=a)

        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    INSERT INTO {deps} (job_id, depends_on, allow_failure)
                    VALUES (%(jid)s, %(dep)s, false)
                    ON CONFLICT DO NOTHING
                """).format(deps=app._t["deps"]),
                {"jid": a.id, "dep": b.id},
            )
            await conn.execute(
                SQL("UPDATE {jobs} SET status = 'waiting' WHERE id = %(id)s").format(jobs=app._t["jobs"]),
                {"id": a.id},
            )

        await make_worker(app).run()

        assert (await app.get_job(a.id)).status == JobStatus.Waiting
        assert (await app.get_job(b.id)).status == JobStatus.Waiting

    async def test_self_dependency_stays_waiting(self, app):
        a = await app.enqueue(noop)
        pool = app._pool_or_raise()
        async with pool.connection() as conn:
            await conn.execute(
                SQL("""
                    INSERT INTO {deps} (job_id, depends_on, allow_failure)
                    VALUES (%(jid)s, %(dep)s, false)
                    ON CONFLICT DO NOTHING
                """).format(deps=app._t["deps"]),
                {"jid": a.id, "dep": a.id},
            )
            await conn.execute(
                SQL("UPDATE {jobs} SET status = 'waiting' WHERE id = %(id)s").format(jobs=app._t["jobs"]),
                {"id": a.id},
            )

        await make_worker(app).run()

        assert (await app.get_job(a.id)).status == JobStatus.Waiting
