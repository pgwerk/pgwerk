from __future__ import annotations

import asyncio
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import pytest

from pgwerk.cron import CronScheduler
from pgwerk.schemas import Schedule
from pgwerk.repos import ScheduleAlreadyExists
from pgwerk.repos import ScheduleNotFound

from .tasks import noop


# ---------------------------------------------------------------------------
# ScheduleRepository direct surface
# ---------------------------------------------------------------------------


class TestScheduleRepository:
    async def test_insert_computes_next_run_at(self, app):
        s = Schedule(name="s.interval", function="tests.integration.tasks.noop", interval_secs=3600)
        stored = await app._schedule_repo.insert(s)
        assert stored.next_run_at is not None
        assert stored.next_run_at > datetime.now(timezone.utc)

    async def test_insert_duplicate_raises(self, app):
        s = Schedule(name="dup", function="tests.integration.tasks.noop", interval_secs=60)
        await app._schedule_repo.insert(s)
        with pytest.raises(ScheduleAlreadyExists):
            await app._schedule_repo.insert(s)

    async def test_insert_rejects_both_interval_and_cron(self, app):
        s = Schedule(
            name="bad",
            function="tests.integration.tasks.noop",
            interval_secs=60,
            cron="* * * * *",
        )
        # compute_next_run() rejects the policy before the INSERT; a DB-level
        # CHECK((interval_secs IS NULL) <> (cron IS NULL)) is a second line of
        # defence if the client ever skipped that call.
        with pytest.raises(ValueError, match="not both"):
            await app._schedule_repo.insert(s)

    async def test_db_check_rejects_both_interval_and_cron(self, app):
        """Raw INSERT (bypassing compute_next_run) hits the table CHECK."""
        import psycopg
        from psycopg.sql import SQL

        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            with pytest.raises(psycopg.errors.CheckViolation):
                await conn.execute(
                    SQL("""
                        INSERT INTO {schedules} (name, function, interval_secs, cron)
                        VALUES ('db-check', 'm.f', 60, '* * * * *')
                    """).format(schedules=app._t["schedules"])
                )

    async def test_get_returns_stored_schedule(self, app):
        await app._schedule_repo.insert(
            Schedule(name="getme", function="tests.integration.tasks.noop", interval_secs=60)
        )
        fetched = await app._schedule_repo.get("getme")
        assert fetched is not None
        assert fetched.name == "getme"

    async def test_get_missing_returns_none(self, app):
        assert await app._schedule_repo.get("missing") is None

    async def test_list_all_ordered_by_name(self, app):
        for n in ("b", "a", "c"):
            await app._schedule_repo.insert(
                Schedule(name=n, function="tests.integration.tasks.noop", interval_secs=60)
            )
        rows = await app._schedule_repo.list_all()
        assert [r.name for r in rows] == ["a", "b", "c"]

    async def test_delete_removes_row(self, app):
        await app._schedule_repo.insert(
            Schedule(name="bye", function="tests.integration.tasks.noop", interval_secs=60)
        )
        assert await app._schedule_repo.delete("bye") is True
        assert await app._schedule_repo.get("bye") is None

    async def test_delete_missing_returns_false(self, app):
        assert await app._schedule_repo.delete("nope") is False


class TestScheduleRepositoryUpdate:
    async def test_update_queue_changes_queue(self, app):
        await app._schedule_repo.insert(
            Schedule(name="u", function="tests.integration.tasks.noop", interval_secs=60)
        )
        updated = await app._schedule_repo.update("u", queue="priority")
        assert updated.queue == "priority"

    async def test_update_interval_clears_cron_and_recomputes_next(self, app):
        await app._schedule_repo.insert(
            Schedule(name="u", function="tests.integration.tasks.noop", cron="0 9 * * *")
        )
        before = (await app._schedule_repo.get("u")).next_run_at
        updated = await app._schedule_repo.update("u", interval_secs=60)
        assert updated.cron is None
        assert updated.interval_secs == 60
        assert updated.next_run_at != before

    async def test_update_cron_clears_interval_and_recomputes_next(self, app):
        await app._schedule_repo.insert(
            Schedule(name="u", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        updated = await app._schedule_repo.update("u", cron="0 9 * * *")
        assert updated.interval_secs is None
        assert updated.cron == "0 9 * * *"

    async def test_update_missing_raises(self, app):
        with pytest.raises(ScheduleNotFound):
            await app._schedule_repo.update("missing", queue="x")

    async def test_update_paused_without_policy_change(self, app):
        await app._schedule_repo.insert(
            Schedule(name="u", function="tests.integration.tasks.noop", interval_secs=60)
        )
        updated = await app._schedule_repo.update("u", paused=True)
        assert updated.paused is True
        assert updated.interval_secs == 60


class TestScheduleRepositoryTick:
    async def test_tick_fires_due_schedule_and_advances_next_run(self, app):
        await app._schedule_repo.insert(
            Schedule(name="tick", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        # Make it due now.
        await app._schedule_repo.trigger("tick")

        fired: list[str] = []

        async def callback(sched: Schedule) -> None:
            fired.append(sched.name)

        count = await app._schedule_repo.tick_once(callback)
        assert count == 1
        assert fired == ["tick"]

        after = await app._schedule_repo.get("tick")
        assert after.last_run_at is not None
        assert after.next_run_at > datetime.now(timezone.utc)

    async def test_tick_skips_paused(self, app):
        await app._schedule_repo.insert(
            Schedule(name="paused", function="tests.integration.tasks.noop", interval_secs=60)
        )
        await app._schedule_repo.trigger("paused")
        await app._schedule_repo.update("paused", paused=True)

        fired: list[str] = []

        async def callback(sched: Schedule) -> None:
            fired.append(sched.name)

        count = await app._schedule_repo.tick_once(callback)
        assert count == 0
        assert fired == []

    async def test_tick_skips_when_nothing_due(self, app):
        await app._schedule_repo.insert(
            Schedule(name="future", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        # next_run_at was just computed ~3600s ahead; don't trigger.
        async def callback(sched: Schedule) -> None:
            pass  # unreachable

        count = await app._schedule_repo.tick_once(callback)
        assert count == 0

    async def test_tick_aborts_transaction_on_callback_exception(self, app):
        await app._schedule_repo.insert(
            Schedule(name="boom", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        await app._schedule_repo.trigger("boom")
        before = await app._schedule_repo.get("boom")

        async def failing(sched: Schedule) -> None:
            raise RuntimeError("callback failed")

        with pytest.raises(RuntimeError):
            await app._schedule_repo.tick_once(failing)

        # The transaction rolled back; next_run_at was not advanced.
        after = await app._schedule_repo.get("boom")
        assert after.next_run_at == before.next_run_at
        assert after.last_run_at == before.last_run_at

    async def test_seconds_until_next_due_fallback_when_empty(self, app):
        assert await app._schedule_repo.seconds_until_next_due(fallback=42.0) == 42.0

    async def test_seconds_until_next_due_nonnegative_when_overdue(self, app):
        await app._schedule_repo.insert(
            Schedule(name="overdue", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        await app._schedule_repo.trigger("overdue")
        assert await app._schedule_repo.seconds_until_next_due(fallback=60.0) == 0.0

    async def test_trigger_advances_due_now(self, app):
        await app._schedule_repo.insert(
            Schedule(name="t", function="tests.integration.tasks.noop", interval_secs=3600)
        )
        before = await app._schedule_repo.get("t")
        triggered = await app._schedule_repo.trigger("t")
        assert triggered is not None
        assert triggered.next_run_at < before.next_run_at

    async def test_trigger_missing_returns_none(self, app):
        assert await app._schedule_repo.trigger("missing") is None


class TestScheduleRepositoryReconcile:
    async def test_keep_does_nothing(self, app):
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        affected = await app._schedule_repo.reconcile([], on_unregistered="keep")
        assert affected == []
        assert (await app._schedule_repo.get("orphan")) is not None

    async def test_pause_marks_orphans_paused(self, app):
        await app._schedule_repo.insert(
            Schedule(name="keep", function="tests.integration.tasks.noop", interval_secs=60)
        )
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        affected = await app._schedule_repo.reconcile(["keep"], on_unregistered="pause")
        assert affected == ["orphan"]
        assert (await app._schedule_repo.get("orphan")).paused is True
        assert (await app._schedule_repo.get("keep")).paused is False

    async def test_pause_skips_already_paused(self, app):
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        await app._schedule_repo.update("orphan", paused=True)
        affected = await app._schedule_repo.reconcile([], on_unregistered="pause")
        assert affected == []  # already paused; no-op

    async def test_delete_removes_orphans(self, app):
        await app._schedule_repo.insert(
            Schedule(name="keep", function="tests.integration.tasks.noop", interval_secs=60)
        )
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        affected = await app._schedule_repo.reconcile(["keep"], on_unregistered="delete")
        assert affected == ["orphan"]
        assert (await app._schedule_repo.get("orphan")) is None
        assert (await app._schedule_repo.get("keep")) is not None

    async def test_rejects_invalid_policy(self, app):
        with pytest.raises(ValueError, match="Invalid on_unregistered"):
            await app._schedule_repo.reconcile([], on_unregistered="bogus")


# ---------------------------------------------------------------------------
# Job FK behavior (ON DELETE SET NULL, ON UPDATE CASCADE)
# ---------------------------------------------------------------------------


class TestScheduleJobForeignKey:
    async def test_delete_schedule_nulls_job_schedule_name(self, app):
        await app._schedule_repo.insert(
            Schedule(name="parent", function="tests.integration.tasks.noop", interval_secs=60)
        )
        job = await app.enqueue(noop, _schedule_name="parent")

        await app._schedule_repo.delete("parent")

        refreshed = await app.get_job(job.id)
        assert refreshed.schedule_name is None

    async def test_insert_job_for_missing_schedule_raises(self, app):
        import psycopg

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            await app.enqueue(noop, _schedule_name="does-not-exist")


# ---------------------------------------------------------------------------
# CronScheduler end-to-end
# ---------------------------------------------------------------------------


class TestCronSchedulerSync:
    async def test_sync_inserts_registered_schedules(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=60, name="via-sync")
        inserted, updated, reconciled = await scheduler.sync()
        assert inserted == 1
        assert updated == 0
        assert reconciled == []
        row = await app._schedule_repo.get("via-sync")
        assert row is not None
        assert row.function == "tests.integration.tasks.noop"
        assert row.interval_secs == 60

    async def test_sync_updates_on_second_registration(self, app):
        # First sync registers.
        s1 = CronScheduler(app, on_unregistered="keep")
        s1.register(noop, interval=60, name="renewed")
        await s1.sync()

        # Second sync with the same name but a different interval upserts via update().
        s2 = CronScheduler(app, on_unregistered="keep")
        s2.register(noop, interval=30, name="renewed")
        inserted, updated, _ = await s2.sync()
        assert inserted == 0
        assert updated == 1
        assert (await app._schedule_repo.get("renewed")).interval_secs == 30

    async def test_sync_pauses_orphans_by_default(self, app):
        # Pre-seed an orphan; sync without re-registering it should pause it.
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        scheduler = CronScheduler(app)  # default on_unregistered="pause"
        scheduler.register(noop, interval=60, name="keep-me")
        _, _, reconciled = await scheduler.sync()
        assert reconciled == ["orphan"]
        assert (await app._schedule_repo.get("orphan")).paused is True

    async def test_sync_deletes_orphans_when_policy_is_delete(self, app):
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        scheduler = CronScheduler(app, on_unregistered="delete")
        _, _, reconciled = await scheduler.sync()
        assert reconciled == ["orphan"]
        assert await app._schedule_repo.get("orphan") is None

    async def test_sync_keeps_orphans_when_policy_is_keep(self, app):
        await app._schedule_repo.insert(
            Schedule(name="orphan", function="tests.integration.tasks.noop", interval_secs=60)
        )
        scheduler = CronScheduler(app, on_unregistered="keep")
        _, _, reconciled = await scheduler.sync()
        assert reconciled == []
        assert (await app._schedule_repo.get("orphan")).paused is False


class TestCronSchedulerTick:
    async def test_tick_fires_and_enqueues_job(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="tick-one")
        await scheduler.sync()
        await app._schedule_repo.trigger("tick-one")

        fired = await scheduler._tick()
        assert fired == 1

        jobs = await app.list_jobs(limit=10)
        sched_jobs = [j for j in jobs if j.schedule_name == "tick-one"]
        assert len(sched_jobs) == 1
        assert sched_jobs[0].function == "tests.integration.tasks.noop"

    async def test_tick_dedups_same_bucket(self, app):
        """Two _tick() calls in the same interval bucket collapse via jobs.key UNIQUE."""
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="dedup")
        await scheduler.sync()

        await app._schedule_repo.trigger("dedup")
        await scheduler._tick()
        # Re-trigger and tick again within the same bucket: the dedup key is identical.
        await app._schedule_repo.trigger("dedup")
        await scheduler._tick()

        jobs = await app.list_jobs(limit=10)
        sched_jobs = [j for j in jobs if j.schedule_name == "dedup"]
        assert len(sched_jobs) == 1


class TestCronSchedulerCrud:
    async def test_pause_prevents_tick_from_firing(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="p")
        await scheduler.sync()
        await app._schedule_repo.trigger("p")
        await scheduler.pause("p")

        fired = await scheduler._tick()
        assert fired == 0

    async def test_resume_allows_tick_to_fire(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="r")
        await scheduler.sync()
        await scheduler.pause("r")
        await app._schedule_repo.trigger("r")
        await scheduler.resume("r")

        fired = await scheduler._tick()
        assert fired == 1

    async def test_delete_removes_row(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="d")
        await scheduler.sync()
        removed = await scheduler.delete("d")
        assert removed is True
        assert await app._schedule_repo.get("d") is None

    async def test_trigger_makes_schedule_due_now(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="t")
        await scheduler.sync()
        triggered = await scheduler.trigger("t")
        assert triggered is not None
        assert triggered.next_run_at <= datetime.now(timezone.utc)


class TestCronSchedulerImperative:
    async def test_schedule_inserts_row(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        sched = await scheduler.schedule(noop, interval=60, name="imp")
        assert sched.name == "imp"
        assert sched.interval_secs == 60
        stored = await app._schedule_repo.get("imp")
        assert stored is not None
        assert stored.next_run_at is not None

    async def test_schedule_is_upsert(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        await scheduler.schedule(noop, interval=60, name="upsert")
        updated = await scheduler.schedule(noop, interval=30, queue="priority", name="upsert")
        assert updated.interval_secs == 30
        assert updated.queue == "priority"
        assert (await app._schedule_repo.get("upsert")).interval_secs == 30

    async def test_schedule_default_name_from_function(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        sched = await scheduler.schedule(noop, interval=60)
        assert sched.name == "tests.integration.tasks.noop"

    async def test_schedule_rejects_both_interval_and_cron(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        with pytest.raises(ValueError, match="not both"):
            await scheduler.schedule(noop, interval=60, cron="* * * * *", name="x")

    async def test_schedule_at_sets_first_run_explicitly(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        when = datetime.now(timezone.utc) + timedelta(days=1)
        sched = await scheduler.schedule_at(noop, when, interval=60, name="at")
        assert sched.next_run_at is not None
        # Allow a small sub-second drift from the DB round-trip.
        assert abs((sched.next_run_at - when).total_seconds()) < 1.0

    async def test_schedule_at_re_anchors_on_reupsert(self, app):
        """Calling schedule_at() again with the same name must honor the new
        first-run time instead of silently recomputing from the policy."""
        scheduler = CronScheduler(app, on_unregistered="keep")
        first = datetime.now(timezone.utc) + timedelta(days=1)
        await scheduler.schedule_at(noop, first, interval=60, name="reanchor")

        second = datetime.now(timezone.utc) + timedelta(days=7)
        updated = await scheduler.schedule_at(noop, second, interval=60, name="reanchor")
        assert updated.next_run_at is not None
        assert abs((updated.next_run_at - second).total_seconds()) < 1.0

        stored = await app._schedule_repo.get("reanchor")
        assert abs((stored.next_run_at - second).total_seconds()) < 1.0

    async def test_schedule_at_normalizes_naive_datetime_to_utc(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        naive = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(tzinfo=None)
        sched = await scheduler.schedule_at(noop, naive, interval=60, name="at-naive")
        assert sched.next_run_at is not None
        assert sched.next_run_at.tzinfo is not None

    async def test_schedule_in_starts_after_delay(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        before = datetime.now(timezone.utc)
        sched = await scheduler.schedule_in(120, noop, interval=60, name="delay")
        assert sched.next_run_at is not None
        delta = (sched.next_run_at - before).total_seconds()
        assert 115 < delta < 125  # ~120s, allowing for clock drift & round-trip

    async def test_schedule_in_not_fired_before_delay(self, app):
        scheduler = CronScheduler(app, on_unregistered="keep")
        await scheduler.schedule_in(3600, noop, interval=60, name="future")
        fired = await scheduler._tick()
        assert fired == 0

    async def test_schedule_in_repeat_uses_policy_after_first_run(self, app):
        """After the delayed first run fires, subsequent runs follow interval."""
        scheduler = CronScheduler(app, on_unregistered="keep")
        await scheduler.schedule_in(0, noop, interval=3600, name="then-policy")
        # Due now: first tick fires it.
        fired = await scheduler._tick()
        assert fired == 1
        after = await app._schedule_repo.get("then-policy")
        # next_run_at is now ~3600s out, following the interval policy.
        delta = (after.next_run_at - datetime.now(timezone.utc)).total_seconds()
        assert 3590 < delta < 3610


class TestCronSchedulerRun:
    async def test_run_ticks_then_stops(self, app):
        """Full run() path: schedule fires at least once, then stop() unblocks.

        Uses a tight poll_interval via the app's config so the loop wakes
        promptly after trigger() makes the schedule due.
        """
        app.config.poll_interval = 0.1
        scheduler = CronScheduler(app, on_unregistered="keep")
        scheduler.register(noop, interval=3600, name="run-once")

        run_task = asyncio.create_task(scheduler.run())
        try:
            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                if await app._schedule_repo.get("run-once") is not None:
                    break
                await asyncio.sleep(0.05)
            await app._schedule_repo.trigger("run-once")

            deadline = asyncio.get_running_loop().time() + 10.0
            while asyncio.get_running_loop().time() < deadline:
                jobs = await app.list_jobs(limit=5)
                if any(j.schedule_name == "run-once" for j in jobs):
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("scheduler never enqueued the due job")
        finally:
            scheduler.stop()
            await asyncio.wait_for(run_task, timeout=5.0)
