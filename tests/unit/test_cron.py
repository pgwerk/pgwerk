from __future__ import annotations

from datetime import datetime
from datetime import timezone
from datetime import timedelta
from unittest.mock import MagicMock

import pytest


croniter = pytest.importorskip("croniter", reason="croniter not installed")

from tests.cron import CronJob
from tests.cron import CronScheduler


class TestCronJobValidation:
    def test_both_interval_and_cron_raises(self):
        with pytest.raises(ValueError, match="not both"):
            CronJob(func=lambda: None, interval=60, cron="* * * * *")

    def test_neither_interval_nor_cron_raises(self):
        with pytest.raises(ValueError, match="either interval or cron"):
            CronJob(func=lambda: None)

    def test_interval_only_ok(self):
        cj = CronJob(func=lambda: None, interval=60)
        assert cj.interval == 60

    def test_cron_only_ok(self):
        cj = CronJob(func=lambda: None, cron="* * * * *")
        assert cj.cron == "* * * * *"
        assert cj.next_run_at is not None


class TestCronJobShouldRun:
    def test_interval_never_run_should_run(self):
        cj = CronJob(func=lambda: None, interval=60)
        assert cj.should_run() is True

    def test_interval_recently_run_should_not(self):
        cj = CronJob(func=lambda: None, interval=3600)
        cj.last_run_at = datetime.now(timezone.utc)
        assert cj.should_run() is False

    def test_interval_overdue_should_run(self):
        cj = CronJob(func=lambda: None, interval=60)
        cj.last_run_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        assert cj.should_run() is True

    def test_cron_past_next_run_should_run(self):
        cj = CronJob(func=lambda: None, cron="* * * * *")
        cj.next_run_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        assert cj.should_run() is True

    def test_cron_future_next_run_should_not(self):
        cj = CronJob(func=lambda: None, cron="* * * * *")
        cj.next_run_at = datetime.now(timezone.utc) + timedelta(seconds=120)
        assert cj.should_run() is False


class TestCronJobSecondsUntilNext:
    def test_interval_never_run_returns_zero(self):
        cj = CronJob(func=lambda: None, interval=60)
        assert cj.seconds_until_next() == 0.0

    def test_interval_returns_remaining(self):
        cj = CronJob(func=lambda: None, interval=100)
        cj.last_run_at = datetime.now(timezone.utc) - timedelta(seconds=40)
        remaining = cj.seconds_until_next()
        assert 50 < remaining <= 60

    def test_interval_overdue_returns_zero(self):
        cj = CronJob(func=lambda: None, interval=60)
        cj.last_run_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        assert cj.seconds_until_next() == 0.0


class TestCronJobMarkEnqueued:
    def test_sets_last_run_at(self):
        cj = CronJob(func=lambda: None, interval=60)
        cj.mark_enqueued()
        assert cj.last_run_at is not None

    def test_cron_advances_next_run(self):
        cj = CronJob(func=lambda: None, cron="* * * * *")
        first_next = cj.next_run_at
        cj.mark_enqueued()
        assert cj.next_run_at >= first_next


class TestCronSchedulerRegister:
    def test_register_adds_job(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        assert cj in scheduler._jobs.values()
        assert len(scheduler._jobs) == 1

    def test_sleep_seconds_no_jobs(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        assert scheduler._sleep_seconds() == 60.0


class TestCronJobSecondsUntilNextCron:
    def test_cron_future_returns_positive(self):
        from datetime import datetime
        from datetime import timezone
        from datetime import timedelta

        cj = CronJob(func=lambda: None, cron="* * * * *")
        cj.next_run_at = datetime.now(timezone.utc) + timedelta(seconds=30)
        remaining = cj.seconds_until_next()
        assert remaining > 0

    def test_cron_no_next_run_returns_60(self):
        cj = CronJob(func=lambda: None, cron="* * * * *")
        cj.next_run_at = None
        assert cj.seconds_until_next() == 60.0

    def test_cron_overdue_returns_zero(self):
        from datetime import datetime
        from datetime import timezone
        from datetime import timedelta

        cj = CronJob(func=lambda: None, cron="* * * * *")
        cj.next_run_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        assert cj.seconds_until_next() == 0.0


class TestCronSchedulerStop:
    def test_stop_sets_running_false(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        scheduler._running = True
        scheduler.stop()
        assert scheduler._running is False


class TestCronSchedulerRegisterCronJobInstance:
    def test_register_cronjob_instance_directly(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = CronJob(func=lambda: None, interval=60)
        result = scheduler.register(cj)
        assert result is cj
        assert cj in scheduler._jobs.values()

    def test_register_returns_cronjob(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        result = scheduler.register(lambda: None, interval=60)
        assert isinstance(result, CronJob)

    def test_sleep_seconds_uses_minimum(self):
        from datetime import datetime
        from datetime import timezone

        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj1 = CronJob(func=lambda: None, interval=5)
        cj2 = CronJob(func=lambda: None, interval=3600)
        cj1.last_run_at = datetime.now(timezone.utc)
        cj2.last_run_at = datetime.now(timezone.utc)
        scheduler._jobs = {cj1.name: cj1, cj2.name: cj2}
        sleep_secs = scheduler._sleep_seconds()
        assert sleep_secs <= 60.0


class TestCronSchedulerTick:
    async def test_tick_enqueues_due_job(self):
        from unittest.mock import AsyncMock

        app = MagicMock()
        app.prefix = "_pgwerk"
        app.enqueue = AsyncMock()
        scheduler = CronScheduler(app)
        cj = CronJob(func=lambda: None, interval=60)
        scheduler._jobs[cj.name] = cj

        await scheduler._tick()

        app.enqueue.assert_called_once()
        assert cj.last_run_at is not None

    async def test_tick_skips_not_due_job(self):
        from datetime import datetime
        from datetime import timezone
        from unittest.mock import AsyncMock

        app = MagicMock()
        app.prefix = "_pgwerk"
        app.enqueue = AsyncMock()
        scheduler = CronScheduler(app)
        cj = CronJob(func=lambda: None, interval=3600)
        cj.last_run_at = datetime.now(timezone.utc)
        scheduler._jobs[cj.name] = cj

        await scheduler._tick()

        app.enqueue.assert_not_called()

    async def test_tick_handles_enqueue_error(self):
        from unittest.mock import AsyncMock

        app = MagicMock()
        app.prefix = "_pgwerk"
        app.enqueue = AsyncMock(side_effect=RuntimeError("db down"))
        scheduler = CronScheduler(app)
        cj = CronJob(func=lambda: None, interval=60)
        scheduler._jobs[cj.name] = cj

        await scheduler._tick()


class TestCronSchedulerDynamicControl:
    def test_unregister_removes_job(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        assert len(scheduler) == 1
        removed = scheduler.unregister(cj.name)
        assert removed is cj
        assert len(scheduler) == 0

    def test_unregister_missing_raises(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        with pytest.raises(KeyError):
            scheduler.unregister("nonexistent")

    def test_pause_prevents_run(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        scheduler.pause(cj.name)
        assert cj.paused is True
        assert cj.should_run() is False

    def test_resume_allows_run(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        scheduler.pause(cj.name)
        scheduler.resume(cj.name)
        assert cj.paused is False
        assert cj.should_run() is True

    def test_jobs_property_returns_copy(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        snapshot = scheduler.jobs
        assert cj.name in snapshot
        # Mutating the snapshot doesn't affect the scheduler.
        del snapshot[cj.name]
        assert len(scheduler) == 1

    def test_get_returns_job_or_none(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        assert scheduler.get(cj.name) is cj
        assert scheduler.get("missing") is None

    def test_contains(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        assert cj.name in scheduler
        assert "missing" not in scheduler

    def test_named_registration(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60, name="my_job")
        assert cj.name == "my_job"
        assert "my_job" in scheduler

    def test_register_replaces_existing_name(self):
        app = MagicMock()
        app.prefix = "_pgwerk"
        scheduler = CronScheduler(app)
        cj1 = scheduler.register(lambda: None, interval=60, name="job")
        cj2 = scheduler.register(lambda: None, interval=120, name="job")
        assert len(scheduler) == 1
        assert scheduler.get("job") is cj2
        assert cj1 is not cj2

    async def test_tick_skips_paused_job(self):
        from unittest.mock import AsyncMock

        app = MagicMock()
        app.prefix = "_pgwerk"
        app.enqueue = AsyncMock()
        scheduler = CronScheduler(app)
        cj = scheduler.register(lambda: None, interval=60)
        scheduler.pause(cj.name)
        await scheduler._tick()
        app.enqueue.assert_not_called()
