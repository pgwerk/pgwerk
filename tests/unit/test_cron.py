from __future__ import annotations

from datetime import datetime
from datetime import timezone
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import AsyncMock

import pytest


croniter = pytest.importorskip("croniter", reason="croniter not installed")

from pgwerk.cron import CronScheduler  # noqa: E402
from pgwerk.cron import ON_UNREGISTERED  # noqa: E402
from pgwerk.schemas import Schedule  # noqa: E402
from pgwerk.utils import compute_next_run  # noqa: E402
from pgwerk.utils import schedule_tick_key  # noqa: E402


def _noop():
    pass


def _noop2():
    pass


# ---------------------------------------------------------------------------
# compute_next_run
# ---------------------------------------------------------------------------


class TestComputeNextRun:
    def test_rejects_both_interval_and_cron(self):
        with pytest.raises(ValueError, match="not both"):
            compute_next_run(interval_secs=60, cron="* * * * *")

    def test_rejects_neither(self):
        with pytest.raises(ValueError, match="not both"):
            compute_next_run(interval_secs=None, cron=None)

    def test_interval_adds_seconds_to_base(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert compute_next_run(60, None, base=base) == base + timedelta(seconds=60)

    def test_cron_advances_past_base(self):
        base = datetime(2026, 1, 1, 8, 30, tzinfo=timezone.utc)
        nxt = compute_next_run(None, "0 9 * * *", base=base)
        assert nxt > base

    def test_cron_result_matches_expression(self):
        base = datetime(2026, 1, 1, 8, 30, tzinfo=timezone.utc)
        nxt = compute_next_run(None, "0 9 * * *", base=base)
        assert nxt == datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)

    def test_interval_defaults_to_now(self):
        before = datetime.now(timezone.utc)
        nxt = compute_next_run(60, None)
        after = datetime.now(timezone.utc)
        assert before + timedelta(seconds=60) - timedelta(seconds=1) <= nxt
        assert nxt <= after + timedelta(seconds=60) + timedelta(seconds=1)

    def test_naive_base_is_treated_as_utc(self):
        base = datetime(2026, 1, 1, 0, 0)  # no tzinfo
        nxt = compute_next_run(60, None, base=base)
        assert nxt.tzinfo is not None
        assert nxt == datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# schedule_tick_key
# ---------------------------------------------------------------------------


class TestScheduleTickKey:
    def test_interval_bucket_is_stable_within_bucket(self):
        s = Schedule(name="x", function="m.f", interval_secs=3600)
        assert schedule_tick_key(s) == schedule_tick_key(s)

    def test_interval_bucket_uses_name(self):
        a = Schedule(name="a", function="m.f", interval_secs=60)
        b = Schedule(name="b", function="m.f", interval_secs=60)
        assert schedule_tick_key(a) != schedule_tick_key(b)

    def test_cron_key_uses_next_run_at(self):
        t = datetime(2026, 5, 8, 9, 0, tzinfo=timezone.utc)
        s = Schedule(name="x", function="m.f", cron="0 9 * * *", next_run_at=t)
        key = schedule_tick_key(s)
        assert "cron" in key
        assert t.isoformat() in key

    def test_cron_key_naive_next_run_normalized_to_utc(self):
        naive = datetime(2026, 5, 8, 9, 0)
        s = Schedule(name="x", function="m.f", cron="0 9 * * *", next_run_at=naive)
        key = schedule_tick_key(s)
        assert "+00:00" in key

    def test_cron_fallback_when_next_run_missing(self):
        s = Schedule(name="x", function="m.f", cron="0 9 * * *", next_run_at=None)
        key = schedule_tick_key(s)
        assert "fallback" in key


# ---------------------------------------------------------------------------
# CronScheduler: construction & validation
# ---------------------------------------------------------------------------


class TestCronSchedulerInit:
    def test_rejects_invalid_on_unregistered(self):
        with pytest.raises(ValueError, match="on_unregistered"):
            CronScheduler(MagicMock(), on_unregistered="bogus")

    def test_default_policy_is_pause(self):
        assert CronScheduler(MagicMock()).on_unregistered == "pause"

    def test_accepts_keep_pause_delete(self):
        for policy in ON_UNREGISTERED:
            assert CronScheduler(MagicMock(), on_unregistered=policy).on_unregistered == policy

    def test_has_unique_id(self):
        a = CronScheduler(MagicMock())
        b = CronScheduler(MagicMock())
        assert a.id != b.id

    def test_name_includes_pid(self):
        import os

        s = CronScheduler(MagicMock())
        assert str(os.getpid()) in s.name


# ---------------------------------------------------------------------------
# CronScheduler: register / unregister
# ---------------------------------------------------------------------------


class TestCronSchedulerRegister:
    def test_register_stages_in_memory(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(_noop, interval=60)
        assert len(scheduler._pending) == 1
        sched = next(iter(scheduler._pending.values()))
        assert sched.interval_secs == 60
        assert sched.cron is None
        assert sched.function.endswith("._noop")

    def test_register_returns_the_function(self):
        scheduler = CronScheduler(MagicMock())
        returned = scheduler.register(_noop, interval=60)
        assert returned is _noop

    def test_register_supports_decorator_call_shape(self):
        scheduler = CronScheduler(MagicMock())
        decorator = scheduler.register(interval=60, name="decorated")
        assert callable(decorator)
        result = decorator(_noop)
        assert result is _noop
        assert "decorated" in scheduler._pending

    def test_register_rejects_both_interval_and_cron(self):
        scheduler = CronScheduler(MagicMock())
        with pytest.raises(ValueError, match="not both"):
            scheduler.register(_noop, interval=60, cron="* * * * *")

    def test_register_rejects_neither(self):
        scheduler = CronScheduler(MagicMock())
        with pytest.raises(ValueError, match="not both"):
            scheduler.register(_noop)

    def test_register_duplicate_name_raises(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(_noop, interval=60, name="dup")
        with pytest.raises(ValueError, match="already registered"):
            scheduler.register(_noop2, interval=120, name="dup")

    def test_register_explicit_name(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(_noop, interval=60, name="my-schedule")
        assert "my-schedule" in scheduler._pending

    def test_register_stores_args_kwargs_queue_meta(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(
            _noop, interval=60, queue="q1", args=(1, 2), kwargs={"k": "v"}, meta={"env": "test"}
        )
        sched = next(iter(scheduler._pending.values()))
        assert sched.queue == "q1"
        assert sched.args == [1, 2]
        assert sched.kwargs == {"k": "v"}
        assert sched.meta == {"env": "test"}

    def test_register_stores_timeout_and_ttls(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(
            _noop, interval=60, timeout=30, result_ttl=3600, failure_ttl=86400
        )
        sched = next(iter(scheduler._pending.values()))
        assert sched.timeout_secs == 30
        assert sched.result_ttl == 3600
        assert sched.failure_ttl == 86400

    def test_register_rejects_lambda(self):
        scheduler = CronScheduler(MagicMock())
        with pytest.raises(ValueError, match="module-level"):
            scheduler.register(lambda: None, interval=60)

    def test_unregister_removes_from_pending(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.register(_noop, interval=60, name="x")
        scheduler.unregister("x")
        assert "x" not in scheduler._pending

    def test_unregister_missing_is_noop(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.unregister("nope")  # must not raise


# ---------------------------------------------------------------------------
# CronScheduler: sync()
# ---------------------------------------------------------------------------


class TestCronSchedulerSync:
    async def test_sync_inserts_new_and_reconciles(self):
        """Fresh registrations insert; orphans are reconciled per policy."""
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.insert = AsyncMock()
        app._schedule_repo.update = AsyncMock()
        app._schedule_repo.reconcile = AsyncMock(return_value=["orphan-a"])

        scheduler = CronScheduler(app, on_unregistered="pause")
        scheduler.register(_noop, interval=60, name="known")

        inserted, updated, reconciled = await scheduler.sync()
        assert inserted == 1
        assert updated == 0
        assert reconciled == ["orphan-a"]
        app._schedule_repo.reconcile.assert_awaited_once_with(["known"], on_unregistered="pause")

    async def test_sync_falls_back_to_update_on_conflict(self):
        from pgwerk.repos import ScheduleAlreadyExists

        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.insert = AsyncMock(side_effect=ScheduleAlreadyExists("dup"))
        app._schedule_repo.update = AsyncMock()
        app._schedule_repo.reconcile = AsyncMock(return_value=[])

        scheduler = CronScheduler(app)
        scheduler.register(_noop, interval=60, name="known")

        inserted, updated, _ = await scheduler.sync()
        assert inserted == 0
        assert updated == 1
        app._schedule_repo.update.assert_awaited_once()

    async def test_sync_passes_known_names_to_reconcile(self):
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.insert = AsyncMock()
        app._schedule_repo.reconcile = AsyncMock(return_value=[])

        scheduler = CronScheduler(app, on_unregistered="delete")
        scheduler.register(_noop, interval=60, name="a")
        scheduler.register(_noop2, interval=120, name="b")
        await scheduler.sync()
        app._schedule_repo.reconcile.assert_awaited_once_with(
            ["a", "b"], on_unregistered="delete"
        )


# ---------------------------------------------------------------------------
# CronScheduler: update / pause / resume / delete / trigger / list / get
# ---------------------------------------------------------------------------


class TestCronSchedulerCrud:
    def _setup(self):
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.update = AsyncMock()
        app._schedule_repo.delete = AsyncMock(return_value=True)
        app._schedule_repo.trigger = AsyncMock()
        app._schedule_repo.list_all = AsyncMock(return_value=[])
        app._schedule_repo.get = AsyncMock(return_value=None)
        return app

    async def test_update_translates_interval_kwarg(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.update("x", interval=300)
        app._schedule_repo.update.assert_awaited_once_with("x", interval_secs=300)

    async def test_update_translates_timeout_kwarg(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.update("x", timeout=10)
        app._schedule_repo.update.assert_awaited_once_with("x", timeout_secs=10)

    async def test_update_passes_through_other_fields(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.update("x", queue="q2", cron="* * * * *", paused=True)
        app._schedule_repo.update.assert_awaited_once_with(
            "x", queue="q2", cron="* * * * *", paused=True
        )

    async def test_pause_sets_paused_true(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.pause("x")
        app._schedule_repo.update.assert_awaited_once_with("x", paused=True)

    async def test_resume_sets_paused_false(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.resume("x")
        app._schedule_repo.update.assert_awaited_once_with("x", paused=False)

    async def test_delete_proxies_to_repo(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        result = await scheduler.delete("x")
        assert result is True
        app._schedule_repo.delete.assert_awaited_once_with("x")

    async def test_trigger_proxies_to_repo(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.trigger("x")
        app._schedule_repo.trigger.assert_awaited_once_with("x")

    async def test_list_schedules_proxies(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.list_schedules()
        app._schedule_repo.list_all.assert_awaited_once()

    async def test_get_proxies(self):
        app = self._setup()
        scheduler = CronScheduler(app)
        await scheduler.get("x")
        app._schedule_repo.get.assert_awaited_once_with("x")


# ---------------------------------------------------------------------------
# CronScheduler: tick
# ---------------------------------------------------------------------------


class TestCronSchedulerTick:
    async def test_tick_returns_count_from_repo(self):
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.tick_once = AsyncMock(return_value=3)
        scheduler = CronScheduler(app)
        assert await scheduler._tick() == 3

    async def test_tick_swallows_errors_and_returns_zero(self):
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.tick_once = AsyncMock(side_effect=RuntimeError("db down"))
        scheduler = CronScheduler(app)
        assert await scheduler._tick() == 0

    async def test_tick_passes_enqueue_due_callback(self):
        app = MagicMock()
        app._schedule_repo = MagicMock()
        app._schedule_repo.tick_once = AsyncMock(return_value=0)
        scheduler = CronScheduler(app)
        await scheduler._tick()
        app._schedule_repo.tick_once.assert_awaited_once_with(scheduler._enqueue_due)

    async def test_enqueue_due_calls_app_enqueue(self):
        app = MagicMock()
        app.enqueue = AsyncMock()
        scheduler = CronScheduler(app)
        sched = Schedule(
            name="x",
            function="m.f",
            queue="q",
            args=[1],
            kwargs={"k": "v"},
            interval_secs=60,
            timeout_secs=30,
            result_ttl=3600,
            failure_ttl=7200,
            meta={"a": 1},
        )
        await scheduler._enqueue_due(sched)
        app.enqueue.assert_awaited_once()
        call_args = app.enqueue.call_args
        assert call_args.args[0] == "m.f"
        assert call_args.args[1] == 1  # *args
        assert call_args.kwargs["_queue"] == "q"
        assert call_args.kwargs["_timeout"] == 30
        assert call_args.kwargs["_result_ttl"] == 3600
        assert call_args.kwargs["_failure_ttl"] == 7200
        assert call_args.kwargs["_meta"] == {"a": 1}
        assert call_args.kwargs["_schedule_name"] == "x"
        assert call_args.kwargs["k"] == "v"

    async def test_enqueue_due_sets_dedupe_key(self):
        app = MagicMock()
        app.enqueue = AsyncMock()
        scheduler = CronScheduler(app)
        sched = Schedule(name="x", function="m.f", interval_secs=60)
        await scheduler._enqueue_due(sched)
        assert app.enqueue.call_args.kwargs["_key"].startswith("_pgwerk_sched:x:interval:")


# ---------------------------------------------------------------------------
# CronScheduler: stop
# ---------------------------------------------------------------------------


class TestCronSchedulerStop:
    def test_stop_sets_running_false(self):
        scheduler = CronScheduler(MagicMock())
        scheduler._running = True
        scheduler.stop()
        assert scheduler._running is False

    def test_stop_is_idempotent(self):
        scheduler = CronScheduler(MagicMock())
        scheduler.stop()
        scheduler.stop()
        assert scheduler._running is False


# ---------------------------------------------------------------------------
# Schedule dataclass
# ---------------------------------------------------------------------------


class TestScheduleDataclass:
    def test_defaults(self):
        s = Schedule(name="x", function="m.f", interval_secs=60)
        assert s.queue == "default"
        assert s.args == []
        assert s.kwargs == {}
        assert s.paused is False
        assert s.meta is None
        assert s.next_run_at is None

    def test_stores_cron_expression(self):
        s = Schedule(name="x", function="m.f", cron="*/5 * * * *")
        assert s.cron == "*/5 * * * *"
        assert s.interval_secs is None

    def test_from_row_decodes_args_kwargs_meta(self):
        from pgwerk.serializers import JSONSerializer
        from pgwerk.serializers import encode

        ser = JSONSerializer()
        row = {
            "name": "x",
            "function": "m.f",
            "queue": "default",
            "args": encode(ser, [1, 2]),
            "kwargs": encode(ser, {"k": "v"}),
            "interval_secs": 60,
            "cron": None,
            "timeout_secs": None,
            "result_ttl": None,
            "failure_ttl": None,
            "meta": encode(ser, {"env": "prod"}),
            "paused": False,
            "next_run_at": None,
            "last_run_at": None,
            "last_registered_at": None,
            "created_at": None,
        }
        s = Schedule.from_row(row, ser)
        assert s.args == [1, 2]
        assert s.kwargs == {"k": "v"}
        assert s.meta == {"env": "prod"}

    def test_from_row_empty_args_kwargs_default_to_empty_containers(self):
        from pgwerk.serializers import JSONSerializer

        ser = JSONSerializer()
        row = {
            "name": "x",
            "function": "m.f",
            "queue": "default",
            "args": None,
            "kwargs": None,
            "interval_secs": 60,
            "cron": None,
            "timeout_secs": None,
            "result_ttl": None,
            "failure_ttl": None,
            "meta": None,
            "paused": False,
            "next_run_at": None,
            "last_run_at": None,
            "last_registered_at": None,
            "created_at": None,
        }
        s = Schedule.from_row(row, ser)
        assert s.args == []
        assert s.kwargs == {}
        assert s.meta is None
