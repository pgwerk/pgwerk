"""Integration tests for lifecycle hooks."""

from __future__ import annotations

import pytest

from pgwerk.app import Wrk

from .tasks import clear_callback_log
from .conftest import _TEST_DSN
from .conftest import _TEST_PREFIX


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestLifecycleHooks:
    async def test_on_startup_called_on_connect(self):
        calls = []
        a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX + "_hooks")
        a.on_startup(lambda: calls.append("startup"))
        await a.connect()
        try:
            assert "startup" in calls
        finally:
            await a.disconnect()

    async def test_on_shutdown_called_on_disconnect(self):
        calls = []
        a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX + "_hooks")
        a.on_shutdown(lambda: calls.append("shutdown"))
        await a.connect()
        await a.disconnect()
        assert "shutdown" in calls

    async def test_async_on_startup_hook(self):
        calls = []

        async def hook():
            calls.append("async_startup")

        a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX + "_hooks")
        a.on_startup(hook)
        await a.connect()
        try:
            assert "async_startup" in calls
        finally:
            await a.disconnect()

    async def test_context_manager_connect_disconnect(self):
        calls = []
        a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX + "_ctx")
        a.on_startup(lambda: calls.append("up"))
        a.on_shutdown(lambda: calls.append("down"))
        async with a:
            assert "up" in calls
        assert "down" in calls

    async def test_double_connect_is_idempotent(self):
        a = Wrk(_TEST_DSN, prefix=_TEST_PREFIX + "_dbl")
        await a.connect()
        pool1 = a._pool
        await a.connect()
        assert a._pool is pool1
        await a.disconnect()
