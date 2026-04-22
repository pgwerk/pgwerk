"""Integration tests for Wrk initialization."""

from __future__ import annotations

import pytest

from pgwerk.app import Wrk

from .tasks import clear_callback_log


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWrkInit:
    def test_config_as_dict(self):
        app = Wrk("postgresql://x/y", config={"prefix": "_test"})
        assert app.prefix == "_test"

    def test_log_level_configures_logging(self):
        Wrk("postgresql://x/y", log_level="WARNING")

    def test_pool_or_raise_when_not_connected(self):
        app = Wrk("postgresql://x/y")
        with pytest.raises(RuntimeError, match="Not connected"):
            app._pool_or_raise()
