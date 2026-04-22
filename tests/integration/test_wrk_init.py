"""Integration tests for Werk initialization."""

from __future__ import annotations

import pytest

from pgwerk.app import Werk

from .tasks import clear_callback_log


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWrkInit:
    def test_config_as_dict(self):
        app = Werk("postgresql://x/y", config={"prefix": "_test"})
        assert app.prefix == "_test"

    def test_log_level_configures_logging(self):
        Werk("postgresql://x/y", log_level="WARNING")

    def test_pool_or_raise_when_not_connected(self):
        app = Werk("postgresql://x/y")
        with pytest.raises(RuntimeError, match="Not connected"):
            app._pool_or_raise()
