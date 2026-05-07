from __future__ import annotations

import pytest

from litestar import Litestar

from pgwerk.api.app import create_app
from pgwerk.api.spa import init_spa
from pgwerk.api.deps import init_werk
from pgwerk.api.routes import SpaController
from pgwerk.api.routes import make_router
from pgwerk.api.exporter import get_exporter
from pgwerk.api.exporter import init_exporter


# ---------------------------------------------------------------------------
# init_werk
# ---------------------------------------------------------------------------


class TestInitWerk:
    def test_registers_werk_dependency(self):
        state, deps, startup, shutdown = {}, {}, [], []
        init_werk("postgresql://localhost/test", None, None, state, deps, startup, shutdown)
        assert "werk" in deps

    def test_registers_one_startup_and_one_shutdown_hook(self):
        state, deps, startup, shutdown = {}, {}, [], []
        init_werk("postgresql://localhost/test", None, None, state, deps, startup, shutdown)
        assert len(startup) == 1
        assert len(shutdown) == 1

    async def test_shutdown_is_noop_when_werk_not_in_state(self):
        state, deps, startup, shutdown = {}, {}, [], []
        init_werk("postgresql://localhost/test", None, None, state, deps, startup, shutdown)
        # should not raise even though state["werk"] was never set
        await shutdown[0]()


# ---------------------------------------------------------------------------
# init_exporter
# ---------------------------------------------------------------------------


class TestInitExporter:
    @pytest.fixture(autouse=True)
    def _clear_exporter_state(self):
        import pgwerk.api.exporter as _mod

        _mod._state.clear()
        yield
        _mod._state.clear()

    async def test_get_exporter_returns_none_before_start(self):
        assert await get_exporter() is None

    def test_registers_one_startup_and_one_shutdown_hook(self):
        startup, shutdown = [], []
        init_exporter(15.0, {}, startup, shutdown)
        assert len(startup) == 1
        assert len(shutdown) == 1

    async def test_shutdown_is_noop_when_exporter_not_started(self):
        startup, shutdown = [], []
        init_exporter(15.0, {}, startup, shutdown)
        await shutdown[0]()  # should not raise


# ---------------------------------------------------------------------------
# init_spa
# ---------------------------------------------------------------------------


class TestInitSpa:
    def test_adds_spa_controller_when_index_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        (tmp_path / "index.html").write_text("<html/>")
        handlers = []
        init_spa(handlers)
        assert SpaController in handlers

    def test_no_controller_when_index_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        handlers = []
        init_spa(handlers)
        assert handlers == []


# ---------------------------------------------------------------------------
# create_app
# ---------------------------------------------------------------------------


def _paths(app: Litestar) -> set[str]:
    return {r.path for r in app.routes}


class TestCreateApp:
    def test_returns_litestar_instance(self):
        assert isinstance(create_app("postgresql://localhost/test"), Litestar)

    def test_no_metrics_route_by_default(self):
        assert "/metrics" not in _paths(create_app("postgresql://localhost/test"))

    def test_metrics_route_registered_when_enabled(self):
        assert "/metrics" in _paths(create_app("postgresql://localhost/test", metrics=True))

    def test_no_spa_routes_when_index_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        assert "/" not in _paths(create_app("postgresql://localhost/test"))

    def test_spa_routes_registered_when_index_exists(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        (tmp_path / "index.html").write_text("<html/>")
        assert "/" in _paths(create_app("postgresql://localhost/test"))

    def test_no_spa_routes_when_ui_disabled(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        (tmp_path / "index.html").write_text("<html/>")
        assert "/" not in _paths(create_app("postgresql://localhost/test", ui=False))

    def test_spa_has_no_guards_without_auth(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        (tmp_path / "index.html").write_text("<html/>")
        from pgwerk.api.spa import init_spa

        handlers: list = []
        init_spa(handlers)
        assert "guards" not in handlers[0].__dict__

    def test_spa_has_basic_auth_guard_when_configured(self, tmp_path, monkeypatch):
        monkeypatch.setattr("pgwerk.api.spa._STATIC_DIR", tmp_path)
        (tmp_path / "index.html").write_text("<html/>")
        from pgwerk.api.spa import init_spa
        from pgwerk.api.auth import make_basic_auth_guard

        handlers: list = []
        guard = make_basic_auth_guard("admin", "secret")
        init_spa(handlers, guard=guard)
        assert handlers[0].guards == [guard]

    def test_api_router_has_bearer_guard_when_token_configured(self):
        from pgwerk.api.auth import make_bearer_guard

        guard = make_bearer_guard("secret")
        router = make_router(guards=[guard])
        assert guard in router.guards

    def test_api_router_has_no_guards_by_default(self):
        router = make_router()
        assert not router.guards

    def test_api_router_always_registered(self):
        paths = _paths(create_app("postgresql://localhost/test"))
        assert "/api/jobs" in paths
        assert "/api/workers" in paths
        assert "/api/stats" in paths
