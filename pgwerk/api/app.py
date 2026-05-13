from __future__ import annotations

from litestar import Litestar

from .auth import make_bearer_guard
from .auth import make_basic_auth_guard
from .spa import init_spa
from .deps import init_werk
from .routes import MetricsController
from .routes import make_router
from .exporter import init_exporter
from .handlers import server_error_handler
from ..config import WerkConfig


def create_app(config: WerkConfig | None = None) -> Litestar:
    """Create the Litestar observability app.

    Args:
        config: Full wrk configuration. Defaults to ``WerkConfig()`` when omitted.

    Returns:
        A configured Litestar application instance.
    """
    if config is None:
        config = WerkConfig.from_env()

    state: dict = {}
    dependencies: dict = {}
    on_startup: list = []
    on_shutdown: list = []

    api_guards = []
    if config.api_token:
        api_guards.append(make_bearer_guard(config.api_token))
    route_handlers: list = [make_router(guards=api_guards)]

    init_werk(config, state, dependencies, on_startup, on_shutdown)

    if config.metrics:
        init_exporter(config.metrics_interval, state, on_startup, on_shutdown)
        route_handlers.append(MetricsController)

    if config.ui:
        spa_guard = None
        if config.ui_auth:
            user, _, password = config.ui_auth.partition(":")
            spa_guard = make_basic_auth_guard(user, password)
        init_spa(route_handlers, guard=spa_guard)

    return Litestar(
        route_handlers=route_handlers,
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: server_error_handler},
    )
