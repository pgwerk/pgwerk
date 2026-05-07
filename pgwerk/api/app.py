from __future__ import annotations

from litestar import Litestar

from .spa import init_spa
from .deps import init_werk
from .routes import MetricsController
from .routes import make_router
from .exporter import init_exporter
from .handlers import server_error_handler


def create_app(
    dsn: str | None = None,
    *,
    schema: str | None = None,
    prefix: str | None = None,
    metrics: bool = False,
    metrics_interval: float = 15.0,
    ui: bool = True,
    auth: str | None = None,
    token: str | None = None,
) -> Litestar:
    """Create the Litestar observability app.

    Args:
        dsn: Postgres connection string. Falls back to PGWERK_DSN if None.
        schema: Postgres schema for wrk tables, or None for the default.
        prefix: Table-name prefix, or None for the default.
        metrics: Enable Prometheus metrics at GET /metrics.
        metrics_interval: Scrape interval in seconds (only used when metrics=True).
        ui: Serve the SPA dashboard (only has effect when static files are present).
        auth: Basic Auth credentials for the SPA as ``user:password``.
        token: Bearer token for the API (guards all ``/api/*`` routes).

    Returns:
        A configured Litestar application instance.
    """
    state: dict = {}
    dependencies: dict = {}
    on_startup: list = []
    on_shutdown: list = []

    api_guards = []
    if token:
        from .auth import make_bearer_guard

        api_guards.append(make_bearer_guard(token))

    route_handlers: list = [make_router(guards=api_guards)]

    init_werk(dsn, schema, prefix, state, dependencies, on_startup, on_shutdown)

    if metrics:
        init_exporter(metrics_interval, state, on_startup, on_shutdown)
        route_handlers.append(MetricsController)

    if ui:
        spa_guard = None
        if auth:
            from .auth import make_basic_auth_guard

            user, _, password = auth.partition(":")
            spa_guard = make_basic_auth_guard(user, password)

        init_spa(route_handlers, guard=spa_guard)

    return Litestar(
        route_handlers=route_handlers,
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: server_error_handler},
    )
