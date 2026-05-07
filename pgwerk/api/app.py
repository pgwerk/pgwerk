from __future__ import annotations

from litestar import Litestar

from .spa import init_spa
from .deps import init_werk
from .routes import MetricsController
from .routes import router
from .exporter import init_exporter
from .handlers import server_error_handler


def create_app(
    dsn: str | None = None,
    *,
    schema: str | None = None,
    prefix: str | None = None,
    exporter_interval: float | None = None,
) -> Litestar:
    """Create the Litestar observability app.

    Args:
        dsn: Postgres connection string. Falls back to PGWERK_DSN if None.
        schema: Postgres schema for wrk tables, or None for the default.
        prefix: Table-name prefix, or None for the default.
        exporter_interval: If set, enables Prometheus metrics at GET /metrics
            with this scrape interval in seconds.

    Returns:
        A configured Litestar application instance.
    """
    state: dict = {}
    dependencies: dict = {}
    on_startup: list = []
    on_shutdown: list = []
    route_handlers: list = [router]

    init_werk(dsn, schema, prefix, state, dependencies, on_startup, on_shutdown)

    if exporter_interval is not None:
        init_exporter(exporter_interval, state, on_startup, on_shutdown)
        route_handlers.append(MetricsController)

    init_spa(route_handlers)

    return Litestar(
        route_handlers=route_handlers,
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: server_error_handler},
    )
