from __future__ import annotations

import click

from .utils import load_app
from ..logging import configure_logging


@click.command()
@click.argument("app", required=False)
@click.option("--host", "-h", default="127.0.0.1", show_default=True, help="Host to bind.")
@click.option("--port", "-p", default=8000, show_default=True, help="Port to bind.")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload (development).")
@click.option("--metrics", is_flag=True, default=False, help="Serve Prometheus metrics at GET /metrics.")
@click.option("--metrics-interval", default=15.0, show_default=True, help="Metrics collection interval in seconds.")
@click.option(
    "--log-level",
    "-l",
    default="INFO",
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.option(
    "--log-format",
    default="text",
    show_default=True,
    type=click.Choice(["text", "json"], case_sensitive=False),
)
@click.option("--no-color", is_flag=True, default=False, help="Disable colored log output.")
def api(
    app: str | None,
    host: str,
    port: int,
    reload: bool,
    metrics: bool,
    metrics_interval: float,
    log_level: str,
    log_format: str,
    no_color: bool,
) -> None:
    """Start the HTTP API server.

    APP is an optional Wrk instance to use, e.g. ``myapp.tasks:wrk``.
    If omitted, WRK_DSN must be set in the environment.
    """
    try:
        import uvicorn
    except ImportError:
        raise click.ClickException("This command requires 'uvicorn'. Install with: pip install 'wrk[api]'")

    configure_logging(level=log_level, format=log_format, color=False if no_color else None)

    from ..api.app import create_app

    if reload:
        if app:
            raise click.ClickException("--reload cannot be used with a custom APP instance.")
        uvicorn.run(
            "wrk.api.app:create_app",
            factory=True,
            host=host,
            port=port,
            reload=True,
            log_level=log_level.lower(),
        )
    else:
        wrk_instance = load_app(app) if app else None
        litestar_app = create_app(
            wrk=wrk_instance,
            exporter_interval=metrics_interval if metrics else None,
        )
        uvicorn.run(
            litestar_app,
            host=host,
            port=port,
            log_level=log_level.lower(),
        )
