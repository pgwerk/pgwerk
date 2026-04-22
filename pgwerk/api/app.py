from __future__ import annotations

import os
import logging
import pathlib

from typing import TYPE_CHECKING

from litestar import Request
from litestar import Litestar
from litestar import Response
from litestar import get
from litestar.di import Provide
from litestar.static_files import StaticFilesConfig
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR


_STATIC_DIR = pathlib.Path(__file__).parent / "static"


def _static_config() -> list[StaticFilesConfig]:
    if (_STATIC_DIR / "index.html").exists():
        return [StaticFilesConfig(directories=[_STATIC_DIR], path="/", html_mode=True)]
    return []


from ..app import Wrk
from .routes import router


if TYPE_CHECKING:
    from ..exporter import WrkExporter


logger = logging.getLogger("wrk.api")


def _server_error_handler(request: Request, exc: Exception) -> Response:
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path, exc_info=exc)
    return Response(
        content={"detail": "Internal server error"},
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
    )


async def _create_pgwerk() -> Wrk:
    dsn = os.environ["PGWERK_DSN"]
    app = Wrk(dsn)
    await app.connect()
    return app


async def _close_pgwerk(werk: Wrk) -> None:
    await wrk.disconnect()


def create_app(
    werk: Wrk | None = None,
    exporter: "WrkExporter | None" = None,
    exporter_interval: float | None = None,
) -> Litestar:
    """Create the Litestar observability app.

    Pass an already-connected ``Wrk`` instance, or set ``PGWERK_DSN`` in the
    environment and one will be created on startup.

    To serve Prometheus metrics at ``GET /metrics`` on the same server:
    - pass a pre-built ``WrkExporter`` via *exporter*, or
    - pass *exporter_interval* (seconds) and one will be created automatically.
    """
    dependencies: dict = {}
    on_startup = []
    on_shutdown = []

    if wrk is not None:
        dependencies["wrk"] = Provide(lambda: wrk, use_cache=True, sync_to_thread=False)

        # Exporter needs the Wrk instance — create it now if only interval was given.
        if exporter is None and exporter_interval is not None:
            from ..exporter import WrkExporter

            exporter = WrkExporter(wrk, interval=exporter_interval)
    else:
        _state: dict = {}

        async def _startup() -> None:
            _state["wrk"] = await _create_pgwerk()
            if exporter_interval is not None and "exporter" not in _state:
                from ..exporter import WrkExporter

                _state["exporter"] = WrkExporter(_state["wrk"], interval=exporter_interval)
                await _state["exporter"].start()

        async def _shutdown() -> None:
            if "exporter" in _state:
                await _state["exporter"].stop()
            if "wrk" in _state:
                await _close_pgwerk(_state["wrk"])

        async def _get_pgwerk() -> Wrk:
            return _state["wrk"]

        dependencies["wrk"] = Provide(_get_pgwerk, use_cache=True, sync_to_thread=False)
        on_startup.append(_startup)
        on_shutdown.append(_shutdown)

        # Metrics handler for the env-var path — reads exporter from _state at request time.
        if exporter_interval is not None:

            @get("/metrics", media_type="text/plain", sync_to_thread=False)
            async def _metrics_from_state() -> Response[bytes]:
                exp = _state.get("exporter")
                if exp is None:
                    return Response(content=b"", media_type="text/plain", status_code=503)
                body, content_type = exp.metrics_bytes()
                return Response(content=body, media_type=content_type)

            return Litestar(
                route_handlers=[router, _metrics_from_state],
                static_files_config=_static_config(),
                dependencies=dependencies,
                on_startup=on_startup,
                on_shutdown=on_shutdown,
                exception_handlers={Exception: _server_error_handler},
            )

    route_handlers: list = [router]

    if exporter is not None:
        _exporter = exporter

        @get("/metrics", media_type="text/plain", sync_to_thread=False)
        async def metrics_handler() -> Response[bytes]:
            body, content_type = _exporter.metrics_bytes()
            return Response(content=body, media_type=content_type)

        route_handlers.append(metrics_handler)
        on_startup.append(_exporter.start)
        on_shutdown.append(_exporter.stop)

    return Litestar(
        route_handlers=route_handlers,
        static_files_config=_static_config(),
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: _server_error_handler},
    )
