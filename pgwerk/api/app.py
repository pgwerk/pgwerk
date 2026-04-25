from __future__ import annotations

import os
import logging
import pathlib
import mimetypes

from typing import TYPE_CHECKING

from litestar import Request
from litestar import Litestar
from litestar import Response
from litestar import get
from litestar.di import Provide
from litestar.response import File
from litestar.exceptions import HTTPException
from litestar.exceptions import NotFoundException
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR


_STATIC_DIR = pathlib.Path(__file__).parent / "static"
_STATIC_RESERVED_PREFIXES = {"api", "metrics"}


def _create_spa_handlers() -> list:
    if not (_STATIC_DIR / "index.html").exists():
        return []

    def _serve_path(path: str | None = None) -> File:
        normalized = path.strip("/") if path else ""
        if normalized.split("/", 1)[0] in _STATIC_RESERVED_PREFIXES:
            raise NotFoundException()

        target = (_STATIC_DIR / normalized) if normalized else (_STATIC_DIR / "index.html")
        if target.is_file():
            media_type, _ = mimetypes.guess_type(str(target))
            return File(path=target, media_type=media_type, content_disposition_type="inline")
        return File(path=_STATIC_DIR / "index.html", media_type="text/html", content_disposition_type="inline")

    @get("/")
    async def spa_index() -> File:
        return _serve_path()

    @get("/{path:path}")
    async def spa_fallback(path: str) -> File:
        return _serve_path(path)

    return [spa_index, spa_fallback]


from ..app import Werk  # noqa: E402
from .routes import router  # noqa: E402


if TYPE_CHECKING:
    from ..exporter import WerkExporter


logger = logging.getLogger("pgwerk.api")


def _server_error_handler(request: Request, exc: Exception) -> Response:
    if isinstance(exc, HTTPException):
        raise exc
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path, exc_info=exc)
    return Response(
        content={"detail": "Internal server error"},
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
    )


async def _create_pgwerk() -> Werk:
    dsn = os.environ["PGWERK_DSN"]
    app = Werk(dsn)
    await app.connect()
    return app


async def _close_pgwerk(werk: Werk) -> None:
    await werk.disconnect()


def create_app(
    werk: Werk | None = None,
    exporter: "WerkExporter | None" = None,
    exporter_interval: float | None = None,
) -> Litestar:
    """Create the Litestar observability app.

    Pass an already-connected ``Werk`` instance, or set ``PGWERK_DSN`` in the
    environment and one will be created on startup.

    To serve Prometheus metrics at ``GET /metrics`` on the same server:
    - pass a pre-built ``WerkExporter`` via *exporter*, or
    - pass *exporter_interval* (seconds) and one will be created automatically.
    """
    dependencies: dict = {}
    on_startup = []
    on_shutdown = []

    if werk is not None:
        dependencies["werk"] = Provide(lambda: werk, use_cache=True, sync_to_thread=False)

        # Exporter needs the Werk instance — create it now if only interval was given.
        if exporter is None and exporter_interval is not None:
            from ..exporter import WerkExporter

            exporter = WerkExporter(werk, interval=exporter_interval)
    else:
        _state: dict = {}

        async def _startup() -> None:
            _state["werk"] = await _create_pgwerk()
            if exporter_interval is not None and "exporter" not in _state:
                from ..exporter import WerkExporter

                _state["exporter"] = WerkExporter(_state["werk"], interval=exporter_interval)
                await _state["exporter"].start()

        async def _shutdown() -> None:
            if "exporter" in _state:
                await _state["exporter"].stop()
            if "werk" in _state:
                await _close_pgwerk(_state["werk"])

        async def _get_pgwerk() -> Werk:
            return _state["werk"]

        dependencies["werk"] = Provide(_get_pgwerk, use_cache=True, sync_to_thread=False)
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
                route_handlers=[router, _metrics_from_state, *_create_spa_handlers()],
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
        route_handlers=[*route_handlers, *_create_spa_handlers()],
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: _server_error_handler},
    )
