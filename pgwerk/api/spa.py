from __future__ import annotations

import pathlib
import mimetypes

from litestar.response import File
from litestar.exceptions import NotFoundException


_STATIC_DIR = pathlib.Path(__file__).parent / "static"
_STATIC_RESERVED_PREFIXES = {"api", "metrics"}


def resolve_spa_file(path: str | None = None) -> File:
    """Resolve a URL path to a static file, falling back to index.html for unknown paths.

    Intended for use as a Litestar DI provider on SpaController; the path
    route parameter is injected automatically.

    Args:
        path: URL path segment to resolve. None or empty string serves index.html.

    Returns:
        A Litestar File response for the resolved static asset.

    Raises:
        NotFoundException: If the path starts with a reserved API prefix.
    """
    normalized = path.strip("/") if path else ""
    if normalized.split("/", 1)[0] in _STATIC_RESERVED_PREFIXES:
        raise NotFoundException()
    target = (_STATIC_DIR / normalized) if normalized else (_STATIC_DIR / "index.html")
    if target.is_file():
        media_type, _ = mimetypes.guess_type(str(target))
        return File(path=target, media_type=media_type, content_disposition_type="inline")
    return File(path=_STATIC_DIR / "index.html", media_type="text/html", content_disposition_type="inline")


def init_spa(route_handlers: list, guard=None) -> None:
    """Register SpaController if a built frontend is present.

    Args:
        route_handlers: List of Litestar route handlers to append SpaController to.
        guard: Optional Litestar guard to apply to all SPA routes.
    """
    if (_STATIC_DIR / "index.html").exists():
        from .routes import SpaController

        if guard is not None:
            SpaController = type("SpaController", (SpaController,), {"guards": [guard]})
        route_handlers.append(SpaController)
