from __future__ import annotations

import logging

from litestar import Request
from litestar import Response
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR


logger = logging.getLogger("pgwerk.api")


def server_error_handler(request: Request, exc: Exception) -> Response:
    """Return a JSON error response for any unhandled exception.

    HTTPException subclasses are forwarded with their original status code and
    detail message. Everything else is logged and returned as a generic 500.

    Args:
        request: The incoming Litestar request.
        exc: The unhandled exception.

    Returns:
        A JSON Response with an appropriate status code and detail message.
    """
    if isinstance(exc, HTTPException):
        return Response(content={"detail": exc.detail}, status_code=exc.status_code)
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path, exc_info=exc)
    return Response(
        content={"detail": "Internal server error"},
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
    )
