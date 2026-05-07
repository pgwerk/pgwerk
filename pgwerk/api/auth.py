from __future__ import annotations

import base64
from typing import Callable

from litestar.connection import ASGIConnection
from litestar.handlers import BaseRouteHandler
from litestar.exceptions import NotAuthorizedException

Guard = Callable[[ASGIConnection, BaseRouteHandler], None]


def make_basic_auth_guard(username: str, password: str) -> Guard:
    expected = base64.b64encode(f"{username}:{password}".encode()).decode()

    def guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
        auth = connection.headers.get("Authorization", "")
        if not auth.startswith("Basic ") or auth[6:] != expected:
            raise NotAuthorizedException(headers={"WWW-Authenticate": 'Basic realm="wrk"'})

    return guard


def make_bearer_guard(token: str) -> Guard:
    expected = f"Bearer {token}"

    def guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
        if connection.headers.get("Authorization") != expected:
            raise NotAuthorizedException(headers={"WWW-Authenticate": "Bearer"})

    return guard
