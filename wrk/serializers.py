from __future__ import annotations

import json

from typing import Any
from typing import Protocol
from typing import runtime_checkable


def encode(serializer: "Serializer", value: Any) -> str | None:
    """Encode a value with *serializer* for JSONB storage."""
    if value is None:
        return None
    return json.dumps(serializer.dumps(value))


def decode(serializer: "Serializer", value: Any) -> Any:
    """Decode a JSONB-stored value produced by :func:`_encode`."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            try:
                return serializer.loads(value)
            except Exception:
                return value
        if isinstance(parsed, str):
            try:
                return serializer.loads(parsed)
            except Exception:
                return parsed
        return parsed
    return value


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, obj: Any) -> str: ...
    def loads(self, s: str | bytes) -> Any: ...


class JSONSerializer:
    def dumps(self, obj: Any) -> str:
        return json.dumps(obj)

    def loads(self, s: str | bytes) -> Any:
        return json.loads(s)


class PickleSerializer:
    """Serializes arbitrary Python objects via pickle + base64.

    Use only when the payload contains types not representable in JSON
    (e.g. dataclasses, numpy arrays). Workers must share the same codebase
    as the enqueueing process for pickle to work correctly.
    """

    def dumps(self, obj: Any) -> str:
        import base64
        import pickle

        return base64.b64encode(pickle.dumps(obj)).decode()

    def loads(self, s: str | bytes) -> Any:
        import base64
        import pickle

        if isinstance(s, str):
            s = s.encode()
        return pickle.loads(base64.b64decode(s))


_default: JSONSerializer | None = None


def get_default() -> JSONSerializer:
    global _default
    if _default is None:
        _default = JSONSerializer()
    return _default
