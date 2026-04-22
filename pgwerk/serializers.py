from __future__ import annotations

import json

from typing import Any
from typing import Protocol
from typing import runtime_checkable


def encode(serializer: "Serializer", value: Any) -> str | None:
    """Encode a value with *serializer* for JSONB storage.

    Args:
        serializer: Serializer instance used to convert the value to a string.
        value: The Python object to encode.

    Returns:
        A JSON-encoded string suitable for insertion into a JSONB column, or
        ``None`` if *value* is ``None``.
    """
    if value is None:
        return None
    return json.dumps(serializer.dumps(value))


def decode(serializer: "Serializer", value: Any) -> Any:
    """Decode a JSONB-stored value produced by :func:`encode`.

    Args:
        serializer: Serializer instance used to deserialize the inner payload.
        value: Raw value from the database — may be ``None``, ``bytes``, or a
            JSON string.

    Returns:
        The decoded Python object, or ``None`` if *value* is ``None``.
    """
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
    """Protocol for job payload serializers.

    Any object that implements ``dumps`` / ``loads`` satisfies this protocol and
    can be passed as the ``serializer`` argument to :class:`~wrk.app.Wrk`.
    """

    def dumps(self, obj: Any) -> str:
        """Serialize *obj* to a string.

        Args:
            obj: Python object to serialize.

        Returns:
            String representation of *obj*.
        """
        ...

    def loads(self, s: str | bytes) -> Any:
        """Deserialize *s* back to a Python object.

        Args:
            s: Serialized string or bytes produced by :meth:`dumps`.

        Returns:
            The deserialized Python object.
        """
        ...


class JSONSerializer:
    """Default serializer that encodes payloads as plain JSON."""

    def dumps(self, obj: Any) -> str:
        """Serialize *obj* to a JSON string.

        Args:
            obj: JSON-serializable Python object.

        Returns:
            JSON string representation.
        """
        return json.dumps(obj)

    def loads(self, s: str | bytes) -> Any:
        """Deserialize a JSON string back to a Python object.

        Args:
            s: JSON string or bytes.

        Returns:
            The parsed Python object.
        """
        return json.loads(s)


class PickleSerializer:
    """Serializes arbitrary Python objects via pickle + base64.

    Use only when the payload contains types not representable in JSON
    (e.g. dataclasses, numpy arrays). Workers must share the same codebase
    as the enqueueing process for pickle to work correctly.
    """

    def dumps(self, obj: Any) -> str:
        """Pickle *obj* and base64-encode the result.

        Args:
            obj: Arbitrary Python object.

        Returns:
            Base64-encoded pickle bytes as a UTF-8 string.
        """
        import base64
        import pickle

        return base64.b64encode(pickle.dumps(obj)).decode()

    def loads(self, s: str | bytes) -> Any:
        """Decode and unpickle a value produced by :meth:`dumps`.

        Args:
            s: Base64-encoded pickle string or bytes.

        Returns:
            The original Python object.
        """
        import base64
        import pickle

        if isinstance(s, str):
            s = s.encode()
        return pickle.loads(base64.b64decode(s))


_default: JSONSerializer | None = None


def get_default() -> JSONSerializer:
    """Return the process-wide default JSONSerializer, creating it if necessary.

    Returns:
        The singleton JSONSerializer instance.
    """
    global _default
    if _default is None:
        _default = JSONSerializer()
    return _default
