from __future__ import annotations

import enum
import json
import uuid
import base64
import pickle
import decimal
import datetime

from typing import Any
from typing import Protocol
from typing import runtime_checkable


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


class _Encoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, enum.Enum):
            return o.value
        return super().default(o)


class JSONSerializer:
    """Default serializer that encodes payloads as plain JSON."""

    def dumps(self, obj: Any) -> str:
        """Serialize *obj* to a JSON string.

        Args:
            obj: JSON-serializable Python object.

        Returns:
            JSON string representation.
        """
        return json.dumps(obj, cls=_Encoder)

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
        return base64.b64encode(pickle.dumps(obj)).decode()

    def loads(self, s: str | bytes) -> Any:
        """Decode and unpickle a value produced by :meth:`dumps`.

        Args:
            s: Base64-encoded pickle string or bytes.

        Returns:
            The original Python object.
        """
        if isinstance(s, str):
            s = s.encode()
        return pickle.loads(base64.b64decode(s))


_T = "__t__"


class TypedJSONSerializer:
    """JSON serializer that round-trips primitive Python types not native to JSON.

    Handles: UUID, datetime, date, timedelta, Decimal, bytes, tuple, set.
    """

    def dumps(self, obj: Any) -> str:
        return json.dumps(self._prepare(obj), default=self._encode)

    def loads(self, s: str | bytes) -> Any:
        return json.loads(s, object_hook=self._decode)

    def _prepare(self, obj: Any) -> Any:
        """Recursively tag tuple and set values before JSON encoding.

        ``json.dumps`` natively converts tuples to arrays and rejects sets,
        so ``default()`` never sees them. Pre-tagging them as typed dicts
        preserves the distinction on the way out.
        """
        if isinstance(obj, dict):
            return {k: self._prepare(v) for k, v in obj.items()}
        if isinstance(obj, tuple):
            return {_T: "tuple", "v": [self._prepare(i) for i in obj]}
        if isinstance(obj, (set, frozenset)):
            return {_T: "set", "v": [self._prepare(i) for i in obj]}
        if isinstance(obj, list):
            return [self._prepare(i) for i in obj]
        return obj

    def _encode(self, o: Any) -> Any:
        if isinstance(o, uuid.UUID):
            return {_T: "uuid", "v": str(o)}
        if isinstance(o, datetime.datetime):
            return {_T: "datetime", "v": o.isoformat()}
        if isinstance(o, datetime.date):
            return {_T: "date", "v": o.isoformat()}
        if isinstance(o, datetime.timedelta):
            return {_T: "timedelta", "v": o.total_seconds()}
        if isinstance(o, decimal.Decimal):
            return {_T: "decimal", "v": str(o)}
        if isinstance(o, bytes):
            return {_T: "bytes", "v": base64.b64encode(o).decode()}
        raise TypeError(f"not serializable: {type(o)!r}")

    def _decode(self, d: dict) -> Any:
        t = d.get(_T)
        if t is None:
            return d
        v = d["v"]
        if t == "uuid":
            return uuid.UUID(v)
        if t == "datetime":
            return datetime.datetime.fromisoformat(v)
        if t == "date":
            return datetime.date.fromisoformat(v)
        if t == "timedelta":
            return datetime.timedelta(seconds=v)
        if t == "decimal":
            return decimal.Decimal(v)
        if t == "bytes":
            return base64.b64decode(v)
        if t == "tuple":
            return tuple(v)
        if t == "set":
            return set(v)
        return d


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
        try:
            return serializer.loads(value)
        except Exception:
            return parsed
    return value


