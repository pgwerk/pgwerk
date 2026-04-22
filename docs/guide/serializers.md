# Serializers

Serializers control how job payloads, results, and metadata are encoded for storage in Postgres. `wrk` ships with two built-in serializers and supports custom implementations.

## JSONSerializer (default)

The default serializer. Payloads are encoded as JSON, which is readable in the database and portable across processes and languages.

```python
from pgwerk import Wrk

app = Wrk("postgresql://user:pass@localhost/mydb")
# JSONSerializer is used automatically
```

JSON imposes the usual type constraints: values must be JSON-serialisable (strings, numbers, booleans, lists, dicts, `None`). Datetimes, dataclasses, and other Python types must be converted before enqueueing.

## PickleSerializer

Serializes payloads with `pickle` and encodes the bytes as base64. This lifts the JSON type restriction — you can pass arbitrary Python objects including dataclasses, numpy arrays, and custom classes.

```python
from pgwerk import Wrk, PickleSerializer

app = Wrk("postgresql://user:pass@localhost/mydb", serializer=PickleSerializer())
```

!!! warning
    Pickle payloads are **not portable**. Workers must share the same codebase and Python version as the enqueueing process. Use `PickleSerializer` only when the payload genuinely cannot be expressed in JSON.

## Custom serializers

Any object that satisfies the `Serializer` protocol works:

```python
from pgwerk import Wrk, Serializer
import msgpack

class MsgPackSerializer:
    def dumps(self, obj) -> str:
        # Must return a str; encode bytes as a string if needed
        return msgpack.packb(obj).hex()

    def loads(self, s: str | bytes):
        data = bytes.fromhex(s) if isinstance(s, str) else s
        return msgpack.unpackb(data)

app = Wrk(dsn, serializer=MsgPackSerializer())
```

The `Serializer` protocol requires two methods:

```python
class Serializer(Protocol):
    def dumps(self, obj: Any) -> str: ...
    def loads(self, s: str | bytes) -> Any: ...
```

## Scope

The configured serializer is used for:

- Job payload (the arguments passed to the handler)
- Job result (the return value of the handler)
- Job metadata (`_meta`)
- Retry intervals and repeat intervals stored on the job row

All workers connected to the same `Wrk` instance share its serializer. If you mix serializers across processes, payloads from one serializer cannot be decoded by another.
