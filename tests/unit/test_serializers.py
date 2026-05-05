from __future__ import annotations

from pgwerk.serializers import Serializer
from pgwerk.serializers import JSONSerializer
from pgwerk.serializers import PickleSerializer
from pgwerk.serializers import get_default


class TestJSONSerializer:
    def test_roundtrip_dict(self):
        s = JSONSerializer()
        data = {"key": "value", "n": 42, "nested": [1, 2, 3]}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_list(self):
        s = JSONSerializer()
        data = [1, "two", None, True]
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_none(self):
        s = JSONSerializer()
        assert s.loads(s.dumps(None)) is None

    def test_loads_bytes(self):
        s = JSONSerializer()
        assert s.loads(b'"hello"') == "hello"

    def test_implements_protocol(self):
        assert isinstance(JSONSerializer(), Serializer)

    def test_uuid_becomes_str(self):
        import uuid

        s = JSONSerializer()
        uid = uuid.uuid4()
        result = s.loads(s.dumps({"id": uid}))
        assert result == {"id": str(uid)}

    def test_datetime_becomes_isoformat(self):
        import datetime

        s = JSONSerializer()
        dt = datetime.datetime(2024, 1, 15, 12, 30, 0)
        result = s.loads(s.dumps({"at": dt}))
        assert result == {"at": "2024-01-15T12:30:00"}

    def test_date_becomes_isoformat(self):
        import datetime

        s = JSONSerializer()
        d = datetime.date(2024, 1, 15)
        result = s.loads(s.dumps({"on": d}))
        assert result == {"on": "2024-01-15"}

    def test_decimal_becomes_str(self):
        import decimal

        s = JSONSerializer()
        result = s.loads(s.dumps({"amount": decimal.Decimal("9.99")}))
        assert result == {"amount": "9.99"}

    def test_enum_becomes_value(self):
        import enum

        class Color(enum.Enum):
            RED = "red"

        s = JSONSerializer()
        result = s.loads(s.dumps({"color": Color.RED}))
        assert result == {"color": "red"}


class TestPickleSerializer:
    def test_roundtrip_dict(self):
        s = PickleSerializer()
        data = {"x": 1, "y": [2, 3]}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_tuple(self):
        s = PickleSerializer()
        data = (1, "two", None, [3, 4])
        assert s.loads(s.dumps(data)) == data

    def test_loads_bytes_input(self):
        s = PickleSerializer()
        encoded = s.dumps(99)
        assert s.loads(encoded.encode()) == 99

    def test_implements_protocol(self):
        assert isinstance(PickleSerializer(), Serializer)


class TestGetDefault:
    def test_returns_json_serializer(self):
        s = get_default()
        assert isinstance(s, JSONSerializer)

    def test_singleton(self):
        assert get_default() is get_default()


class TestEncodeDecodeHelpers:
    def test_encode_none_returns_none(self):
        from pgwerk.serializers import encode

        assert encode(JSONSerializer(), None) is None

    def test_encode_dict(self):
        import json

        from pgwerk.serializers import encode

        result = encode(JSONSerializer(), {"key": "val"})
        assert json.loads(json.loads(result)) == {"key": "val"}

    def test_encode_list(self):
        import json

        from pgwerk.serializers import encode

        result = encode(JSONSerializer(), [1, 2, 3])
        assert json.loads(json.loads(result)) == [1, 2, 3]

    def test_decode_none_returns_none(self):
        from pgwerk.serializers import decode

        assert decode(JSONSerializer(), None) is None

    def test_decode_bytes(self):
        import json

        from pgwerk.serializers import decode

        raw = json.dumps(json.dumps(42)).encode()
        assert decode(JSONSerializer(), raw) == 42

    def test_decode_dict_passthrough(self):
        from pgwerk.serializers import decode

        assert decode(JSONSerializer(), {"x": 1}) == {"x": 1}

    def test_decode_json_int_string_returns_int(self):
        from pgwerk.serializers import decode

        result = decode(JSONSerializer(), "42")
        assert result == 42

    def test_decode_non_json_string_returns_original(self):
        from pgwerk.serializers import decode

        result = decode(JSONSerializer(), "not json {{")
        assert result == "not json {{"

    def test_decode_nested_json_string_inner_string_deserializes(self):
        import json

        from pgwerk.serializers import decode

        inner = json.dumps("hello")
        outer = json.dumps(inner)
        result = decode(JSONSerializer(), outer)
        assert result == "hello"

    def test_decode_nested_string_fallback_on_serializer_failure(self):
        import json

        from pgwerk.serializers import PickleSerializer
        from pgwerk.serializers import decode

        s = PickleSerializer()
        outer = json.dumps("hello")
        result = decode(s, outer)
        assert result == "hello"
