"""Optional transit response formats for direct (uncached) queries.

The query service can answer POST /query with transit instead of JSON
(Accept application/transit+json or application/transit+msgpack). Decoding
needs the optional transit-python package (the patternq[transit] extra, or
pip install 'transit-python[msgpack]').
Decoded values are converted to exactly what the JSON response gives
(keywords -> ":ns/name" strings, instants -> ISO strings, tuples -> lists),
so everything downstream of patternq.query.query works unchanged.
"""
import datetime
import decimal
import io
import uuid
from collections.abc import Mapping
from typing import Any, Dict

FORMATS = {"json": "application/json",
           "transit+json": "application/transit+json",
           "transit+msgpack": "application/transit+msgpack"}

_PLAIN = frozenset([str, int, float, bool, type(None)])


def accept(fmt: str) -> str:
    if fmt not in FORMATS:
        raise ValueError(f"format must be one of {sorted(FORMATS)}, not {fmt!r}")
    return FORMATS[fmt]


def _reader(fmt: str):
    try:
        from transit.reader import Reader
    except ImportError as e:  # pragma: no cover - depends on the environment
        raise ImportError("format='%s' needs the optional transit-python package: "
                          "pip install 'transit-python[msgpack]'" % fmt) from e
    return Reader("msgpack" if fmt == "transit+msgpack" else "json")


def _types():
    from transit.transit_types import Boolean, Named, TaggedValue
    return Named, TaggedValue, Boolean


def _instant(x: datetime.datetime) -> str:
    """As the JSON response writes instants: UTC, milliseconds without
    trailing zeros ("...:35.17Z", "...:35Z")."""
    if x.tzinfo is not None:
        x = x.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return x.isoformat(timespec="milliseconds").rstrip("0").rstrip(".") + "Z"


def plain(x: Any, _named=None, _tagged=None, _bool=None) -> Any:
    """A decoded transit value as the JSON response has it."""
    t = type(x)
    if t in _PLAIN:
        return x
    if _named is None:
        _named, _tagged, _bool = _types()
    if t is _bool:  # transit's own true / false (Python's bool is an int)
        return bool(x)
    if t is tuple or t is list:
        return [v if type(v) in _PLAIN else plain(v, _named, _tagged, _bool) for v in x]
    if isinstance(x, Mapping):  # dict and transit's frozendict
        return {(k if type(k) is str else plain(k, _named, _tagged, _bool)):
                (v if type(v) in _PLAIN else plain(v, _named, _tagged, _bool)) for k, v in x.items()}
    if isinstance(x, _named):  # keywords and symbols
        return ":" + str(x)
    if isinstance(x, datetime.datetime):
        return _instant(x)
    if isinstance(x, (uuid.UUID,)):
        return str(x)
    if isinstance(x, decimal.Decimal):
        return float(x)
    if isinstance(x, (set, frozenset)):
        return [plain(v, _named, _tagged, _bool) for v in x]
    if isinstance(x, _tagged):
        return plain(x.rep, _named, _tagged, _bool)
    return str(x)


def decode(content: bytes, fmt: str) -> Dict[str, Any]:
    """Decode a transit query response into the JSON response's shape."""
    return plain(_reader(fmt).read(io.BytesIO(content)))
