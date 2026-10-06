"""Optional transit response formats: decoded results are identical to JSON."""
import pandas as pd
import pytest

from patternq import dataset as pqd
from patternq import query as pqq
from patternq import transit as pqt

# The transit formats are optional for users but always tested: a missing
# transit-python fails here instead of skipping (pip install -e ".[test]").
import transit.writer as transit


def roundtrip(value, fmt):
    import io
    out = io.StringIO() if fmt == "json" else io.BytesIO()
    transit.Writer(out, fmt).write(value)
    data = out.getvalue()
    return pqt.decode(data.encode() if isinstance(data, str) else data,
                      "transit+json" if fmt == "json" else "transit+msgpack")


@pytest.mark.parametrize("fmt", ["json", "msgpack"])
def test_decoded_values_have_the_json_shape(fmt):
    import datetime
    from transit.transit_types import Keyword, frozendict, true, false
    v = frozendict({"query_result": ((frozendict({Keyword("sample/id"): "S1",
                                                  Keyword("sample/type"): frozendict({Keyword("db/ident"): Keyword("sample.type/tumor")}),
                                                  Keyword("sample/recurrence"): false,
                                                  Keyword("sample/uid"): ("A", "B")}),
                                      true, 17592186270456, 1.5, None,
                                      datetime.datetime(2026, 3, 31, 22, 21, 35, 170000, tzinfo=datetime.timezone.utc),
                                      datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)),),
                    "basis_t": 42})
    res = roundtrip(v, fmt)
    assert res == {"query_result": [[{":sample/id": "S1", ":sample/type": {":db/ident": ":sample.type/tumor"},
                                      ":sample/recurrence": False, ":sample/uid": ["A", "B"]},
                                     True, 17592186270456, 1.5, None,
                                     "2026-03-31T22:21:35.17Z", "1970-01-01T00:00:00Z"]],
                   "basis_t": 42}
    assert type(res["query_result"][0][1]) is bool


def test_unknown_format():
    with pytest.raises(ValueError):
        pqt.accept("edn")


@pytest.mark.live
@pytest.mark.parametrize("fmt", ["transit+json", "transit+msgpack"])
def test_transit_results_match_json(fmt):
    db = pqq.resolve_db("tcga-uvm")
    for f in (pqd.samples, pqd.variants):
        a = f(db, cache=False)
        b = f(db, format=fmt)
        cols = sorted(a.columns)
        key = [c for c in cols if a[c].map(lambda x: not isinstance(x, list)).all()]
        pd.testing.assert_frame_equal(a[cols].sort_values(key).reset_index(drop=True),
                                      b[cols].sort_values(key).reset_index(drop=True))
