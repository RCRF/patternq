from patternq import query as pqq
from patternq import results as pqres


def test_normalize_prepends_db():
    q = pqq._normalize({":find": ["?id"], ":where": [["?s", ":subject/id", "?id"]]})
    assert q[":in"] == ["$"]
    q = pqq._normalize({"find": ["?id"], "in": ["$", "?x"], "where": []})
    assert q[":in"] == ["$", "?x"]


def test_flatten_pull():
    m = {":sample/id": "S1", ":db/id": 1,
         ":sample/subject": {":subject/id": "P1"},
         ":sample/specimen": {":db/ident": ":sample.specimen/ffpe"}}
    assert pqres.flatten_pull(m) == {"sample_id": "S1", "subject_id": "P1", "sample_specimen": "ffpe"}


def test_find_column_names():
    assert pqres.find_column_names(["?sample-id", ["count", "?s"]]) == ["sample_id", "count_s"]
