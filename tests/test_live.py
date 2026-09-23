import pytest

import patternq as pq
import patternq.dataset as pqd
import patternq.plots as pqp
from conftest import test_db as tdb

pytestmark = pytest.mark.live


def test_h37001():
    db = tdb("H37001")
    assert len(pqd.samples(db)) == 6
    assert list(pqd.subjects(db)["subject_id"]) == ["H37001"]
    v = pqd.variants(db)
    assert len(v) > 100
    assert "BAP1" in set(v["hgnc_symbol"])
    assert list(v.columns[:4]) == ["sample_id", "measurement_set", "variant_id", "hgnc_symbol"]
    assert pq.provenance(v).db == db
    gx = pqd.gene_expression(db, genes=["BAP1", "GNAQ"], measurement="rsem-normalized-count")
    assert set(gx["hgnc_symbol"]) == {"BAP1", "GNAQ"}
    assert len(pqp.plot_vaf_histogram(v, samples=["H37001-001"]).data) == 1


def test_tcga_uvm():
    db = tdb("tcga-uvm")
    assert len(pqd.subjects(db)) == 80
    assert set(pqd.dataset_summary(db)["assay_technology"]) == {"WES", "RNA-seq"}
    assert len(pqd.gene_expression(db, genes=["BAP1", "PRAME"])) > 100
    n = pq.do_query({":find": [["count", "?s"]], ":where": [["?s", ":sample/id"]]}, db=db, cache=False)
    assert n["count_s"][0] == 80
    d = pq.datoms("aevt", [":subject/id"], db=db, limit=3)
    assert len(d) == 3 and d["a"][0] == ":subject/id"
