import numpy as np
import pytest

import patternq as pq
import patternq.context as pqc
import patternq.dataset as pqd
import patternq.plots as pqp
import patternq.reference as pqr
from conftest import test_db as tdb

pytestmark = pytest.mark.live


def test_prince():
    db = tdb("prince-2022")
    mt = pqd.measurement_types("PICI CyTOF Immune Profiling", db=db)
    assert {"percent-of-parent", "median-channel-value", "cell-population", "epitope"} <= set(mt["attribute"])
    pp = pqd.measurements("percent-of-parent", "PICI CyTOF Immune Profiling", db=db)
    assert list(pp.columns) == ["sample_id", "measurement_set", "cell_population", "value"]
    assert len(pp) == int(mt.loc[mt["attribute"] == "percent-of-parent", "count"].iloc[0])
    mcv = pqd.measurements("median-channel-value", "PICI CyTOF Immune Profiling", db=db)
    assert {"cell_population", "epitope_id"} <= set(mcv.columns)
    ol = pqd.measurements("olink-npx", "PICI Olink Proteomics", db=db, wide=True)
    assert ol.shape[1] > 10
    oc = pqd.subject_outcomes(db)
    assert {"subject_id", "bor", "os", "os_event", "pfs"} <= set(oc.columns)
    assert len(oc) == len(pqd.subjects(db))
    ok = oc.dropna(subset=["os", "os_event"])
    km = pqp.kaplan_meier(ok["os"], ok["os_event"].astype(bool))
    assert (np.diff(km["surv"]) <= 0).all()
    tp = pqd.timepoints(db)
    assert list(tp["timepoint_relative_order"]) == sorted(tp["timepoint_relative_order"])
    ctx = pqc.add_sample_context(pp.head(20), db=db, include_outcomes=True)
    assert {"subject_id", "timepoint_id", "bor"} <= set(ctx.columns)
    pqp.plot_survival(oc, group="bor")
    pqp.plot_by_timepoint(ctx, group="bor")
    va = pqr.variant_annotations(db, genes=["KRAS"])
    assert (va["hgnc_symbol"] == "KRAS").all()
    assert pqr.map_gene_symbols(["p53", "HER2"], db=db) == {"p53": "TP53", "HER2": "ERBB2"}
    assert pq.provenance(oc).db == db


def test_h37004_cnv_isoforms_interventions():
    db = tdb("H37004")
    with pytest.raises(ValueError, match="subset"):
        pqd.cnv_segments(db)
    cs = pqd.cnv_segments(db, genes=["TP53"])
    assert (cs["hgnc_symbol"] == "TP53").all()
    assert {"contig", "start", "end", "segment_mean_lrr"} <= set(cs.columns)
    assert len(pqd.cnv_segments(db, subjects=["H37004"])) > len(cs)
    iso = pqd.isoforms("TP53", db=db)
    assert {"transcript_id", "isoform_percent"} <= set(iso.columns)
    assert len(pqd.clinical_interventions(db)) > 0
    assert len(pqd.measurement_matrices(db)) == 2


def test_painter_gene_cnv_calls():
    db = tdb("painter-2025-angiosarc")
    calls = pqd.cnv_gene_calls(db, genes=["MYC", "CDKN2A"])
    assert set(calls["value"]) <= {-2, -1, 0, 1, 2}
    assert calls["sample_id"].nunique() < len(pqd.samples(db))
