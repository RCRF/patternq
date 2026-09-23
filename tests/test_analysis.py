import math

import numpy as np
import pandas as pd
import pytest

import patternq.analysis as pqa
import patternq.plots as pqp
from conftest import test_db as tdb


def test_percentile_rank_mid_rank_ties():
    assert pqa.percentile_rank([1, 2, 3, 4], 2.5) == 50
    assert pqa.percentile_rank([1, 2, 2, 3], 2) == 50
    assert pqa.percentile_rank([1, 2, 3, 4], 10) == 100
    assert math.isnan(pqa.percentile_rank([], 1))


def test_log_fold_change_matches_clojure_definition():
    lfc = pqa.log_fold_change({"G1": 0, "G2": 3, "G3": 7}, {"G1": 1, "G2": 3, "G4": 15}).set_index("hgnc_symbol")
    assert lfc.loc["G1", "lfc"] == 1
    assert lfc.loc["G2", "lfc"] == 0
    assert lfc.loc["G3", "lfc"] == -3
    assert lfc.loc["G4", "lfc"] == 4
    assert lfc.loc["G2", "avg_log10"] == pytest.approx(np.log10(4))


def test_ssgsea_score():
    x = pd.Series(np.arange(100, 0, -1), index=[f"G{i}" for i in range(1, 101)])
    assert pqa.ssgsea_score(x, [f"G{i}" for i in range(1, 11)]) > 0
    assert pqa.ssgsea_score(x, [f"G{i}" for i in range(91, 101)]) < 0
    assert math.isnan(pqa.ssgsea_score(x, ["nope"]))


def test_expression_distances():
    a, b = {"A": 1, "B": 0}, {"A": 0, "B": 1}
    assert pqa.expression_distance(a, a) == 0
    assert pqa.expression_distance(a, b) == 1
    assert pqa.expression_distance(a, b, "euclidean") == pytest.approx(math.sqrt(2))
    m = pd.DataFrame([[1, 0], [0, 1]], index=["s1", "s2"], columns=["A", "B"])
    assert list(pqa.nearest_samples(a, m, n=1)["sample_id"]) == ["s1"]


def test_top_varying_genes_and_kaplan_meier():
    m = pd.DataFrame({"flat": [5] * 4, "varied": [0, 10, 100, 1000]})
    assert list(pqa.top_varying_genes(m, 1)["hgnc_symbol"]) == ["varied"]
    km = pqp.kaplan_meier([1, 2, 3, 4], [True, False, True, True])
    assert list(km["surv"]) == pytest.approx([0.75, 0.75, 0.375, 0])


def test_genesets_ship_with_package():
    gs = pqa.genesets()
    assert {"hallmark_hypoxia", "housekeeping", "multi_cancer"} <= set(gs)
    assert "GAPDH" in pqa.geneset("housekeeping")
    with pytest.raises(KeyError):
        pqa.geneset("nope")


def test_ssgsea_matrix_and_plot_ma_offline():
    m = pd.DataFrame(np.arange(12).reshape(3, 4), index=["s1", "s2", "s3"], columns=["A", "B", "C", "D"])
    ss = pqa.ssgsea(m, {"ab": ["A", "B"]})
    assert ss.shape == (3, 1)
    ch = pqa.log_fold_change({"A": 1, "B": 1000, "C": 50}, {"A": 900, "B": 1, "C": 50})
    fig = pqp.plot_ma(ch, lfc_threshold=2, label_min_avg=0)
    assert {t.name.split(" ")[0] for t in fig.data} == {"up", "down", "within"}
    assert {a.text for a in fig.layout.annotations} == {"A", "B"}


@pytest.mark.live
def test_live_sample_vs_cohort_and_two_sample_change():
    db, uvm = tdb("H37001"), tdb("tcga-uvm")
    genes = ["MLANA", "PMEL", "TYR", "GAPDH", "BAP1", "PRAME"]
    cmp = pqa.compare_to_cohort("H37001-003", db=db, cohort_db=uvm, genes=genes)
    assert set(cmp["hgnc_symbol"]) == set(genes)
    assert (cmp["cohort_n"] == 80).all()
    assert cmp["percentile"].between(0, 100).all()
    pqp.plot_zscores(cmp)
    ch = pqa.compare_samples("H37001-003", "H37001-001", db=db)
    assert (ch["avg_log10"] >= 0.5).all()
    pqp.plot_ma(ch)
    fig = pqa.examine_geneset(["MLANA", "PMEL"], ["H37001-003"], db=db, cohort_dbs=uvm)
    assert len(fig.data) == 2
