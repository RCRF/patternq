import numpy as np
import pandas as pd
import pytest

from patternq import context as pqc
from patternq import dataset as pqd
from patternq import plots as pqp
from patternq import results as pqres


def test_flatten_pull_many_refs_and_single_element_lists():
    m = {":variant/id": "V1",
         ":variant/so-consequences": [{":so-sequence-feature/name": "missense_variant"}],
         ":variant/impact": {":db/ident": ":variant.impact/high"}}
    assert pqres.flatten_pull(m) == {"variant_id": "V1", "variant_so_consequences": ["missense_variant"],
                                     "variant_impact": "high"}
    df = pqres.result_to_df([[m]], [["pull", "?v", ["*"]]])
    assert df.loc[0, "variant_so_consequences"] == "missense_variant"


def test_to_matrix_and_back():
    df = pd.DataFrame({"sample_id": ["a", "a", "b"], "gene": ["X", "Y", "X"], "value": [1.0, 2.0, 3.0]})
    m = pqc.to_matrix(df, col="gene")
    assert m.loc["a", "Y"] == 2.0 and np.isnan(m.loc["b", "Y"])
    back = pqc.to_long(m, col_name="gene")
    assert len(back) == 3


def test_kaplan_meier():
    km = pqp.kaplan_meier([1, 2, 2, 3, 4], [True, True, False, True, False])
    assert list(km["time"]) == [1, 2, 3, 4]
    assert (np.diff(km["surv"]) <= 0).all()
    assert km["surv"].iloc[0] == pytest.approx(0.8)


def test_average_linkage_groups_similar_rows():
    x = np.array([[0.0, 0], [10, 10], [0.1, 0], [10.1, 10]])
    order = pqp._average_linkage_order(x)
    pos = {r: i for i, r in enumerate(order)}
    assert abs(pos[0] - pos[2]) == 1 and abs(pos[1] - pos[3]) == 1


def test_cnv_requires_subset():
    with pytest.raises(ValueError, match="subset"):
        pqd.cnv_segments("any-db")
    with pytest.raises(ValueError, match="subset"):
        pqd.cnv_gene_calls("any-db")


def test_measurements_query_targets():
    q, args = pqd.measurements_query("median-channel-value", "S",
                                     targets=[":measurement/cell-population", ":measurement/epitope"])
    assert q[":find"] == ["?sample-id", "?measurement-set", "?cell-population", "?epitope-id", "?value"]
    assert args == ["S"]
    q, _ = pqd.measurements_query("cnv-call", targets=[])
    assert ["?value-ref", ":db/ident", "?value"] in q[":where"]


def test_plots_offline():
    th = pqp.plot_theme()
    assert len(th["categorical"]) == 8
    v = pd.DataFrame({"sample_id": ["s1", "s1", "s2"], "hgnc_symbol": ["A", "B", "A"],
                      "vaf": [0.1, 0.5, 0.3], "impact": [None, None, None]})
    fig = pqp.plot_mutation_landscape(v)
    assert fig.data[0].showscale is False
    m = pd.DataFrame(np.random.default_rng(0).normal(size=(5, 4)), index=list("abcde"), columns=list("wxyz"))
    fig = pqp.plot_heatmap(m, scale="row")
    assert fig.data[0].zmin == -fig.data[0].zmax
    with pytest.raises(ValueError):
        pqp.plot_heatmap(m.iloc[:0])
    oc = pd.DataFrame({"os": [1, 2, 3, 4], "os_event": [True, False, True, True], "bor": ["PR", "PR", "PD", "PD"]})
    fig = pqp.plot_survival(oc, group="bor")
    assert [t.name for t in fig.data if t.mode == "lines"] == ["PD (n=2)", "PR (n=2)"]
