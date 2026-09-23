import math

import numpy as np
import pandas as pd
import pytest

import patternq.plots as pqp
import patternq.survival as pqs

TIME = [1, 2, 2, 3, 4, 5, 6, 6, 7, 8, 9, 10, 11, 12, 13, 15]
EVENT = [1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 0, 1]


def test_logrank_matches_r_survdiff():
    # reference values from R: patternq::logrank_test == survival::survdiff
    r2 = pqs.logrank_test(TIME, EVENT, ["a", "b"] * 8)
    assert r2["chisq"] == pytest.approx(0.0302901755, abs=1e-9)
    assert r2["p"] == pytest.approx(0.8618334533, abs=1e-9)
    assert r2["expected"]["a"] == pytest.approx(4.256777, abs=1e-6)
    r3 = pqs.logrank_test(TIME, EVENT, ["a", "b", "c", "c"] * 4)
    assert r3["df"] == 2
    assert r3["chisq"] == pytest.approx(0.8158267956, abs=1e-9)
    assert r3["p"] == pytest.approx(0.6650364699, abs=1e-9)
    assert math.isnan(pqs.logrank_test(TIME, EVENT, ["a"] * 16)["p"])


def test_logrank_shared_reference_case():
    # shared with R/patternq/tests/testthat/test-survival.R
    r = pqs.logrank_test(range(1, 11), [True, True, False, True, True, True, False, True, False, True],
                         list("abaababbab"))
    assert r["df"] == 1
    assert r["chisq"] == pytest.approx(0.2201257417, abs=1e-9)


def test_chisq_pvalue():
    assert pqs.chisq_pvalue(3.841458820694124, 1) == pytest.approx(0.05, abs=1e-10)
    assert pqs.chisq_pvalue(5.991464547107979, 2) == pytest.approx(0.05, abs=1e-10)
    assert pqs.chisq_pvalue(0, 3) == 1


def test_kaplan_meier_reexported():
    assert pqp.kaplan_meier is pqs.kaplan_meier


def test_median_split():
    s = pqs.median_split([1, 2, 3, 4, None])
    assert list(s[:4]) == ["low", "low", "high", "high"]
    assert s[4] is None


def test_survival_status():
    oc = pd.DataFrame({"os": [20, 5, 5, None], "os_event": [False, True, False, True]})
    assert list(pqs.survival_status(oc)) == ["alive at 1 year", "died within 1 year", None, None]


def test_survival_by_median():
    oc = pd.DataFrame({"subject_id": list("abcdef"), "os": [1, 2, 3, 10, 11, 12],
                       "os_event": [True] * 6})
    t = pqs.survival_by_median({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "z": 9}, oc)
    assert list(t["group"]) == ["low"] * 3 + ["high"] * 3
    assert t.attrs["logrank"]["p"] < 0.05


def test_change_from_baseline():
    tab = pd.DataFrame({"subject_id": ["s1"] * 3 + ["s2"] * 2 + ["s3"],
                        "timepoint_id": ["C1D1", "C2D1", "C4D1", "C1D1", "C2D1", "C2D1"],
                        "cell_population": ["T"] * 6,
                        "value": [1.0, 2.0, 0.5, 4.0, 0.0, 3.0]})
    ch = pqs.change_from_baseline(tab)
    assert set(ch["subject_id"]) == {"s1", "s2"}
    assert list(ch["change"][:3]) == [0, 1, -1]
    assert np.isnan(ch["change"].iloc[4])  # log2(0) -> NaN
    d = pqs.change_from_baseline(tab, method="difference")
    assert list(d["change"]) == [0, 1, -0.5, 0, -4]
    r = pqs.change_from_baseline(tab, method="ratio", pseudocount=1)
    assert r["change"].iloc[1] == pytest.approx(1.5)


def test_plots_survival_and_timepoint_and_heatmap_groups():
    oc = pd.DataFrame({"os": TIME, "os_event": [bool(e) for e in EVENT], "g": ["a", "b"] * 8})
    fig = pqp.plot_survival(oc, group="g", levels=["b", "a"])
    assert fig.layout.annotations[0].text == "log-rank p = 0.86"
    assert fig.data[0].name.startswith("b ")
    assert not pqp.plot_survival(oc, group="g", pvalue=False).layout.annotations
    tab = pd.DataFrame({"subject_id": ["s1", "s1", "s2", "s2"], "timepoint_id": ["C1D1", "C2D1"] * 2,
                        "change": [0, 1, 0, -1], "bor": ["PR", "PR", "PD", "PD"]})
    fig = pqp.plot_by_timepoint(tab, group="bor", lines=True, value="change", timepoints=["C2D1", "C1D1"])
    boxes = [t for t in fig.data if t.type == "box"]
    assert [b.name for b in boxes] == ["PD", "PR"] and all(b.boxpoints is False for b in boxes)
    assert sum(t.type == "scatter" for t in fig.data) == 2
    assert list(fig.layout.xaxis.categoryarray) == ["C2D1", "C1D1"]
    m = pd.DataFrame(np.arange(12.0).reshape(3, 4), index=list("xyz"), columns=list("abcd"))
    fig = pqp.plot_heatmap(m, col_groups={"a": "alive", "b": "died", "c": "alive", "d": "died"})
    assert len(fig.data) == 2
    assert [a.text for a in fig.layout.annotations] == ["■ alive", "■ died"]
