"""Survival helpers for outcome-association analyses (as in the PRINCE
biomarker figures: OS stratified at the median of a baseline biomarker,
log-rank p-values, landmark survival status). Mirrors R/patternq/R/survival.R.

    import patternq.survival as pqs
"""
import math
from typing import Dict, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


def kaplan_meier(time: Sequence[float], event: Sequence[bool]) -> pd.DataFrame:
    """Kaplan-Meier estimate: time, n_risk, n_event, n_censor, surv."""
    t = np.asarray(time, dtype=float)
    e = np.asarray(event, dtype=bool)
    ut = np.unique(t)
    n_risk = np.array([(t >= u).sum() for u in ut])
    n_event = np.array([((t == u) & e).sum() for u in ut])
    n_censor = np.array([((t == u) & ~e).sum() for u in ut])
    surv = np.cumprod(1 - n_event / n_risk)
    return pd.DataFrame({"time": ut, "n_risk": n_risk, "n_event": n_event, "n_censor": n_censor, "surv": surv})


def _gammaincc(a: float, x: float) -> float:
    """Regularized upper incomplete gamma Q(a, x) (series / continued fraction)."""
    if x <= 0:
        return 1.0
    gln = math.lgamma(a)
    if x < a + 1:
        ap, s, d = a, 1.0 / a, 1.0 / a
        for _ in range(1000):
            ap += 1
            d *= x / ap
            s += d
            if abs(d) < abs(s) * 1e-15:
                break
        return 1.0 - s * math.exp(-x + a * math.log(x) - gln)
    b = x + 1 - a
    c = 1.0 / 1e-300
    d = 1.0 / b
    h = d
    for i in range(1, 1000):
        an = -i * (i - a)
        b += 2
        d = an * d + b
        d = 1e-300 if abs(d) < 1e-300 else d
        c = b + an / c
        c = 1e-300 if abs(c) < 1e-300 else c
        d = 1.0 / d
        h *= d * c
        if abs(d * c - 1) < 1e-15:
            break
    return math.exp(-x + a * math.log(x) - gln) * h


def chisq_pvalue(chisq: float, df: int) -> float:
    """Upper tail probability of the chi-squared distribution."""
    if chisq is None or not np.isfinite(chisq):
        return float("nan")
    return _gammaincc(df / 2.0, chisq / 2.0)


def logrank_test(time: Sequence[float], event: Sequence, group: Sequence) -> Dict:
    """Mantel-Haenszel log-rank test for a difference in survival between two or
    more groups (hand-rolled; matches R's survival::survdiff and
    patternq::logrank_test).

    Returns dict: chisq, df, p, observed, expected, n (per group, dicts)."""
    d = pd.DataFrame({"time": pd.Series(list(time), dtype=float),
                      "event": pd.Series(list(event), dtype=object),
                      "group": pd.Series(list(group), dtype=object)})
    d = d.dropna()
    t = d["time"].to_numpy()
    e = d["event"].astype(bool).to_numpy()
    g = d["group"].astype(str).to_numpy()
    lv = sorted(set(g))
    k = len(lv)
    if k < 2:
        return {"chisq": float("nan"), "df": 0, "p": float("nan")}
    O = np.zeros(k)
    E = np.zeros(k)
    V = np.zeros((k, k))
    for tt in np.unique(t[e]):
        at_risk = np.array([((t >= tt) & (g == lvl)).sum() for lvl in lv], dtype=float)
        d_g = np.array([((t == tt) & e & (g == lvl)).sum() for lvl in lv], dtype=float)
        n = at_risk.sum()
        dd = d_g.sum()
        if n < 1:
            continue
        O += d_g
        E += dd * at_risk / n
        if n > 1:
            f = dd * (n - dd) / (n ** 2 * (n - 1))
            V += f * (np.diag(at_risk * n) - np.outer(at_risk, at_risk))
    diff = (O - E)[:k - 1]
    chisq = float(diff @ np.linalg.solve(V[:k - 1, :k - 1], diff))
    return {"chisq": chisq, "df": k - 1, "p": chisq_pvalue(chisq, k - 1),
            "observed": dict(zip(lv, O)), "expected": dict(zip(lv, E)),
            "n": {lvl: int((g == lvl).sum()) for lvl in lv}}


def median_split(x: Sequence[float], labels: Sequence[str] = ("low", "high")) -> pd.Series:
    """Split values at the median: labels[1] at or above, labels[0] below;
    missing where x is missing."""
    s = pd.Series(x, dtype=float)
    m = s.median()
    out = pd.Series(np.where(s >= m, labels[1], labels[0]), index=s.index, dtype=object)
    return out.where(s.notna(), None)


def survival_status(outcomes: pd.DataFrame, time: str = "os", event: str = "os_event", at: float = 12,
                    labels: Optional[Mapping[str, str]] = None) -> pd.Series:
    """Landmark survival status, e.g. alive at 1 year: time >= at -> "alive";
    event before at -> "died"; censored before at -> None (unknown)."""
    labels = labels or {"alive": "alive at 1 year", "died": "died within 1 year"}
    out = []
    for t, e in zip(outcomes[time], outcomes[event]):
        if t is None or (isinstance(t, float) and math.isnan(t)) or pd.isna(t):
            out.append(None)
        elif t >= at:
            out.append(labels["alive"])
        elif not pd.isna(e) and bool(e):
            out.append(labels["died"])
        else:
            out.append(None)
    return pd.Series(out, index=outcomes.index, dtype=object)


def survival_by_median(values: Mapping[str, float], outcomes: pd.DataFrame, time: str = "os",
                       event: str = "os_event") -> pd.DataFrame:
    """Split subjects at the median of a subject-level biomarker (values: subject
    id -> value) and test the difference in survival (log-rank). Returns the
    outcomes of subjects with a value, plus value and group ("low"/"high"); the
    log-rank test is in df.attrs["logrank"]."""
    v = pd.Series(values, dtype=float).dropna()
    tab = outcomes[outcomes["subject_id"].isin(v.index)].copy()
    tab["value"] = tab["subject_id"].map(v)
    tab["group"] = median_split(tab["value"]).to_numpy()
    tab.attrs["logrank"] = logrank_test(tab[time], tab[event], tab["group"])
    return tab


_TARGET_COLS = ["cell_population", "epitope_id", "hgnc_symbol", "signature", "measurement_set", "target"]


def change_from_baseline(tab: pd.DataFrame, baseline: str = "C1D1", by: Optional[Sequence[str]] = None,
                         method: str = "log2_ratio", pseudocount: float = 0) -> pd.DataFrame:
    """Change of value per subject (and target) relative to the subject's value at
    the baseline timepoint.

    by: columns identifying a series besides subject_id (default: the target
      columns present among cell_population, epitope_id, hgnc_symbol,
      signature, measurement_set, target).
    method: "log2_ratio" (log2(value / baseline); frequencies), "difference"
      (value - baseline; values already on a log scale, like Olink NPX) or
      "ratio". pseudocount is added to both before a ratio.

    Returns tab restricted to subjects with a baseline value, plus
    baseline_value and change (non-finite changes -> NaN)."""
    if method not in ("log2_ratio", "difference", "ratio"):
        raise ValueError("method must be 'log2_ratio', 'difference' or 'ratio'")
    by = [c for c in _TARGET_COLS if c in tab.columns] if by is None else list(by)
    keys = ["subject_id"] + by
    base = tab[(tab["timepoint_id"].astype(str) == str(baseline)) & tab["value"].notna()]
    bval = base.groupby(keys, dropna=False)["value"].mean().rename("baseline_value").reset_index()
    out = tab.drop(columns=["baseline_value"], errors="ignore").merge(bval, on=keys, how="left")
    out = out[out["baseline_value"].notna()].copy()
    v, b = out["value"].astype(float), out["baseline_value"].astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        if method == "log2_ratio":
            ch = np.log2((v + pseudocount) / (b + pseudocount))
        elif method == "difference":
            ch = v - b
        else:
            ch = (v + pseudocount) / (b + pseudocount)
    out["change"] = ch.where(np.isfinite(ch))
    out.attrs = dict(tab.attrs)
    return out.reset_index(drop=True)
