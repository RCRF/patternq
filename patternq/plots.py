"""plotly plots, hand-rolled (mirrors R/patternq/R/plots.R). Each returns a
plotly.graph_objects.Figure; fig.to_dict() gives the figure spec.

The theme (theme/plotly-theme.json, shared by all three libraries) uses a
colorblind-validated categorical order: colors follow entities in fixed order
and are never cycled; series past 8 fold into "Other"."""
import json
from functools import lru_cache
from importlib import resources
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from patternq.survival import kaplan_meier, logrank_test  # noqa: F401  (kaplan_meier re-exported)

OTHER_COLOR = "#8a8983"


@lru_cache(maxsize=None)
def plot_theme() -> Dict:
    """Colors and fonts used by patternq plots (shared with R and Clojure)."""
    return json.loads(resources.files("patternq").joinpath("plotly-theme.json").read_text())


def series_colors(n: int) -> List[str]:
    cols = list(plot_theme()["categorical"])
    return (cols + [OTHER_COLOR] * max(0, n - len(cols)))[:n]


def fold_other(x: pd.Series, max_n: int = 8) -> pd.Series:
    """Keep the max_n - 1 most frequent categories, fold the rest into "Other"."""
    counts = x.value_counts()
    if len(counts) <= max_n:
        return x
    keep = set(counts.index[:max_n - 1])
    return x.where(x.isin(keep) | x.isna(), "Other")


def _sequential_scale():
    s = plot_theme()["sequential"]
    return [[i / (len(s) - 1), c] for i, c in enumerate(s)]


def _diverging_scale():
    d = plot_theme()["diverging"]
    return [[0, d["low"]], [0.5, d["mid"]], [1, d["high"]]]


def _alpha(hex_color: str, a: float) -> str:
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{a:.2f})"


def _layout(fig: go.Figure, title: Optional[str] = None, xaxis: Optional[dict] = None,
            yaxis: Optional[dict] = None, **kwargs) -> go.Figure:
    th = plot_theme()
    axis = {"gridcolor": th["grid"], "zerolinecolor": th["grid"], "linecolor": th["grid"],
            "tickfont": {"color": th["text_secondary"]}}
    fig.update_layout(title={"text": title, "x": 0, "xanchor": "left"},
                      font={"family": th["font"], "color": th["text_primary"]},
                      colorway=th["categorical"], paper_bgcolor=th["surface"], plot_bgcolor=th["surface"],
                      hoverlabel={"font": {"family": th["font"]}},
                      xaxis={**axis, **(xaxis or {})}, yaxis={**axis, **(yaxis or {})}, **kwargs)
    return fig


def plot_vaf_histogram(variants: pd.DataFrame, samples: Optional[Sequence[str]] = None,
                       bin_size: float = 0.05, title: str = "VAF histogram") -> go.Figure:
    """Overlaid VAF histograms per sample, from dataset.variants() output."""
    if samples is not None:
        variants = variants[variants["sample_id"].isin(samples)]
    ids = sorted(variants["sample_id"].unique())
    fig = go.Figure()
    for sid, col in zip(ids, series_colors(len(ids))):
        fig.add_trace(go.Histogram(x=variants.loc[variants["sample_id"] == sid, "vaf"], name=sid, opacity=0.7,
                                   marker={"color": col, "line": {"color": plot_theme()["surface"], "width": 1}},
                                   xbins={"start": 0, "end": 1, "size": bin_size}))
    return _layout(fig, title, barmode="overlay", showlegend=len(ids) > 1,
                   xaxis={"title": {"text": "VAF"}, "range": [0, 1]},
                   yaxis={"title": {"text": "variants"}})


def plot_gene_expression(expr: pd.DataFrame, title: str = "Gene expression",
                         ylab: str = "value", log: bool = False) -> go.Figure:
    """Grouped bars of expression per gene and sample, from
    dataset.gene_expression() output."""
    agg = expr.groupby(["sample_id", "hgnc_symbol"], as_index=False)["value"].sum()
    ids = sorted(agg["sample_id"].unique())
    fig = go.Figure()
    for sid, col in zip(ids, series_colors(len(ids))):
        a = agg[agg["sample_id"] == sid]
        fig.add_trace(go.Bar(x=a["hgnc_symbol"], y=a["value"], name=sid, marker={"color": col}))
    return _layout(fig, title, barmode="group", bargap=0.2, bargroupgap=0.05, showlegend=len(ids) > 1,
                   yaxis={"title": {"text": ylab}, "type": "log" if log else "linear"},
                   xaxis={"title": {"text": ""}})


def plot_sample_overview(sample_assays: pd.DataFrame,
                         title: str = "Samples per subject and measurement set") -> go.Figure:
    """Heatmap of sample counts per subject (rows) and measurement set (columns),
    from dataset.sample_assays(). Subjects without samples in a set are blank."""
    sa = sample_assays[["subject_id", "sample_id", "measurement_set_name"]].drop_duplicates()
    m = sa.groupby(["subject_id", "measurement_set_name"]).size().unstack()  # NaN = none: blank cell
    fig = go.Figure(go.Heatmap(x=list(m.columns), y=list(m.index), z=m.values, zmin=0,
                               colorscale=_sequential_scale(), xgap=1, ygap=1,
                               colorbar={"title": {"text": "samples"}},
                               hovertemplate="%{y}<br>%{x}<br>%{z} samples<extra></extra>"))
    return _layout(fig, title, xaxis={"title": {"text": ""}, "tickangle": -30},
                   yaxis={"title": {"text": "subject"}, "autorange": "reversed"})


def plot_by_timepoint(tab: pd.DataFrame, group: Optional[str] = None, lines: bool = False,
                      title: Optional[str] = None, ylab: str = "value", value: str = "value",
                      timepoints: Optional[Sequence[str]] = None,
                      levels: Optional[Sequence[str]] = None) -> go.Figure:
    """Box plot of values per timepoint (ordered by timepoint_relative_order when
    present, or by `timepoints`), optionally split by a grouping column (e.g.
    "bor"). lines=True draws per-subject trajectories (needs subject_id),
    colored by group when given, under translucent boxes.

    value: column to plot (e.g. "change" from survival.change_from_baseline).
    timepoints: optional timepoints to show, in order. levels: optional group
    order (and so color order)."""
    tab = tab.copy()
    tab["value"] = tab[value]
    tab = tab.dropna(subset=["value", "timepoint_id"])
    tab["timepoint_id"] = tab["timepoint_id"].astype(str)
    if timepoints is not None:
        timepoints = [str(t) for t in timepoints]
        tab = tab[tab["timepoint_id"].isin(timepoints)]
        present = set(tab["timepoint_id"])
        order = [t for t in timepoints if t in present]
    elif "timepoint_relative_order" in tab.columns:
        order = list(dict.fromkeys(tab.sort_values("timepoint_relative_order")["timepoint_id"]))
    else:
        order = sorted(tab["timepoint_id"].unique())
    rank = {t: i for i, t in enumerate(order)}
    fig = go.Figure()
    if group is None:
        if lines and "subject_id" in tab.columns:
            for sid, s in tab.groupby("subject_id"):
                s = s.assign(_r=s["timepoint_id"].map(rank)).sort_values("_r")
                fig.add_trace(go.Scatter(x=s["timepoint_id"], y=s["value"], mode="lines", text=sid,
                                         hoverinfo="text", showlegend=False,
                                         line={"color": "rgba(82,81,78,0.25)", "width": 1}))
        col = series_colors(1)[0]
        fig.add_trace(go.Box(x=tab["timepoint_id"], y=tab["value"], name=ylab, marker={"color": col},
                             line={"color": col}, boxpoints="all", jitter=0.3, pointpos=0, showlegend=False))
    else:
        g = fold_other(tab[group].astype("object").where(tab[group].notna()))
        present = set(g.dropna().unique())
        lv = sorted(present) if levels is None else [x for x in levels if x in present]
        cols = series_colors(len(lv))
        if lines and "subject_id" in tab.columns:
            # per-subject trajectories, colored by group, drawn under the boxes
            for l, col in zip(lv, cols):
                sub = tab[g == l]
                for sid, s in sub.groupby("subject_id"):
                    if len(s) < 2:
                        continue
                    s = s.assign(_r=s["timepoint_id"].map(rank)).sort_values("_r")
                    fig.add_trace(go.Scatter(x=s["timepoint_id"], y=s["value"], mode="lines",
                                             line={"color": _alpha(col, 0.35), "width": 1}, legendgroup=str(l),
                                             showlegend=False, hoverinfo="text", text=f"{sid}<br>{l}"))
        for l, col in zip(lv, cols):
            s = tab[g == l]
            fig.add_trace(go.Box(x=s["timepoint_id"], y=s["value"], name=str(l), legendgroup=str(l),
                                 marker={"color": col, "size": 4}, line={"color": col},
                                 fillcolor=_alpha(col, 0.15),
                                 boxpoints=False if lines else "all", jitter=0.3, pointpos=0))
        fig.update_layout(boxmode="overlay" if lines else "group")
    return _layout(fig, title, xaxis={"title": {"text": "timepoint"}, "type": "category",
                                      "categoryorder": "array", "categoryarray": order},
                   yaxis={"title": {"text": ylab}})


def plot_by_group(tab: pd.DataFrame, group: str, violin: bool = False, title: Optional[str] = None,
                  ylab: str = "value") -> go.Figure:
    """Box (or violin) plot of values per group, e.g. a measurement by best
    overall response."""
    tab = tab.dropna(subset=["value", group])
    g = fold_other(tab[group].astype(str))
    levels = sorted(g.unique())
    th = plot_theme()
    fig = go.Figure()
    for lv, col in zip(levels, series_colors(len(levels))):
        y = tab.loc[g == lv, "value"]
        x = [lv] * len(y)
        if violin:
            fig.add_trace(go.Violin(x=x, y=y, name=lv, line={"color": col, "width": 1.5},
                                    fillcolor=_alpha(col, 0.25),
                                    box={"visible": True, "fillcolor": th["surface"],
                                         "line": {"color": col}, "width": 0.15},
                                    meanline={"visible": False}, points="all", jitter=0.4, pointpos=0,
                                    marker={"color": col, "size": 5, "opacity": 0.8}))
        else:
            fig.add_trace(go.Box(x=x, y=y, name=lv, marker={"color": col}, line={"color": col},
                                 boxpoints="all", jitter=0.3, pointpos=0))
    return _layout(fig, title, showlegend=False, xaxis={"title": {"text": group}},
                   yaxis={"title": {"text": ylab}})


def _format_p(p: float) -> str:
    if p is None or not np.isfinite(p):
        return "NA"
    return "< 2.2e-16" if p < 2.2e-16 else f"{p:.2g}"


def plot_survival(tab: pd.DataFrame, time: str = "os", event: str = "os_event", group: Optional[str] = None,
                  title: str = "Survival", xlab: Optional[str] = None, pvalue: bool = True,
                  levels: Optional[Sequence[str]] = None) -> go.Figure:
    """Kaplan-Meier curves (hand-rolled, no lifelines), optionally by group, with
    censoring ticks and (pvalue=True, 2+ groups) a "log-rank p = ..." annotation.
    levels: optional group order (and so color order). tab: e.g.
    dataset.subject_outcomes()."""
    tab = tab.dropna(subset=[time, event])
    g = pd.Series("all", index=tab.index) if group is None else fold_other(tab[group].astype("object"))
    keep = g.notna()
    tab, g = tab[keep], g[keep]
    present = set(g.unique())
    lv = sorted(present) if levels is None else [x for x in levels if x in present]
    fig = go.Figure()
    for l, col in zip(lv, series_colors(len(lv))):
        sub = tab[g == l]
        km = kaplan_meier(sub[time], sub[event].astype(bool))
        nm = f"{l} (n={len(sub)})"
        fig.add_trace(go.Scatter(x=[0] + km["time"].tolist(), y=[1] + km["surv"].tolist(), mode="lines", name=nm,
                                 line={"shape": "hv", "color": col, "width": 2},
                                 hovertemplate=nm + "<br>t=%{x:.1f}<br>S=%{y:.2f}<extra></extra>"))
        cens = km[km["n_censor"] > 0]
        if len(cens):
            fig.add_trace(go.Scatter(x=cens["time"], y=cens["surv"], mode="markers", showlegend=False,
                                     name=nm + " censored", hoverinfo="skip",
                                     marker={"symbol": "line-ns-open", "size": 9, "color": col}))
    ann = []
    if pvalue and len(lv) > 1:
        lr = logrank_test(tab[time], tab[event], g)
        ann = [{"xref": "paper", "yref": "paper", "x": 0.02, "y": 0.04, "xanchor": "left", "showarrow": False,
                "text": f"log-rank p = {_format_p(lr['p'])}", "font": {"color": plot_theme()["text_secondary"]}}]
    return _layout(fig, title, showlegend=len(lv) > 1, annotations=ann,
                   xaxis={"title": {"text": xlab or time}, "rangemode": "tozero"},
                   yaxis={"title": {"text": "survival probability"}, "range": [0, 1.02]})


def _average_linkage_order(x: np.ndarray) -> List[int]:
    """Leaf order of average-linkage (UPGMA) hierarchical clustering on
    Euclidean distances; numpy only, O(n^3), fine for the few hundred rows a
    heatmap shows."""
    n = x.shape[0]
    if n < 3:
        return list(range(n))
    x = np.where(np.isfinite(x), x, 0.0)
    d = np.sqrt(((x[:, None, :] - x[None, :, :]) ** 2).sum(-1))
    np.fill_diagonal(d, np.inf)
    members = [[i] for i in range(n)]
    active = list(range(n))
    while len(active) > 1:
        sub = d[np.ix_(active, active)]
        ia, ib = np.unravel_index(np.argmin(sub), sub.shape)
        a, b = active[min(ia, ib)], active[max(ia, ib)]
        na, nb = len(members[a]), len(members[b])
        # Lance-Williams update for average linkage; a absorbs b
        d[a, :] = (na * d[a, :] + nb * d[b, :]) / (na + nb)
        d[:, a] = d[a, :]
        d[a, a] = np.inf
        members[a] = members[a] + members[b]
        active.remove(b)
    return members[active[0]]


def _scale_rows(m: np.ndarray) -> np.ndarray:
    mu = np.nanmean(m, axis=1, keepdims=True)
    sd = np.nanstd(m, axis=1, ddof=1, keepdims=True)
    sd[~np.isfinite(sd) | (sd == 0)] = 1
    return (m - mu) / sd


def plot_heatmap(m: pd.DataFrame, scale: str = "none", cluster_rows: bool = True, cluster_cols: bool = True,
                 title: Optional[str] = None, zlab: Optional[str] = None,
                 col_groups=None) -> go.Figure:
    """Clustered heatmap (average linkage, numpy only). scale "row" or "column"
    z-scores values and draws them on a diverging scale; otherwise sequential.
    col_groups: optional dict or Series (column name -> group label, e.g.
    survival status) drawn as an annotation strip above the heatmap, with a
    colored legend of the groups."""
    if m.shape[0] == 0 or m.shape[1] == 0:
        raise ValueError("plot_heatmap: empty matrix")
    v = m.to_numpy(dtype=float)
    if scale == "row":
        v = _scale_rows(v)
    elif scale == "column":
        v = _scale_rows(v.T).T
    ro = _average_linkage_order(v) if cluster_rows else list(range(v.shape[0]))
    co = _average_linkage_order(v.T) if cluster_cols else list(range(v.shape[1]))
    v = v[np.ix_(ro, co)]
    rows = [str(m.index[i]) for i in ro]
    cols = [str(m.columns[i]) for i in co]
    div = scale != "none"
    lim = float(np.nanmax(np.abs(v))) if div else None
    hm = go.Heatmap(x=cols, y=rows, z=v, colorscale=_diverging_scale() if div else _sequential_scale(),
                    zmin=-lim if div else None, zmax=lim if div else None,
                    colorbar={"title": {"text": zlab or ("z-score" if div else "value")}},
                    hovertemplate="%{y}<br>%{x}<br>%{z:.3g}<extra></extra>")
    if col_groups is None:
        fig = go.Figure(hm)
        return _layout(fig, title, xaxis={"title": {"text": ""}, "tickangle": -45, "type": "category"},
                       yaxis={"title": {"text": ""}, "type": "category", "autorange": "reversed"})
    groups = dict(col_groups)
    grp = [groups.get(c, groups.get(m.columns[i])) for c, i in zip(cols, co)]
    lv = sorted({x for x in grp if x is not None and not (isinstance(x, float) and np.isnan(x))})
    gcols = series_colors(len(lv))
    idx = {l: i + 1 for i, l in enumerate(lv)}
    z = [[idx.get(x, None) for x in grp]]
    cs = [[0 if len(gcols) == 1 else i / (len(gcols) - 1), c] for i, c in enumerate(gcols)] or [[0, OTHER_COLOR], [1, OTHER_COLOR]]
    if len(cs) == 1:
        cs = [[0, gcols[0]], [1, gcols[0]]]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.05, 0.95], vertical_spacing=0.005)
    fig.add_trace(go.Heatmap(x=cols, y=["group"], z=z, showscale=False, xgap=1, colorscale=cs, zmin=1,
                             zmax=max(1, len(lv)), text=[[str(x) if x is not None else "" for x in grp]],
                             hovertemplate="%{x}<br>%{text}<extra></extra>"), row=1, col=1)
    fig.add_trace(hm, row=2, col=1)
    ann = [{"xref": "paper", "yref": "paper", "x": 1, "y": 1.02 + 0.045 * (len(lv) - 1 - i), "xanchor": "right",
            "yanchor": "bottom", "showarrow": False, "text": f"\u25A0 {l}", "font": {"color": c, "size": 12}}
           for i, (l, c) in enumerate(zip(lv, gcols))]
    fig = _layout(fig, title, annotations=ann, margin={"t": 60 + 18 * len(lv)},
                  xaxis={"showticklabels": False, "type": "category"},
                  yaxis={"title": {"text": ""}, "showticklabels": False})
    th = plot_theme()
    axis = {"gridcolor": th["grid"], "zerolinecolor": th["grid"], "linecolor": th["grid"],
            "tickfont": {"color": th["text_secondary"]}}
    fig.update_layout(xaxis2={**axis, "title": {"text": ""}, "tickangle": -45, "type": "category"},
                      yaxis2={**axis, "title": {"text": ""}, "type": "category", "autorange": "reversed"})
    return fig


def plot_mutation_landscape(variants: pd.DataFrame, n_genes: int = 25, genes: Optional[Sequence[str]] = None,
                            title: str = "Mutation landscape") -> go.Figure:
    """Genes (rows, most frequently mutated first) by samples; cells show the most
    severe impact (modifier < low < moderate < high), or simply mutated when the
    dataset has no impact annotation."""
    levels = ["modifier", "low", "moderate", "high"]
    v = variants.dropna(subset=["hgnc_symbol"]).copy()
    has_impact = "impact" in v.columns and v["impact"].notna().any()
    v["severity"] = v["impact"].map({lv: i + 1 for i, lv in enumerate(levels)}).fillna(1) if has_impact else 1
    freq = v.groupby("hgnc_symbol")["sample_id"].nunique().sort_values(ascending=False, kind="stable")
    if genes is None:
        genes = list(freq.index[:n_genes])
    v = v[v["hgnc_symbol"].isin(genes)]
    m = v.pivot_table(index="hgnc_symbol", columns="sample_id", values="severity", aggfunc="max")
    m = m.reindex([g for g in genes if g in m.index])
    pres = m.notna().to_numpy()
    order = sorted(range(m.shape[1]), key=lambda j: tuple(-pres[:, j].astype(int)))
    m = m.iloc[:, order]
    s = plot_theme()["sequential"]
    labels = [f"{g} ({freq[g]})" for g in m.index]
    if has_impact:
        cs = [[0, s[1]], [0.33, s[2]], [0.34, s[3]], [0.66, s[4]], [0.67, s[5]], [1, s[6]]]
        txt = [[levels[int(x) - 1] if np.isfinite(x) else "" for x in row] for row in m.to_numpy()]
        trace = go.Heatmap(x=list(m.columns), y=labels, z=m.to_numpy(), text=txt, colorscale=cs, zmin=1, zmax=4,
                           xgap=1, ygap=1, colorbar={"title": {"text": "impact"}, "tickvals": [1, 2, 3, 4],
                                                     "ticktext": levels},
                           hovertemplate="%{y}<br>%{x}<br>%{text}<extra></extra>")
    else:
        trace = go.Heatmap(x=list(m.columns), y=labels, z=m.to_numpy(), colorscale=[[0, s[4]], [1, s[4]]],
                           showscale=False, xgap=1, ygap=1, hovertemplate="%{y}<br>%{x}<br>mutated<extra></extra>")
    fig = go.Figure(trace)
    return _layout(fig, title,
                   xaxis={"title": {"text": f"samples ({m.shape[1]})"}, "showticklabels": m.shape[1] <= 40,
                          "type": "category"},
                   yaxis={"title": {"text": ""}, "autorange": "reversed", "type": "category"})


# -- analysis plots (mirror R/patternq/R/analysis_expression.R) -----------------

def plot_zscores(comparison: pd.DataFrame, n: int = 30, min_value: float = 1, min_observed: float = 0.5,
                 title: Optional[str] = None) -> go.Figure:
    """Horizontal diverging bars of the top genes by z-score vs a reference
    cohort, from analysis.compare_to_cohort()."""
    from patternq.analysis import top_by_zscore
    top = top_by_zscore(comparison, n=n, min_value=min_value, min_observed=min_observed)
    top = top.sort_values("z", kind="stable")
    d = plot_theme()["diverging"]
    info = comparison.attrs.get("patternq_comparison")
    if title is None and info:
        title = f"{info['sample']} vs {info['cohort_db']}: top genes by z-score"
    text = [f"value {v:.3g} · cohort median {2 ** m - 1:.3g} · {p:.1f} pct"
            for v, m, p in zip(top["value"], top["cohort_median"], top["percentile"])]
    fig = go.Figure(go.Bar(x=top["z"], y=top["hgnc_symbol"], orientation="h",
                           marker={"color": [d["high"] if z >= 0 else d["low"] for z in top["z"]]},
                           text=text, textposition="none",
                           hovertemplate="%{y}<br>z = %{x:.2f}<br>%{text}<extra></extra>"))
    return _layout(fig, title, showlegend=False, bargap=0.25,
                   xaxis={"title": {"text": "z-score vs cohort (log2(1+x))"}, "zeroline": True},
                   yaxis={"title": {"text": ""}, "type": "category", "categoryorder": "array",
                          "categoryarray": list(top["hgnc_symbol"])})


def _power10_label(t: int) -> str:
    v = 10.0 ** t
    return f"{int(v):,}" if t >= 3 else f"{v:g}"


def plot_vs_cohort(sample_expr: pd.DataFrame, cohort_expr: pd.DataFrame, type: str = "violin", log: bool = True,
                   floor: float = 0.01, title: str = "Samples vs cohort expression",
                   xlab: str = "expression") -> go.Figure:
    """One row per gene: each cohort's distribution (violin or box) with each
    sample's value marked. A "cohort" column in cohort_expr distinguishes
    several cohorts. On a log scale, log10 values (floored at `floor`) are drawn
    on a linear axis so violin densities are estimated on the log scale, and
    ticks are labelled as powers of ten."""
    if type not in ("violin", "box"):
        raise ValueError("type must be 'violin' or 'box'")
    th = plot_theme()
    ce = cohort_expr.copy()
    if "cohort" not in ce.columns:
        ce["cohort"] = "cohort"
    ce = ce.groupby(["cohort", "sample_id", "hgnc_symbol"], as_index=False)["value"].sum()
    se = sample_expr.groupby(["sample_id", "hgnc_symbol"], as_index=False)["value"].sum()
    genes = list(dict.fromkeys(list(se["hgnc_symbol"].unique()) + list(ce["hgnc_symbol"].unique())))

    def fl(v):
        v = np.asarray(v, dtype=float)
        return np.log10(np.maximum(v, floor)) if log else v

    fig = go.Figure()
    cohorts = sorted(ce["cohort"].unique())
    for c, col in zip(cohorts, series_colors(len(cohorts))):
        sub = ce[ce["cohort"] == c]
        if type == "violin":
            fig.add_trace(go.Violin(x=fl(sub["value"]), y=sub["hgnc_symbol"], orientation="h", name=c,
                                    legendgroup=c, line={"color": col, "width": 1}, fillcolor=_alpha(col, 0.25),
                                    points=False, spanmode="hard", scalemode="width", width=0.8,
                                    box={"visible": True, "fillcolor": th["surface"], "line": {"color": col},
                                         "width": 0.2},
                                    meanline={"visible": False}, hoverinfo="y+name"))
        else:
            fig.add_trace(go.Box(x=fl(sub["value"]), y=sub["hgnc_symbol"], orientation="h", name=c, legendgroup=c,
                                 marker={"color": col, "size": 3}, line={"color": col},
                                 fillcolor=_alpha(col, 0.2), boxpoints="outliers"))
    symbols = ["diamond", "square", "circle", "triangle-up", "x", "star"]
    for j, sid in enumerate(sorted(se["sample_id"].unique())):
        sub = se[se["sample_id"] == sid]
        fig.add_trace(go.Scatter(x=fl(sub["value"]), y=sub["hgnc_symbol"], mode="markers", name=sid,
                                 marker={"symbol": symbols[j % len(symbols)], "size": 11, "color": th["text_primary"],
                                         "line": {"color": th["surface"], "width": 1.5}},
                                 customdata=sub["value"],
                                 hovertemplate=sid + "<br>%{y}: %{customdata:.3g}<extra></extra>"))
    xaxis = {"title": {"text": xlab}}
    if log:
        allv = np.concatenate([fl(ce["value"]), fl(se["value"])])
        allv = allv[np.isfinite(allv)]
        if len(allv):
            ticks = list(range(int(np.floor(allv.min())), int(np.ceil(allv.max())) + 1))
            xaxis.update(tickvals=ticks, ticktext=[_power10_label(t) for t in ticks])
    fig.update_layout(height=160 + 42 * len(genes), boxmode="group", violinmode="group")
    return _layout(fig, title, xaxis=xaxis,
                   yaxis={"title": {"text": ""}, "type": "category", "categoryorder": "array",
                          "categoryarray": list(reversed(genes))})


def plot_ma(change: pd.DataFrame, lfc_threshold: float = 2.5, highlight: Optional[Sequence[str]] = None,
            label_top: int = 8, label_min_avg: float = 1.5, title: Optional[str] = None) -> go.Figure:
    """MA plot of expression change between two samples (from
    analysis.compare_samples / log_fold_change). x: avg_log10; y: log2 fold
    change. Points past lfc_threshold are colored (up red, down blue, on the
    diverging scale), the rest recede; highlight genes and the top label_top
    each way (above label_min_avg) are labelled, thinned greedily so labels do
    not pile up (highlighted genes win)."""
    th = plot_theme()
    d = th["diverging"]
    info = change.attrs.get("patternq_comparison") or {}
    if title is None and info.get("sample_a"):
        title = f"Expression change: {info['sample_a']} → {info['sample_b']}"
    lfc = change["lfc"].to_numpy()
    cls = np.where(lfc >= lfc_threshold, "up", np.where(lfc <= -lfc_threshold, "down", "unchanged"))
    spec = {"unchanged": (_alpha(OTHER_COLOR, 0.35), "within threshold", 5),
            "down": (d["low"], f"down (lfc ≤ -{lfc_threshold:g})", 7),
            "up": (d["high"], f"up (lfc ≥ {lfc_threshold:g})", 7)}
    fig = go.Figure()
    for k in ("unchanged", "down", "up"):
        s = change[cls == k]
        if not len(s):
            continue
        col, name, size = spec[k]
        fig.add_trace(go.Scattergl(x=s["avg_log10"], y=s["lfc"], mode="markers", text=s["hgnc_symbol"], name=name,
                                   marker={"color": col, "size": size},
                                   customdata=np.column_stack([s["value_a"], s["value_b"]]),
                                   hovertemplate="%{text}<br>lfc %{y:.2f}<br>a %{customdata[0]:.3g} → "
                                                 "b %{customdata[1]:.3g}<extra></extra>"))
    lab = change[change["avg_log10"] >= label_min_avg]
    lab = pd.concat([lab.sort_values("lfc", ascending=False).head(label_top), lab.sort_values("lfc").head(label_top)])
    lab = lab[lab["lfc"].abs() >= lfc_threshold]
    if highlight:
        lab = pd.concat([change[change["hgnc_symbol"].isin(highlight)], lab])
    lab = lab.drop_duplicates(subset="hgnc_symbol")
    xr = float(np.ptp(change["avg_log10"])) if len(change) else 1.0
    yr = float(np.ptp(change["lfc"])) if len(change) else 1.0
    kept: List[tuple] = []
    ann = []
    for _, r in lab.iterrows():
        if any(abs(r["avg_log10"] - x) < 0.06 * xr and abs(r["lfc"] - y) < 0.05 * yr for x, y in kept):
            continue
        kept.append((r["avg_log10"], r["lfc"]))
        ann.append({"x": r["avg_log10"], "y": r["lfc"], "text": r["hgnc_symbol"], "showarrow": True, "arrowhead": 0,
                    "arrowwidth": 1, "arrowcolor": th["text_secondary"], "ax": 18,
                    "ay": -16 if r["lfc"] > 0 else 16, "font": {"size": 11, "color": th["text_primary"]}})

    def guide(y):
        return {"type": "line", "xref": "paper", "x0": 0, "x1": 1, "y0": y, "y1": y,
                "line": {"color": th["text_secondary"], "width": 1, "dash": "solid" if y == 0 else "dot"}}

    return _layout(fig, title, annotations=ann, hovermode="closest",
                   shapes=[guide(0), guide(lfc_threshold), guide(-lfc_threshold)],
                   xaxis={"title": {"text": "average expression, log10(1 + x)"}},
                   yaxis={"title": {"text": "log2 fold change"}})


def plot_fold_change(change: pd.DataFrame, genes: Optional[Sequence[str]] = None,
                     title: str = "Change in gene expression") -> go.Figure:
    """Diverging fold-change bars for selected genes (default: top 15 up and 15
    down), from analysis.compare_samples()."""
    if genes is None:
        s = change.sort_values("lfc", kind="stable")
        genes = list(dict.fromkeys(list(s["hgnc_symbol"].head(15)) + list(s["hgnc_symbol"].tail(15))))
    s = change[change["hgnc_symbol"].isin(genes)].sort_values("lfc", kind="stable")
    d = plot_theme()["diverging"]
    fig = go.Figure(go.Bar(x=s["lfc"], y=s["hgnc_symbol"], orientation="h",
                           marker={"color": [d["high"] if v >= 0 else d["low"] for v in s["lfc"]]},
                           hovertemplate="%{y}: %{x:.2f}<extra></extra>"))
    fig.update_layout(height=140 + 22 * len(s))
    return _layout(fig, title, showlegend=False, xaxis={"title": {"text": "log2 fold change"}},
                   yaxis={"title": {"text": ""}, "type": "category", "categoryorder": "array",
                          "categoryarray": list(s["hgnc_symbol"])})
