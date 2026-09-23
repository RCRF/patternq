"""Broad-scale descriptive expression analysis, backported from the Clojure
variant-forensics reports (unify-central/analysis). Mirrors
R/patternq/R/analysis_expression.R.

  - a sample vs a reference cohort: z-scores, percentile ranks, top genes
    (quantitative.clj rank-expression, util.clj percentile-rank)
  - geneset views of samples against cohort distributions
    (util.clj examine-geneset*, plot-genex-vs-cohort*)
  - two-sample change: log fold change and MA plots
    (quantitative.clj log-fold-change, H37001 gene_expression_change.clj)
  - ssGSEA, top varying genes, expression distances / nearest samples

Everything works on data returned by dataset.gene_expression(); the only
queries are gene_expression() calls (batched over genes for cohorts).

    import patternq.analysis as pqa
"""
from functools import lru_cache
from importlib import resources
from typing import Dict, List, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd

from patternq import config
from patternq import dataset as pqd
from patternq import results as pqres

# -- gene sets -----------------------------------------------------------------


@lru_cache(maxsize=None)
def _load_genesets() -> Dict[str, tuple]:
    out = {}
    d = resources.files("patternq").joinpath("genesets")
    for f in sorted(d.iterdir(), key=lambda p: p.name):
        if f.name.endswith(".txt"):
            genes = [g.strip() for g in f.read_text().splitlines()]
            out[f.name[:-4]] = tuple(g for g in genes if g)
    return out


def genesets() -> Dict[str, List[str]]:
    """Gene sets shipped with patternq: hallmark sets (apoptosis, DNA repair,
    hypoxia, inflammatory), antibody therapy targets, housekeeping genes,
    germline multi-cancer panel, melanoma phenotype sets, neural crest and adult
    kidney reference sets."""
    return {k: list(v) for k, v in _load_genesets().items()}


def geneset(name: str) -> List[str]:
    """One gene set by name (see genesets())."""
    gs = _load_genesets()
    if name not in gs:
        raise KeyError(f"Unknown gene set '{name}'")
    return list(gs[name])


# -- sample vs cohort ----------------------------------------------------------

def sample_expression(sample: str, db: Optional[str] = None, measurement: str = "tpm",
                      measurement_set: Optional[str] = None,
                      genes: Optional[Sequence[str]] = None) -> pd.Series:
    """Expression of one sample as a Series (HGNC symbol -> value; values of
    several gene products of the same gene are summed)."""
    gx = pqd.gene_expression(db, genes=genes, samples=[sample], measurement=measurement,
                             measurement_set=measurement_set)
    if not len(gx):
        return pd.Series(dtype=float)
    s = gx.groupby("hgnc_symbol")["value"].sum().astype(float)
    s.attrs = dict(gx.attrs)
    return s


def percentile_rank(values: Sequence[float], x) -> Union[float, np.ndarray]:
    """Mid-rank percentile of x within values: (count below + half the ties) / n,
    as a percentage in [0, 100]. NaN if values is empty."""
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    scalar = np.isscalar(x)
    xs = np.atleast_1d(np.asarray(x, dtype=float))
    if not len(v):
        out = np.full(len(xs), np.nan)
    else:
        out = np.array([100 * ((v < xi).sum() + 0.5 * (v == xi).sum()) / len(v) for xi in xs])
    return float(out[0]) if scalar else out


def _log_tr(v):
    return np.log2(1 + np.maximum(np.asarray(v, dtype=float), 0))


def compare_to_cohort(sample: str, db: Optional[str] = None, cohort_db: str = None, measurement: str = "tpm",
                      cohort_measurement: Optional[str] = None, genes: Optional[Sequence[str]] = None,
                      measurement_set: Optional[str] = None, cohort_measurement_set: Optional[str] = None,
                      log: bool = True, fill_missing: bool = True, anchor_gene: str = "GAPDH",
                      sd_floor: float = 0.25, batch_size: int = 2000, min_cohort: int = 5) -> pd.DataFrame:
    """Place a sample's expression within a reference cohort's distribution, per
    gene: cohort mean/sd/median on log2(1 + x) scale (or raw with log=False),
    z-score and percentile rank. The cohort is streamed in gene batches.

    Measurements must be comparable (same units / normalization) between the
    sample's database and the cohort's; TPM is the usual common ground.

    fill_missing: count a gene with no stored value in a cohort sample as 0.
      Imports often omit zero measurements, so without this a gene's cohort
      distribution is built from only the samples that express it. Cohort
      samples are those with a value for anchor_gene.
    sd_floor: minimum cohort sd (comparison scale), so near-constant genes do
      not produce enormous z-scores.
    min_cohort: minimum cohort values for a gene to be scored.

    Returns hgnc_symbol, value, z, percentile, cohort_n (cohort size),
    cohort_observed (cohort samples with a stored value; low coverage of a gene
    that is usually expressed points to an import or annotation problem),
    cohort_mean, cohort_sd, cohort_median; ordered by |z|. Comparison details
    are in df.attrs["patternq_comparison"]."""
    if cohort_db is None:
        raise ValueError("cohort_db is required")
    cohort_measurement = cohort_measurement or measurement
    x = sample_expression(sample, db=db, measurement=measurement, measurement_set=measurement_set, genes=genes)
    if not len(x):
        raise ValueError(f"No {measurement} expression for sample {sample}")
    tr = _log_tr if log else (lambda v: np.asarray(v, dtype=float))
    cohort_samples: Optional[List[str]] = None
    if fill_missing:
        anchor = pqd.gene_expression(cohort_db, genes=[anchor_gene], measurement=cohort_measurement,
                                     measurement_set=cohort_measurement_set)
        cohort_samples = sorted(anchor["sample_id"].unique()) if len(anchor) else []
        if not cohort_samples:
            raise ValueError(f"No {cohort_measurement} values for {anchor_gene} in {cohort_db}; "
                             "pass anchor_gene or fill_missing=False")
    zero = float(tr([0.0])[0])
    rows = []
    names = list(x.index)
    for i in range(0, len(names), batch_size):
        batch = names[i:i + batch_size]
        cg = pqd.gene_expression(cohort_db, genes=batch, measurement=cohort_measurement,
                                 measurement_set=cohort_measurement_set)
        if not len(cg):
            continue
        cg = cg.groupby(["sample_id", "hgnc_symbol"], as_index=False)["value"].sum()
        for g, grp in cg.groupby("hgnc_symbol"):
            vals = tr(grp["value"].to_numpy())
            observed = len(vals)
            if cohort_samples is not None:
                vals = np.concatenate([vals, np.full(max(0, len(cohort_samples) - observed), zero)])
            xv = float(tr([x[g]])[0])
            rows.append({"hgnc_symbol": g, "value": float(x[g]), "cohort_n": len(vals),
                         "cohort_observed": observed, "cohort_mean": float(np.mean(vals)),
                         "cohort_sd": float(np.std(vals, ddof=1)) if len(vals) > 1 else np.nan,
                         "cohort_median": float(np.median(vals)),
                         "percentile": percentile_rank(vals, xv)})
    cols = ["hgnc_symbol", "value", "z", "percentile", "cohort_n", "cohort_observed", "cohort_mean",
            "cohort_sd", "cohort_median"]
    out = pd.DataFrame(rows, columns=[c for c in cols if c != "z"])
    out = out[out["cohort_n"] >= min_cohort].copy()
    sd = np.maximum(out["cohort_sd"].fillna(0).to_numpy(), sd_floor)
    out["z"] = (tr(out["value"].to_numpy()) - out["cohort_mean"].to_numpy()) / sd
    out = out[cols]
    out = out.iloc[np.argsort(-np.abs(out["z"].to_numpy()), kind="stable")].reset_index(drop=True)
    out.attrs = {"patternq_provenance": pqres.provenance(x),
                 "patternq_comparison": {"sample": sample, "db": config.ensure_db(db), "cohort_db": cohort_db,
                                         "measurement": measurement, "cohort_measurement": cohort_measurement,
                                         "log": log, "cohort_size": len(cohort_samples or [])}}
    return out


def top_by_zscore(comparison: pd.DataFrame, n: int = 25, direction: str = "both", min_value: float = 1,
                  min_observed: float = 0.5) -> pd.DataFrame:
    """Top genes by z-score from compare_to_cohort().

    direction: "both" (largest |z|), "up" or "down".
    min_value: ignore genes whose sample value and cohort median are both below
      this (noise among barely-expressed genes).
    min_observed: minimum fraction of the cohort with a stored value for the gene
      (guards against genes missing or mis-annotated in the cohort's import
      dominating the ranking)."""
    if direction not in ("both", "up", "down"):
        raise ValueError("direction must be 'both', 'up' or 'down'")
    cmp = comparison[comparison["z"].notna()]
    if "cohort_observed" in cmp.columns:
        cmp = cmp[cmp["cohort_observed"] >= min_observed * cmp["cohort_n"]]
    expressed = (cmp["value"] >= min_value) | ((2 ** cmp["cohort_median"] - 1) >= min_value)
    cmp = cmp[expressed]
    if direction == "both":
        cmp = cmp.iloc[np.argsort(-np.abs(cmp["z"].to_numpy()), kind="stable")]
    elif direction == "up":
        cmp = cmp.sort_values("z", ascending=False, kind="stable")
        cmp = cmp[cmp["z"] > 0]
    else:
        cmp = cmp.sort_values("z", kind="stable")
        cmp = cmp[cmp["z"] < 0]
    out = cmp.head(n).reset_index(drop=True)
    out.attrs = dict(comparison.attrs)
    return out


# -- two samples ---------------------------------------------------------------

def log_fold_change(a: Mapping[str, float], b: Mapping[str, float], pseudocount: float = 1) -> pd.DataFrame:
    """lfc = log2((b + pseudocount) / (a + pseudocount)); avg_log10 (the MA
    plot's x) is the mean of log10(1 + a) and log10(1 + b), as in the Clojure
    reports. Genes missing in one profile are 0.

    Returns hgnc_symbol, value_a, value_b, lfc, avg_log10."""
    a = pd.Series(a, dtype=float)
    b = pd.Series(b, dtype=float)
    genes = list(dict.fromkeys(list(a.index) + list(b.index)))
    va = a.reindex(genes).fillna(0).to_numpy()
    vb = b.reindex(genes).fillna(0).to_numpy()
    return pd.DataFrame({"hgnc_symbol": genes, "value_a": va, "value_b": vb,
                         "lfc": np.log2((vb + pseudocount) / (va + pseudocount)),
                         "avg_log10": (np.log10(1 + va) + np.log10(1 + vb)) / 2})


def compare_samples(sample_a: str, sample_b: str, db: Optional[str] = None, db_b: Optional[str] = None,
                    measurement: str = "tpm", min_avg: float = 0.5, pseudocount: float = 1) -> pd.DataFrame:
    """Expression change from sample_a (reference, e.g. baseline) to sample_b:
    log_fold_change() output, genes with avg_log10 >= min_avg, ordered by lfc
    (descending). Use db_b if sample_b lives in another database."""
    a = sample_expression(sample_a, db=db, measurement=measurement)
    b = sample_expression(sample_b, db=db_b or db, measurement=measurement)
    out = log_fold_change(a, b, pseudocount=pseudocount)
    out = out[out["avg_log10"] >= min_avg].sort_values("lfc", ascending=False, kind="stable").reset_index(drop=True)
    out.attrs = {"patternq_provenance": pqres.provenance(a),
                 "patternq_comparison": {"sample_a": sample_a, "sample_b": sample_b, "measurement": measurement}}
    return out


# -- gene sets and profiles ----------------------------------------------------

def ssgsea_score(x: Mapping[str, float], gene_set: Sequence[str], alpha: float = 0.25) -> float:
    """Single-sample GSEA enrichment score (Barbie et al. 2009): genes ranked by
    expression; sums, over the ranked list, the difference between the weighted
    empirical distribution of the gene set (weights rank^alpha, highest
    expression = highest rank) and the distribution of the other genes.

    Note: the Clojure report version weights by the inverse rank (highest
    expression = rank 1); this follows the published method. NaN when none or
    all genes are in the set."""
    s = pd.Series(x, dtype=float).dropna()
    vals = s.to_numpy()
    n = len(vals)
    # rank ties by first occurrence (R rank(ties.method = "first"))
    asc = np.argsort(vals, kind="stable")
    rank = np.empty(n)
    rank[asc] = np.arange(1, n + 1)
    o = np.argsort(-vals, kind="stable")
    hit = np.isin(s.index.to_numpy()[o], list(gene_set))
    nh = int(hit.sum())
    if nh == 0 or nh == n:
        return float("nan")
    w = np.where(hit, rank[o] ** alpha, 0.0)
    p_hit = np.cumsum(w) / w.sum()
    p_miss = np.cumsum(~hit) / (n - nh)
    return float((p_hit - p_miss).sum())


def ssgsea(m: pd.DataFrame, gene_sets: Optional[Mapping[str, Sequence[str]]] = None,
           alpha: float = 0.25) -> pd.DataFrame:
    """ssGSEA scores, samples x gene sets. m: samples x genes (e.g.
    context.to_matrix(gene_expression(...), col="hgnc_symbol"))."""
    gene_sets = gene_sets if gene_sets is not None else genesets()
    return pd.DataFrame({name: [ssgsea_score(row, gs, alpha) for _, row in m.iterrows()]
                         for name, gs in gene_sets.items()}, index=m.index)


def top_varying_genes(m: pd.DataFrame, n: int = 500) -> pd.DataFrame:
    """Most variable genes (variance of log2(1 + x) across samples): hgnc_symbol,
    mean, variance."""
    lg = np.log2(1 + m.clip(lower=0))
    out = pd.DataFrame({"hgnc_symbol": m.columns, "mean": lg.mean(axis=0).to_numpy(),
                        "variance": lg.var(axis=0, ddof=1).to_numpy()})
    return out.sort_values("variance", ascending=False, kind="stable").head(n).reset_index(drop=True)


def expression_distance(a: Mapping[str, float], b: Mapping[str, float], method: str = "cosine") -> float:
    """Distance between profiles: "cosine" (1 - cosine similarity) or
    "euclidean". Genes of a missing from b count as 0 (as in the Clojure
    reports)."""
    a = pd.Series(a, dtype=float).dropna()
    b = pd.Series(b, dtype=float).dropna()
    bb = b.reindex(a.index).fillna(0)
    if method == "euclidean":
        return float(np.sqrt(((a - bb) ** 2).sum()))
    if method != "cosine":
        raise ValueError("method must be 'cosine' or 'euclidean'")
    ma, mb = np.sqrt((a ** 2).sum()), np.sqrt((b ** 2).sum())
    if ma == 0 or mb == 0:
        return 1.0
    return float(1 - (a * bb).sum() / (ma * mb))


def nearest_samples(x: Mapping[str, float], m: pd.DataFrame, method: str = "cosine", n: int = 10) -> pd.DataFrame:
    """Samples of m (samples x genes) nearest to profile x: sample_id, distance.

    Caution (from the Clojure reports): raw-expression nearest neighbours are
    sensitive to batch, vendor and pipeline effects; compare within a
    consistently processed cohort."""
    d = [expression_distance(x, row.fillna(0), method) for _, row in m.iterrows()]
    out = pd.DataFrame({"sample_id": list(m.index), "distance": d})
    return out.sort_values("distance", kind="stable").head(n).reset_index(drop=True)


# -- data for geneset plots ----------------------------------------------------

def examine_geneset(genes: Union[str, Sequence[str]], samples: Sequence[str], db: Optional[str] = None,
                    cohort_dbs: Union[str, Sequence[str], Mapping[str, str]] = (), measurement: str = "tpm",
                    cohort_measurement: Optional[str] = None, type: str = "violin",
                    title: Optional[str] = None, **kwargs):
    """Samples vs reference cohort(s) for a gene set: fetches expression for
    genes (or a gene set name from genesets()) in the samples and each cohort
    and draws plots.plot_vs_cohort. cohort_dbs: a database name, a list, or a
    dict {label: database}. Port of the Clojure reports' examine-geneset family.
    Returns a plotly Figure."""
    from patternq import plots as pqp
    if isinstance(genes, str):
        if genes in _load_genesets():
            title = title or genes
            genes = geneset(genes)
        else:
            genes = [genes]
    if isinstance(cohort_dbs, str):
        cohort_dbs = [cohort_dbs]
    labelled = dict(cohort_dbs) if isinstance(cohort_dbs, Mapping) else {d: d for d in cohort_dbs}
    se = pqd.gene_expression(db, genes=list(genes), samples=list(samples), measurement=measurement)
    parts = []
    for label, cdb in labelled.items():
        d = pqd.gene_expression(cdb, genes=list(genes), measurement=cohort_measurement or measurement)
        if len(d):
            d["cohort"] = label
            parts.append(d)
    ce = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["sample_id", "hgnc_symbol", "measurement_set", "value", "cohort"])
    return pqp.plot_vs_cohort(se, ce, type=type, title=title or "Samples vs cohort expression",
                              xlab=f"{measurement} (log scale)", **kwargs)
