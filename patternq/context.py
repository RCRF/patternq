"""Reshaping measurement tables and joining context (samples, subjects,
variants, CNVs), ported from wick. Mirrors R/patternq/R/context.R."""
from typing import Dict, List, Optional, Sequence, Union

import pandas as pd

from patternq import dataset as pqd
from patternq import reference as pqref
from patternq import results as pqres


def to_matrix(df: pd.DataFrame, col: Union[str, Sequence[str]], row: str = "sample_id",
              value: str = "value", aggfunc="mean") -> pd.DataFrame:
    """Long -> wide: rows x cols DataFrame (several col columns are joined with
    "|"), NaN where missing, duplicates aggregated with aggfunc."""
    cols = [col] if isinstance(col, str) else list(col)
    d = df.copy()
    if len(cols) > 1:
        d["_col"] = d[cols].astype(str).agg("|".join, axis=1)
        key = "_col"
    else:
        key = cols[0]
    m = d.pivot_table(index=row, columns=key, values=value, aggfunc=aggfunc)
    m.columns.name = None
    return m.sort_index().sort_index(axis=1)


def to_long(m: pd.DataFrame, row_name: str = "sample_id", col_name: str = "target",
            value_name: str = "value") -> pd.DataFrame:
    """Wide -> long, dropping missing cells."""
    out = m.rename_axis(row_name).reset_index().melt(id_vars=row_name, var_name=col_name, value_name=value_name)
    return out.dropna(subset=[value_name]).reset_index(drop=True)


def split_by_measurement_set(df: pd.DataFrame, col: Optional[Sequence[str]] = None, wide: bool = True,
                             aggfunc="mean") -> Dict[str, pd.DataFrame]:
    """One element per measurement set (wick's group_by_assay_meas_set),
    optionally as samples x target matrices."""
    parts = {k: g for k, g in df.groupby("measurement_set")}
    if not wide:
        return parts
    if col is None:
        col = [c for c in df.columns if c not in ("sample_id", "measurement_set", "value")]
    return {k: to_matrix(g, col=col, aggfunc=aggfunc) for k, g in parts.items()}


def select_targets(m: pd.DataFrame, include: Optional[Sequence[str]] = None,
                   exclude: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """Keep or drop matrix columns."""
    if include is not None:
        m = m.loc[:, [c for c in m.columns if c in set(include)]]
    if exclude is not None:
        m = m.loc[:, [c for c in m.columns if c not in set(exclude)]]
    return m


def _ensure_long(tab: pd.DataFrame, col_name: str = "target") -> pd.DataFrame:
    if "sample_id" not in tab.columns and tab.index.name in (None, "sample_id") and tab.shape[1] > 0 \
            and all(pd.api.types.is_numeric_dtype(t) for t in tab.dtypes):
        return pqres.keep_provenance(to_long(tab, col_name=col_name), tab)
    return tab


def _merge_new(tab: pd.DataFrame, other: pd.DataFrame, on: str) -> pd.DataFrame:
    other = other[[on] + [c for c in other.columns if c not in tab.columns]]
    return pqres.keep_provenance(tab.merge(other, on=on, how="left"), tab)


def add_subject_context(tab: pd.DataFrame, db: Optional[str] = None,
                        include_outcomes: bool = False) -> pd.DataFrame:
    """Join subject attributes (and optionally subject_outcomes) by subject_id."""
    if "subject_id" not in tab.columns:
        raise ValueError("tab has no subject_id column")
    out = _merge_new(tab, pqd.subjects(db), "subject_id")
    if include_outcomes:
        out = _merge_new(out, pqd.subject_outcomes(db), "subject_id")
    return out


def add_sample_context(tab: pd.DataFrame, db: Optional[str] = None, include_subjects: bool = True,
                       include_outcomes: bool = False) -> pd.DataFrame:
    """Join sample attributes (and subject attributes / outcomes) by sample_id.
    Wide matrices are converted to long format first."""
    tab = _ensure_long(tab)
    out = _merge_new(tab, pqd.samples(db), "sample_id")
    if include_subjects and "subject_id" in out.columns:
        out = add_subject_context(out, db=db, include_outcomes=include_outcomes)
    return out


def add_variant_context(tab: pd.DataFrame, db: Optional[str] = None) -> pd.DataFrame:
    """Join variant annotations by variant_id."""
    tab = _ensure_long(tab, col_name="variant_id")
    if "variant_id" not in tab.columns:
        raise ValueError("tab has no variant_id column")
    va = pqref.variant_annotations(db, variant_ids=sorted(tab["variant_id"].dropna().unique()))
    return _merge_new(tab, va, "variant_id")


def add_cnv_context(tab: pd.DataFrame, db: Optional[str] = None) -> pd.DataFrame:
    """Join CNV entities by cnv_id."""
    tab = _ensure_long(tab, col_name="cnv_id")
    if "cnv_id" not in tab.columns:
        raise ValueError("tab has no cnv_id column")
    return _merge_new(tab, pqref.cnvs(db), "cnv_id")


_LEVELS = ["kingdom", "phylum", "class", "order", "family", "genus", "species"]


def _taxonomy_levels(taxa: pd.DataFrame) -> List[str]:
    prefix = "otu_" if any(c.startswith("otu_") for c in taxa.columns) else "sgb_"
    return [prefix + lv for lv in _LEVELS if prefix + lv in taxa.columns]


def deduplicate_taxonomy(taxa: pd.DataFrame, na_value: str = "Unclassified", sep: str = "_") -> pd.DataFrame:
    """Make taxon names unique at each level by prefixing ancestors where the
    same name occurs under different parents (OTU or SGB tables)."""
    levels = _taxonomy_levels(taxa)
    taxa = taxa.copy()
    out = taxa.copy()
    for i, cur in enumerate(levels):
        out[cur] = out[cur].fillna(na_value)
        taxa[cur] = taxa[cur].fillna(na_value)
        uniq = taxa[levels[:i + 1]].drop_duplicates()
        dup = set(uniq.loc[uniq[cur].duplicated(), cur])
        w = out[cur].isin(dup)
        if w.any():
            out.loc[w, cur] = taxa.loc[w, levels[:i + 1]].astype(str).agg(sep.join, axis=1)
    return out


def aggregate_taxa(tab: pd.DataFrame, taxa: pd.DataFrame, id_col: str, normalize: bool = True,
                   wide: bool = False, na_value: str = "Unclassified") -> Dict[str, pd.DataFrame]:
    """Sum measurements to each taxonomic level, optionally per-sample
    proportions; one element per level."""
    taxa = deduplicate_taxonomy(taxa, na_value=na_value)
    tab = tab.dropna(subset=["value"]).merge(taxa, on=id_col)
    out = {}
    for cur in _taxonomy_levels(taxa):
        m = tab.groupby(["sample_id", cur], as_index=False)["value"].sum().rename(columns={cur: "taxon"})
        if normalize:
            m["value"] = m["value"] / m.groupby("sample_id")["value"].transform("sum")
        if wide:
            m = to_matrix(m, col="taxon", aggfunc="sum").fillna(0)
        out[cur] = m
    return out
