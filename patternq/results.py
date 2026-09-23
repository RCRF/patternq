"""Converting query results to DataFrames, and result provenance."""
import re
from collections import namedtuple
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

PatternQProvenance = namedtuple("PatternQProvenance", ["db", "basis_t", "timestamp"])


def clean_name(x: str) -> str:
    """"?sample-id" -> "sample_id"; ":subject/id" -> "subject_id"."""
    x = re.sub(r"^[?:]", "", str(x))
    return re.sub(r"[-/.]", "_", x)


def ident_name(x):
    """":variant.impact/high" -> "high"; other values unchanged."""
    if isinstance(x, str) and re.match(r"^:[^/]+/", x):
        return re.sub(r"^:[^/]+/", "", x)
    return x


def _is_pull(elem) -> bool:
    return isinstance(elem, (list, tuple)) and len(elem) > 0 and elem[0] == "pull"


def find_column_names(find: List[Any]) -> List[str]:
    names = []
    for e in find:
        if isinstance(e, (list, tuple)):
            names.append("_".join(clean_name(x) for x in e if not isinstance(x, (list, dict))))
        else:
            names.append(clean_name(e))
    seen: Dict[str, int] = {}
    out = []
    for n in names:
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            out.append(n)
    return out


def _is_ident_map(v) -> bool:
    return isinstance(v, dict) and list(v.keys()) == [":db/ident"]


def flatten_pull(m: Dict[str, Any], exclude_ids: bool = True) -> Dict[str, Any]:
    """Flatten one pulled entity into columns.

    Scalar attributes become columns named after the attribute (":subject/id" ->
    "subject_id"). Enum refs ({":db/ident": ...}) become the enum name without
    namespace in a column named after the referring attribute. Nested single
    refs are flattened recursively (colliding names are prefixed with the
    referring attribute). Cardinality-many values stay as lists."""
    out: Dict[str, Any] = {}
    if not isinstance(m, dict):
        return out
    for k, v in m.items():
        if exclude_ids and (k == ":db/id" or k.endswith("/uid")):
            continue
        if v is None:
            continue
        col = clean_name(k)
        if _is_ident_map(v):
            out[col] = ident_name(v[":db/ident"])
        elif isinstance(v, dict) and list(v.keys()) == [":db/id"]:
            if not exclude_ids:
                out[col] = v[":db/id"]
        elif isinstance(v, dict):
            for nk, nv in flatten_pull(v, exclude_ids).items():
                out[nk if nk not in out else f"{col}_{nk}"] = nv
        elif isinstance(v, list):
            if all(_is_ident_map(e) for e in v):
                out[col] = [ident_name(e[":db/ident"]) for e in v]
            elif all(not isinstance(e, (dict, list)) for e in v):
                out[col] = v
            else:
                flat = [flatten_pull(e, exclude_ids) if isinstance(e, dict) else e for e in v]
                # refs to entities pulled for a single attribute -> plain values
                if all(isinstance(e, dict) and len(e) == 1 for e in flat):
                    flat = [next(iter(e.values())) for e in flat]
                out[col] = flat
        else:
            out[col] = v
    return out


def result_to_df(rows: List[List[Any]], find: List[Any], exclude_ids: bool = True) -> pd.DataFrame:
    """Query result rows -> DataFrame, flattening pull expressions."""
    names = find_column_names(find)
    pulls = [_is_pull(e) for e in find]
    if not any(pulls):
        return pd.DataFrame(rows, columns=names)
    recs = []
    for r in rows:
        rec: Dict[str, Any] = {}
        for j, v in enumerate(r):
            if pulls[j]:
                rec.update(flatten_pull(v, exclude_ids))
            else:
                rec[names[j]] = v
        # as in R: single-element cardinality-many values read as scalars
        recs.append({k: (v[0] if isinstance(v, list) and len(v) == 1 and not isinstance(v[0], (dict, list))
                         else v) for k, v in rec.items()})
    return pd.DataFrame.from_records(recs)


def order_columns(df: pd.DataFrame, first: List[str]) -> pd.DataFrame:
    """Known columns first, in order; others after. Keeps provenance."""
    first = [c for c in first if c in df.columns]
    out = df[first + [c for c in df.columns if c not in first]]
    prov = provenance(df)
    if prov is not None:
        out.attrs["patternq_provenance"] = prov
    return out


def add_provenance(df: pd.DataFrame, qres: Dict[str, Any]) -> pd.DataFrame:
    """Attach provenance (db, basis_t, client timestamp) in DataFrame.attrs."""
    df.attrs["patternq_provenance"] = PatternQProvenance(
        qres.get("db_name"), qres.get("basis_t"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return df


def provenance(df) -> Optional[PatternQProvenance]:
    """Provenance of a result returned by patternq, or None."""
    try:
        return df.attrs.get("patternq_provenance")
    except AttributeError:
        return None


def join_many(v, sep: str = "; "):
    """Collapse a cardinality-many value (list) to a string; scalars unchanged."""
    if isinstance(v, list):
        return sep.join(str(x) for x in v) if v else None
    return v


def keep_provenance(df: pd.DataFrame, source) -> pd.DataFrame:
    """Copy provenance from a source result to a derived DataFrame."""
    prov = provenance(source)
    if prov is not None:
        df.attrs["patternq_provenance"] = prov
    return df
