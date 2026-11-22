"""Canned queries over a dataset database.

Every dataset is its own database, so queries start from samples, subjects,
assays and measurement sets directly; there is no dataset argument. Each main
function has a *_query() companion returning (query, args), so the query can be
inspected, modified or run elsewhere.

Mirrors the R library (R/patternq/R/queries_*.R); see PARITY.md.
"""
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from patternq import backpressure as bp
from patternq import config
from patternq import query as pqq
from patternq import results as pqres

VariantImpact = Literal["modifier", "low", "moderate", "high"]
RNASeqMeasurementAttribute = Literal[
    "tpm", "fpkm", "fpkm-upper-quartile", "rpkm", "rsem-normalized-count",
    "kallisto-abundance", "rsem-raw-count", "rsem-scaled-estimate", "read-count"]

QueryAndArgs = Tuple[dict, List[Any]]


def _mattr(measurement: str) -> str:
    return ":measurement/" + measurement.lstrip(":").replace("measurement/", "")


def _run(q: QueryAndArgs, db, **kwargs) -> pd.DataFrame:
    return pqq.do_query(q[0], q[1], db=db, **kwargs)


# Starting from a few samples walks only their measurements. A subject can
# own most of a dataset (a single-participant deep dive has one subject and
# every sample), so subject-only queries start from the measurement attribute
# instead.
_SAMPLE_LEAD = [["?s", ":sample/id", "?sample-id"], ["?m", ":measurement/sample", "?s"]]


def _lead_with(where: List[Any], lead: List[Any]) -> List[Any]:
    """`where` reordered to start from `lead`, the clauses of the subset the
    caller bound (genes, samples, subjects), then each remaining clause as soon
    as it shares a variable with those already placed. Datomic joins in clause
    order, so this keeps a query from starting on a whole attribute (every
    variant, CNV, or expression measurement) and filtering at the end."""
    def vars_of(c):
        return {x for x in c if isinstance(x, str) and x.startswith("?")}
    out = [c for c in lead]
    bound = set().union(*map(vars_of, out)) if out else set()
    rest = [c for c in where if c not in lead]
    while rest:
        i = next((k for k, c in enumerate(rest) if vars_of(c) & bound), 0)
        c = rest.pop(i)
        out.append(c)
        bound |= vars_of(c)
    return out




# -- samples & subjects --------------------------------------------------------

def samples_query() -> QueryAndArgs:
    q = {":find": [["pull", "?s", ["*",
                                   {":sample/subject": [":subject/id"]},
                                   {":sample/timepoint": [":timepoint/id", ":timepoint/relative-order"]},
                                   {":sample/specimen": [":db/ident"]},
                                   {":sample/type": [":db/ident"]},
                                   {":sample/container": [":db/ident"]},
                                   {":sample/study-day": [":study-day/id"]},
                                   {":sample/gdc-anatomic-site": [":gdc-anatomic-site/name"]}]]],
         ":where": [["?s", ":sample/id"]]}
    return q, []


def samples(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per sample: sample_id, subject_id, timepoint_id and the other
    sample attributes present."""
    return _run(samples_query(), db, **kwargs)


def subjects_query() -> QueryAndArgs:
    q = {":find": [["pull", "?s", ["*",
                                   {":subject/sex": [":db/ident"]},
                                   {":subject/race": [":db/ident"]},
                                   {":subject/ethnicity": [":db/ident"]},
                                   {":subject/smoker": [":db/ident"]},
                                   {":subject/cause-of-death": [":db/ident"]},
                                   {":subject/meddra-disease": [":meddra-disease/preferred-name"]}]]],
         ":where": [["?s", ":subject/id"]]}
    return q, []


def subjects(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per subject: subject_id and demographic attributes present
    (enums as names, e.g. subject_sex = "female")."""
    return _run(subjects_query(), db, **kwargs)


# -- dataset structure ---------------------------------------------------------

def dataset_summary_query() -> QueryAndArgs:
    q = {":find": ["?assay-name", "?assay-technology", "?measurement-set-name"],
         ":where": [["?a", ":assay/name", "?assay-name"],
                    ["?a", ":assay/technology", "?t"],
                    ["?t", ":db/ident", "?assay-technology"],
                    ["?a", ":assay/measurement-sets", "?ms"],
                    ["?ms", ":measurement-set/name", "?measurement-set-name"]]}
    return q, []


def dataset_summary(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per measurement set: assay_name, assay_technology,
    measurement_set_name."""
    df = _run(dataset_summary_query(), db, **kwargs)
    df["assay_technology"] = df["assay_technology"].map(pqres.ident_name)
    out = df.sort_values(["assay_name", "measurement_set_name"]).reset_index(drop=True)
    return pqres.keep_provenance(out, df)


# A query that binds every measurement of a set (count, sample, [?m ?a]) makes
# the server hold the whole set in memory before any aggregate runs; on large
# expression sets that runs the service out of heap. The calls below read the
# set's measurement ids from the datoms index instead, in bounded pages, and
# query only those ids.
_PAGE = 10000  # the server's datoms page cap; each page is also an audit write


def _measurement_set_eids(measurement_set: str, db: str, **kwargs) -> List[int]:
    r = pqq.do_query({":find": ["?ms"], ":in": ["?ms-name"],
                      ":where": [["?ms", ":measurement-set/name", "?ms-name"]]},
                     [measurement_set], db=db, **kwargs)
    if not len(r):
        raise ValueError(f"No measurement set named {measurement_set!r} in {db}")
    return [int(e) for e in r["ms"]]


def _set_measurement_ids(ms: int, db: str, limit: int, offset: int = 0) -> Tuple[List[int], bool]:
    """Up to `limit` measurement ids of set `ms` in index order, starting at
    `offset`, and whether they reach the end of the set."""
    ids: List[int] = []
    while len(ids) < limit:
        want = min(_PAGE, limit - len(ids))
        page = pqq.datoms("eavt", [ms, ":measurement-set/measurements"], db=db,
                          offset=offset + len(ids), limit=want)
        ids += [int(v) for v in page["v"]]
        if len(page) < want:
            return ids, True
    return ids, not len(pqq.datoms("eavt", [ms, ":measurement-set/measurements"], db=db,
                                   offset=offset + len(ids), limit=1))


def _count_set_measurements(ms: int, db: str, cap: int) -> Tuple[int, bool]:
    """(count, exact): the set's measurement count when it is at most `cap`,
    else (cap, False). One page for a small set. A set filling the first page
    gets one probe past the cap, then one-datom probes binary-search the
    10000-wide block it ends in, and one page of that block finishes the count:
    about six round trips for any size, instead of a page per 10000."""
    def page(offset, limit):
        return len(pqq.datoms("eavt", [ms, ":measurement-set/measurements"], db=db, offset=offset, limit=limit))
    step = 10000
    n = page(0, step)
    if n < step:
        return n, True
    if page(cap, 1):
        return cap, False
    lo, hi = 1, -(-cap // step)  # a datom at lo * step; none at hi * step (or past the cap)
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if page(mid * step, 1):
            lo = mid
        else:
            hi = mid
    return lo * step + page(lo * step, step), True


def measurement_sets(db: Optional[str] = None, count_cap: int = 100000, **kwargs) -> pd.DataFrame:
    """dataset_summary plus measurement_count per measurement set, and
    measurement_count_exact: False when the set has more than count_cap
    measurements, in which case measurement_count is count_cap (read it as
    "at least"). Counting never binds a whole set in one query."""
    db = config.ensure_db(db)
    df = dataset_summary(db, **kwargs)
    sets = pqq.do_query({":find": ["?ms", "?measurement-set-name"],
                         ":where": [["?a", ":assay/measurement-sets", "?ms"],
                                    ["?ms", ":measurement-set/name", "?measurement-set-name"]]},
                        db=db, **kwargs)
    counts: Dict[str, Tuple[int, bool]] = {}
    # one or two round trips per set, and a dataset can have dozens of sets:
    # count them concurrently (the back pressure still caps requests in flight)
    with ThreadPoolExecutor(bp.retry_policy()["max_concurrency"]) as pool:
        found = list(pool.map(lambda e: _count_set_measurements(e, db, count_cap),
                              [int(e) for e in sets["ms"]]))
    for (_, row), (n, exact) in zip(sets.iterrows(), found):
        prev = counts.get(row["measurement_set_name"], (0, True))
        counts[row["measurement_set_name"]] = (prev[0] + n, prev[1] and exact)
    df["measurement_count"] = df["measurement_set_name"].map(lambda s: counts.get(s, (0, True))[0]).astype(int)
    df["measurement_count_exact"] = df["measurement_set_name"].map(lambda s: counts.get(s, (0, True))[1])
    return df


def measurement_types(measurement_set: str, db: Optional[str] = None, limit: int = 20000,
                      **kwargs) -> pd.DataFrame:
    """Count every attribute across a measurement set's measurements: value
    attributes (tpm, percent-of-parent, ...) and target references
    (gene-product, cell-population, ...). A set can mix several kinds, e.g.
    CyTOF populations with percent-of-parent alongside population x marker
    median-channel-value. Columns: attribute, kind ("value"/"target"), count.

    Counts cover the set's first `limit` measurements in index order; the
    result's attrs["exact"] is False when the set is larger than that, and the
    counts are then a sample (kinds laid out after the first `limit` can be
    missed). Raise `limit` for exact counts on a bigger set."""
    db = config.ensure_db(db)
    parts, exact = [], True
    for ms in _measurement_set_eids(measurement_set, db, **kwargs):
        ids, complete = _set_measurement_ids(ms, db, limit)
        exact = exact and complete
        for i in range(0, len(ids), _PAGE):
            r = pqq.do_query({":find": ["?attribute", ["count", "?m"]], ":in": [["?m", "..."]],
                              ":where": [["?m", "?a"], ["?a", ":db/ident", "?attribute"]]},
                             [ids[i:i + _PAGE]], db=db, **kwargs)
            if len(r):
                parts.append(r)
    if not parts:
        out = pd.DataFrame(columns=["attribute", "kind", "count"])
        out.attrs["exact"] = exact
        return out
    r = pd.concat(parts, ignore_index=True).groupby("attribute", as_index=False)["count_m"].sum()
    r = r[~r["attribute"].isin([":measurement/id", ":measurement/uid", ":measurement/sample"])]
    r = r.rename(columns={"count_m": "count"})
    r["kind"] = r["attribute"].map(lambda a: "target" if a in MEASUREMENT_TARGETS else "value")
    r["attribute"] = r["attribute"].str.replace(":measurement/", "", regex=False)
    out = r.sort_values(["kind", "count"], ascending=[True, False])[["attribute", "kind", "count"]]
    out = pqres.keep_provenance(out.reset_index(drop=True), parts[0])
    out.attrs["exact"] = exact
    return out


def measurement_set_attributes(measurement_set: str, db: Optional[str] = None,
                               measurement: Optional[str] = None, n: int = 200) -> List[str]:
    """Attributes of the set's first n measurements in index order (optionally
    only those carrying `measurement`, looked for among the set's first 100000
    measurements, the deepest the server pages by offset). Reads bounded pages
    of the datoms index, so it stays cheap for sets with millions of
    measurements."""
    db = config.ensure_db(db)
    attr = _mattr(measurement) if measurement else None
    found: List[int] = []
    for ms in _measurement_set_eids(measurement_set, db):
        offset = 0
        for _ in range(10):  # the server caps datoms offsets at 100000
            ids, complete = _set_measurement_ids(ms, db, _PAGE if attr else n, offset)
            offset += len(ids)
            if attr and ids:
                r = pqq.do_query({":find": ["?m"], ":in": [["?m", "..."]], ":where": [["?m", attr]]},
                                 [ids], db=db)
                ids = [int(m) for m in r["m"]] if len(r) else []
            found += ids
            if len(found) >= n or complete or not attr:
                break
        if len(found) >= n:
            break
    found = found[:n]
    if not found:
        return []
    attrs = pqq.do_query({":find": ["?attr"], ":in": [["?m", "..."]],
                          ":where": [["?m", "?a"], ["?a", ":db/ident", "?attr"]]},
                         [found], db=db)
    return sorted(set(attrs["attr"]) - {":measurement/uid", ":measurement/id"})


# measurement target references: the entity a measurement is "of", how to
# name it, and the result column (same table as R).
MEASUREMENT_TARGETS: Dict[str, Dict[str, Any]] = {
    ":measurement/gene-product": {"clauses": [["?tgp", ":gene-product/gene", "?tg"],
                                              ["?tg", ":gene/hgnc-symbol", "?hgnc-symbol"]],
                                  "ref": "?tgp", "var": "?hgnc-symbol"},
    ":measurement/variant": {"clauses": [["?tv", ":variant/id", "?variant-id"]], "ref": "?tv", "var": "?variant-id"},
    ":measurement/cnv": {"clauses": [["?tc", ":cnv/id", "?cnv-id"]], "ref": "?tc", "var": "?cnv-id"},
    ":measurement/epitope": {"clauses": [["?te", ":epitope/id", "?epitope-id"]], "ref": "?te", "var": "?epitope-id"},
    ":measurement/cell-population": {"clauses": [["?tcp", ":cell-population/name", "?cell-population"]],
                                     "ref": "?tcp", "var": "?cell-population"},
    ":measurement/tcr": {"clauses": [["?tt", ":tcr/id", "?tcr-id"]], "ref": "?tt", "var": "?tcr-id"},
    ":measurement/otu": {"clauses": [["?to", ":otu/id", "?otu-id"]], "ref": "?to", "var": "?otu-id"},
    ":measurement/sgb": {"clauses": [["?ts", ":sgb/metaphlan-id", "?sgb-id"]], "ref": "?ts", "var": "?sgb-id"},
    ":measurement/pathway": {"clauses": [["?tp", ":pathway/id", "?pathway-id"]], "ref": "?tp", "var": "?pathway-id"},
    ":measurement/metabolite-feature": {"clauses": [["?tmf", ":metabolite-feature/rt-mz-peak", "?metabolite-feature"]],
                                        "ref": "?tmf", "var": "?metabolite-feature"},
    ":measurement/nanostring-signature": {"clauses": [["?tns", ":nanostring-signature/name", "?signature"]],
                                          "ref": "?tns", "var": "?signature"},
    ":measurement/atac-peak": {"clauses": [["?tap", ":atac-peak/name", "?atac-peak"]], "ref": "?tap", "var": "?atac-peak"},
    ":measurement/single-cell": {"clauses": [["?tsc", ":single-cell/id", "?single-cell-id"]],
                                 "ref": "?tsc", "var": "?single-cell-id"},
}

ENUM_MEASUREMENT_ATTRS = {":measurement/cnv-call", ":measurement/msi-status"}


def measurements_query(measurement: str, measurement_set: Optional[str] = None,
                       samples: Optional[Sequence[str]] = None,
                       targets: Sequence[str] = ()) -> QueryAndArgs:
    attr = _mattr(measurement)
    enum = attr in ENUM_MEASUREMENT_ATTRS
    where = [["?m", attr, "?value-ref" if enum else "?value"],
             ["?m", ":measurement/sample", "?s"],
             ["?s", ":sample/id", "?sample-id"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?ms", ":measurement-set/name", "?measurement-set"]]
    if enum:
        where.append(["?value-ref", ":db/ident", "?value"])
    find: List[Any] = ["?sample-id", "?measurement-set"]
    for t in targets:
        spec = MEASUREMENT_TARGETS.get(t)
        if spec is None:
            raise ValueError(f"Unknown measurement target {t}")
        where += [["?m", t, spec["ref"]]] + spec["clauses"]
        find.append(spec["var"])
    find.append("?value")
    ins: List[Any] = []
    args: List[Any] = []
    if measurement_set is not None:
        ins.append("?measurement-set")
        args.append(measurement_set)
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    return {":find": find, ":with": ["?m"], ":in": ins, ":where": where}, args


def measurements(measurement: str, measurement_set: Optional[str] = None, db: Optional[str] = None,
                 samples: Optional[Sequence[str]] = None, targets: Optional[Sequence[str]] = None,
                 wide: bool = False, aggfunc: str = "mean", **kwargs):
    """Values of one measurement attribute (e.g. "tpm", "percent-of-parent",
    "olink-npx", "median-channel-value") with what each measurement is of (its
    target: gene, cell population, epitope, ...). Targets are detected from a
    sample of the set's measurements that carry `measurement`, unless given.

    Long format: sample_id, measurement_set, target column(s), value; or with
    wide=True a samples x targets DataFrame (see context.to_matrix)."""
    db = config.ensure_db(db)
    if targets is None:
        if measurement_set is None:
            raise ValueError("Give measurement_set (or targets) so measurement targets can be detected")
        attrs = measurement_set_attributes(measurement_set, db=db, measurement=measurement)
        targets = [a for a in attrs if a in MEASUREMENT_TARGETS]
    df = _run(measurements_query(measurement, measurement_set, samples, targets), db, **kwargs)
    if len(df) and df["value"].dtype == object:
        df["value"] = df["value"].map(pqres.ident_name)
    if wide:
        from patternq import context as pqc
        cols = [c for c in df.columns if c not in ("sample_id", "measurement_set", "value")] or ["measurement_set"]
        return pqres.keep_provenance(pqc.to_matrix(df, col=cols, aggfunc=aggfunc), df)
    return df


def sample_assays_query() -> QueryAndArgs:
    """Samples x measurement sets from :measurement-set/includes-samples, the
    enrichment that lists each set's samples (schema 0.3.18), so nothing walks
    the measurements."""
    q = {":find": ["?sample-id", "?assay-name", "?assay-technology", "?measurement-set-name"],
         ":where": [["?a", ":assay/name", "?assay-name"],
                    ["?a", ":assay/technology", "?t"],
                    ["?t", ":db/ident", "?assay-technology"],
                    ["?a", ":assay/measurement-sets", "?ms"],
                    ["?ms", ":measurement-set/name", "?measurement-set-name"],
                    ["?ms", ":measurement-set/includes-samples", "?s"],
                    ["?s", ":sample/id", "?sample-id"]]}
    return q, []


def _has_includes_samples(db: str, **kwargs) -> bool:
    r = pqq.do_query({":find": ["?a"], ":in": ["?ident"], ":where": [["?a", ":db/ident", "?ident"]]},
                     [":measurement-set/includes-samples"], db=db, **kwargs)
    return len(r) > 0


def sample_assays(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """Which samples were measured in which measurement sets: subject_id,
    sample_id, timepoint_id, assay_name, assay_technology,
    measurement_set_name. Reads :measurement-set/includes-samples; a database
    from before that enrichment falls back to walking each set's
    measurements, which can time out on large expression sets."""
    db = config.ensure_db(db)
    if _has_includes_samples(db, **kwargs):
        df = _run(sample_assays_query(), db, **kwargs)
        if not len(df):
            return pd.DataFrame(columns=["subject_id", "sample_id", "timepoint_id", "assay_name",
                                         "assay_technology", "measurement_set_name"])
        df["assay_technology"] = df["assay_technology"].map(pqres.ident_name)
        smp = samples(db)
        keep = [c for c in ("sample_id", "subject_id", "timepoint_id") if c in smp.columns]
        out = smp[keep].merge(df, on="sample_id", how="right")
        return pqres.keep_provenance(pqres.order_columns(out, ["subject_id", "sample_id", "timepoint_id"]), df)
    sets = dataset_summary(db)
    parts = []
    for _, row in sets.iterrows():
        r = pqq.do_query({":find": ["?sample-id"], ":in": ["?ms-name"],
                          ":where": [["?ms", ":measurement-set/name", "?ms-name"],
                                     ["?ms", ":measurement-set/measurements", "?m"],
                                     ["?m", ":measurement/sample", "?s"],
                                     ["?s", ":sample/id", "?sample-id"]]},
                         [row["measurement_set_name"]], db=db, **kwargs)
        if len(r):
            r["assay_name"] = row["assay_name"]
            r["assay_technology"] = row["assay_technology"]
            r["measurement_set_name"] = row["measurement_set_name"]
            parts.append(r)
    if not parts:
        return pd.DataFrame(columns=["subject_id", "sample_id", "timepoint_id", "assay_name",
                                     "assay_technology", "measurement_set_name"])
    df = pd.concat(parts, ignore_index=True)
    smp = samples(db)
    keep = [c for c in ("sample_id", "subject_id", "timepoint_id") if c in smp.columns]
    df = smp[keep].merge(df, on="sample_id", how="right")
    return pqres.keep_provenance(pqres.order_columns(df, ["subject_id", "sample_id", "timepoint_id"]), sets)


def measurement_matrices_query() -> QueryAndArgs:
    q = {":find": ["?assay-name", "?measurement-set-name", "?matrix-name", "?measurement-type", "?matrix-key"],
         ":where": [["?a", ":assay/name", "?assay-name"],
                    ["?a", ":assay/measurement-sets", "?ms"],
                    ["?ms", ":measurement-set/name", "?measurement-set-name"],
                    ["?ms", ":measurement-set/measurement-matrices", "?mm"],
                    ["?mm", ":measurement-matrix/name", "?matrix-name"],
                    ["?mm", ":measurement-matrix/measurement-type", "?mt"],
                    ["?mt", ":db/ident", "?measurement-type"],
                    ["?mm", ":measurement-matrix/backing-file", "?matrix-key"]]}
    return q, []


def measurement_matrices(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """Measurement matrices (file-backed measurements): assay_name,
    measurement_set_name, matrix_name, measurement_type, matrix_key. Pass
    matrix_key to query.measurement_matrix()."""
    df = _run(measurement_matrices_query(), db, **kwargs)
    df["measurement_type"] = df["measurement_type"].map(pqres.ident_name)
    return df


def measurement_matrix_by_name(matrix_name: str, db: Optional[str] = None) -> pd.DataFrame:
    """Download a measurement matrix by its name."""
    mm = measurement_matrices(db)
    keys = mm.loc[mm["matrix_name"] == matrix_name, "matrix_key"]
    if not len(keys):
        raise KeyError(f"No measurement matrix named '{matrix_name}'")
    return pqq.measurement_matrix(keys.iloc[0], db=db)


def dataset_info(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """The dataset entity stored in the database: name, description, doi, url."""
    q = {":find": [["pull", "?d", [":dataset/name", ":dataset/description", ":dataset/doi", ":dataset/url"]]],
         ":where": [["?d", ":dataset/name"]]}
    return pqq.do_query(q, db=db, **kwargs)


# -- variants ------------------------------------------------------------------

def variants_query(samples: Optional[Sequence[str]] = None,
                   genes: Optional[Sequence[str]] = None,
                   measurement_set: Optional[str] = None) -> QueryAndArgs:
    where = [["?m", ":measurement/vaf", "?vaf"],
             ["?m", ":measurement/variant", "?v"],
             ["?m", ":measurement/sample", "?s"],
             ["?s", ":sample/id", "?sample-id"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?ms", ":measurement-set/name", "?measurement-set"]]
    ins: List[Any] = []
    args: List[Any] = []
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    if genes is not None:
        where += [["?v", ":variant/gene", "?g"], ["?g", ":gene/hgnc-symbol", "?gene"]]
        ins.append(["?gene", "..."])
        args.append(list(genes))
    if measurement_set is not None:
        ins.append("?measurement-set")
        args.append(measurement_set)
    if genes is not None:
        where = _lead_with(where, [["?g", ":gene/hgnc-symbol", "?gene"], ["?v", ":variant/gene", "?g"],
                                   ["?m", ":measurement/variant", "?v"]])
    elif samples is not None:
        where = _lead_with(where, _SAMPLE_LEAD)
    q = {":find": ["?sample-id", "?measurement-set", "?vaf",
                   ["pull", "?v", [":variant/id", ":variant/HGVSp", ":variant/HGVSc",
                                   {":variant/gene": [":gene/hgnc-symbol"]},
                                   {":variant/impact": [":db/ident"]}]],
                   ["pull", "?m", [":measurement/t-depth"]]],
         ":in": ins,
         ":where": where}
    return q, args


def variants(db: Optional[str] = None, samples: Optional[Sequence[str]] = None,
             genes: Optional[Sequence[str]] = None, measurement_set: Optional[str] = None,
             **kwargs) -> pd.DataFrame:
    """Somatic variant measurements: one row per measurement with sample_id,
    measurement_set, variant_id, hgnc_symbol, HGVSp, HGVSc, impact, vaf, t_depth
    (where present). HGVSp/HGVSc (cardinality many) are joined with "; "."""
    q, args = variants_query(samples=samples, genes=genes, measurement_set=measurement_set)
    df = pqq.do_query(q, args, db=db, **kwargs)
    df = pqres.keep_provenance(
        df.rename(columns={"variant_HGVSp": "HGVSp", "variant_HGVSc": "HGVSc",
                           "variant_impact": "impact", "gene_hgnc_symbol": "hgnc_symbol",
                           "measurement_t_depth": "t_depth"}), df)
    for col in ("HGVSp", "HGVSc"):
        if col in df.columns:
            df[col] = df[col].map(pqres.join_many)
    return pqres.order_columns(df, ["sample_id", "measurement_set", "variant_id", "hgnc_symbol",
                                    "HGVSp", "HGVSc", "impact", "vaf", "t_depth"])


def variants_by_impact(sample_id: Optional[str] = None, measurement_set: Optional[str] = None,
                       impact: Optional[VariantImpact] = None, db: Optional[str] = None,
                       **kwargs) -> pd.DataFrame:
    """Variants filtered by impact (patternq 0.2 compatible wrapper over variants())."""
    df = variants(db, samples=[sample_id] if sample_id else None, measurement_set=measurement_set, **kwargs)
    if impact is not None and "impact" in df.columns:
        df = pqres.keep_provenance(df[df["impact"] == impact].reset_index(drop=True), df)
    return df


def variant_measurements(measurement_set: str, db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: variants() of one measurement set."""
    return variants(db, measurement_set=measurement_set, **kwargs)


def measurements_of_variants(variant_ids: Sequence[str], db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: variant measurements for the given variant ids."""
    df = variants(db, **kwargs)
    return pqres.keep_provenance(df[df["variant_id"].isin(list(variant_ids))].reset_index(drop=True), df)


# -- gene expression -----------------------------------------------------------

def gene_expression_query(genes: Optional[Sequence[str]] = None,
                          samples: Optional[Sequence[str]] = None,
                          measurement: str = "tpm",
                          measurement_set: Optional[str] = None) -> QueryAndArgs:
    where = [["?g", ":gene/hgnc-symbol", "?hgnc-symbol"],
             ["?gp", ":gene-product/gene", "?g"],
             ["?m", ":measurement/gene-product", "?gp"],
             ["?m", _mattr(measurement), "?value"],
             ["?m", ":measurement/sample", "?s"],
             ["?s", ":sample/id", "?sample-id"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?ms", ":measurement-set/name", "?measurement-set"]]
    ins: List[Any] = []
    args: List[Any] = []
    if genes is not None:
        ins.append(["?hgnc-symbol", "..."])
        args.append(list(genes))
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    if measurement_set is not None:
        ins.append("?measurement-set")
        args.append(measurement_set)
    if genes is None and samples is not None:
        where = _lead_with(where, _SAMPLE_LEAD)
    q = {":find": ["?sample-id", "?hgnc-symbol", "?measurement-set", "?value"],
         ":with": ["?m"],
         ":in": ins,
         ":where": where}
    return q, args


def gene_expression(db: Optional[str] = None, genes: Optional[Sequence[str]] = None,
                    samples: Optional[Sequence[str]] = None,
                    measurement: RNASeqMeasurementAttribute = "tpm",
                    measurement_set: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """Gene expression, long format: sample_id, hgnc_symbol, measurement_set, value.

    measurement: attribute without namespace ("tpm", "rsem-normalized-count", ...).
    measurement_set: datasets may have several sets carrying the attribute (bulk,
    pseudobulk, ...); without it all are returned, told apart by measurement_set."""
    q, args = gene_expression_query(genes=genes, samples=samples, measurement=measurement,
                                    measurement_set=measurement_set)
    return pqq.do_query(q, args, db=db, **kwargs)


def gene_expression_for_genes(sample_id: str, measurement_set: Optional[str] = None,
                              measurement_attr: RNASeqMeasurementAttribute = "tpm",
                              genes: Optional[Sequence[str]] = None, db: Optional[str] = None,
                              **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: gene_expression() for one sample."""
    return gene_expression(db, genes=genes, samples=[sample_id], measurement=measurement_attr,
                           measurement_set=measurement_set, **kwargs)


def gene_expression_measurements(measurement_set: str, measurement_attr: RNASeqMeasurementAttribute = "tpm",
                                 db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: gene_expression() of one measurement set (all genes)."""
    return gene_expression(db, measurement=measurement_attr, measurement_set=measurement_set, **kwargs)


def cohort_gene_expression(measurement_attr: RNASeqMeasurementAttribute, gene: str,
                           db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: gene_expression() of one gene across the cohort."""
    return gene_expression(db, genes=[gene], measurement=measurement_attr, **kwargs)


def isoforms(gene: str, db: Optional[str] = None, samples: Optional[Sequence[str]] = None,
             **kwargs) -> pd.DataFrame:
    """Isoform-level expression for a gene: sample_id, transcript_id,
    transcript_length, isoform_percent, effective_length."""
    ins: List[Any] = ["?hgnc"]
    args: List[Any] = [gene]
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    q = {":find": ["?sample-id", "?transcript-id", "?transcript-length", "?isoform-percent", "?effective-length"],
         ":with": ["?m"], ":in": ins,
         ":where": [["?g", ":gene/hgnc-symbol", "?hgnc"],
                    ["?gp", ":gene-product/gene", "?g"],
                    ["?gp", ":gene-product/id", "?transcript-id"],
                    ["?gp", ":gene-product/transcript-length", "?transcript-length"],
                    ["?m", ":measurement/gene-product", "?gp"],
                    ["?m", ":measurement/isoform-percent", "?isoform-percent"],
                    ["?m", ":measurement/effective-transcript-length", "?effective-length"],
                    ["?m", ":measurement/sample", "?s"],
                    ["?s", ":sample/id", "?sample-id"]]}
    return pqq.do_query(q, args, db=db, **kwargs)


# -- copy number (subset required) ---------------------------------------------

def _require_cnv_subset(genes, samples, subjects):
    # CNV data is large (segments x samples, or genes x samples); the CNV queries
    # insist on a subset. Same rule in the R and Clojure libraries.
    if genes is None and samples is None and subjects is None:
        raise ValueError("CNV queries need a subset: give genes, samples or subjects")


def _subject_filter(subjects, where, ins, args):
    if subjects is not None:
        where += [["?s", ":sample/subject", "?p"], ["?p", ":subject/id", "?subject-id"]]
        ins.append(["?subject-id", "..."])
        args.append(list(subjects))


def _strip_cnv_prefix(c: str) -> str:
    if c == "measurement_set":
        return c
    for p in ("genomic_coordinate_", "measurement_"):
        if c.startswith(p):
            return c[len(p):]
    return c


def cnv_segments(db: Optional[str] = None, genes: Optional[Sequence[str]] = None,
                 samples: Optional[Sequence[str]] = None, subjects: Optional[Sequence[str]] = None,
                 **kwargs) -> pd.DataFrame:
    """Segment-level CNV measurements with coordinates: sample_id,
    measurement_set, cnv_id, hgnc_symbol (with genes), contig, start, end,
    segment_mean_lrr, absolute_cn, ... At least one of genes, samples, subjects
    is required."""
    _require_cnv_subset(genes, samples, subjects)
    where = [["?m", ":measurement/cnv", "?c"],
             ["?c", ":cnv/id", "?cnv-id"],
             ["?m", ":measurement/sample", "?s"],
             ["?s", ":sample/id", "?sample-id"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?ms", ":measurement-set/name", "?measurement-set"]]
    find: List[Any] = ["?sample-id", "?measurement-set", "?cnv-id",
                       ["pull", "?c", [{":cnv/genomic-coordinates": [":genomic-coordinate/contig",
                                                                     ":genomic-coordinate/start",
                                                                     ":genomic-coordinate/end"]}]],
                       ["pull", "?m", [":measurement/segment-mean-lrr", ":measurement/absolute-cn",
                                       ":measurement/a-allele-cn", ":measurement/b-allele-cn",
                                       ":measurement/loh"]]]
    ins: List[Any] = []
    args: List[Any] = []
    if genes is not None:
        # Datomic joins in clause order: start from the genes and the segments
        # covering them, not from every CNV measurement in the database
        where = [["?g", ":gene/hgnc-symbol", "?hgnc-symbol"], ["?c", ":cnv/genes", "?g"]] + where
        find.append("?hgnc-symbol")
        ins.append(["?hgnc-symbol", "..."])
        args.append(list(genes))
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    _subject_filter(subjects, where, ins, args)
    if genes is None and samples is not None:
        where = _lead_with(where, _SAMPLE_LEAD)
    df = pqq.do_query({":find": find, ":in": ins, ":where": where}, args, db=db, **kwargs)
    df = pqres.keep_provenance(df.rename(columns=_strip_cnv_prefix), df)
    return pqres.order_columns(df, ["sample_id", "measurement_set", "cnv_id", "hgnc_symbol",
                                    "contig", "start", "end"])


def cnv_gene_calls(db: Optional[str] = None, genes: Optional[Sequence[str]] = None,
                   samples: Optional[Sequence[str]] = None, subjects: Optional[Sequence[str]] = None,
                   measurement: str = "cnv-call-score", **kwargs) -> pd.DataFrame:
    """Gene-level CNV calls (e.g. GISTIC2 discrete calls, cnv-call-score, or the
    cnv-call enum): sample_id, measurement_set, hgnc_symbol, value. At least one
    of genes, samples, subjects is required."""
    _require_cnv_subset(genes, samples, subjects)
    attr = _mattr(measurement)
    enum = attr in ENUM_MEASUREMENT_ATTRS
    # gene-first clause order: gene-level CNV sets are large (genes x samples)
    where = [["?g", ":gene/hgnc-symbol", "?hgnc-symbol"],
             ["?gp", ":gene-product/gene", "?g"],
             ["?m", ":measurement/gene-product", "?gp"],
             ["?m", attr, "?value-ref" if enum else "?value"],
             ["?m", ":measurement/sample", "?s"],
             ["?s", ":sample/id", "?sample-id"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?ms", ":measurement-set/name", "?measurement-set"]]
    if enum:
        where.append(["?value-ref", ":db/ident", "?value"])
    ins: List[Any] = []
    args: List[Any] = []
    if genes is not None:
        ins.append(["?hgnc-symbol", "..."])
        args.append(list(genes))
    if samples is not None:
        ins.append(["?sample-id", "..."])
        args.append(list(samples))
    _subject_filter(subjects, where, ins, args)
    if genes is None:
        where = _lead_with(where, _SAMPLE_LEAD if samples is not None
                           else [["?m", attr, "?value-ref" if enum else "?value"]])
    df = pqq.do_query({":find": ["?sample-id", "?measurement-set", "?hgnc-symbol", "?value"],
                       ":with": ["?m"], ":in": ins, ":where": where}, args, db=db, **kwargs)
    if len(df) and df["value"].dtype == object:
        df["value"] = df["value"].map(pqres.ident_name)
    return df


def cnv_by_gene_measurements(sample_id: str, measurement_set: Optional[str] = None,
                             db: Optional[str] = None, genes: Optional[Sequence[str]] = None,
                             **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: CNV segments of one sample (optionally for genes)."""
    df = cnv_segments(db, genes=genes, samples=[sample_id], **kwargs)
    if measurement_set is not None:
        df = pqres.keep_provenance(df[df["measurement_set"] == measurement_set].reset_index(drop=True), df)
    return df


# -- timepoints & clinical -----------------------------------------------------

def timepoints(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """Timepoints ordered by relative order: timepoint_id,
    timepoint_relative_order, timepoint_type, offset, cycle/day where present."""
    df = pqq.do_query({":find": [["pull", "?t", ["*", {":timepoint/type": [":db/ident"]}]]],
                       ":where": [["?t", ":timepoint/id"]]}, db=db, **kwargs)
    if "timepoint_relative_order" in df.columns:
        df = pqres.keep_provenance(df.sort_values("timepoint_relative_order").reset_index(drop=True), df)
    return pqres.order_columns(df, ["timepoint_id", "timepoint_relative_order", "timepoint_type"])


def clinical_observation_sets(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """clinical_observation_set_name, clinical_observation_set_description."""
    return pqq.do_query({":find": [["pull", "?c", [":clinical-observation-set/name",
                                                   ":clinical-observation-set/description"]]],
                         ":where": [["?c", ":clinical-observation-set/name"]]}, db=db, **kwargs)


def clinical_observations(db: Optional[str] = None, obs_type: Optional[str] = None,
                          set_name: Optional[str] = None, subjects: Optional[Sequence[str]] = None,
                          **kwargs) -> pd.DataFrame:
    """Clinical observations, either one type across the dataset (obs_type, e.g.
    "os", "pfs", "bor", "recist", "ldh": subject_id, timepoint_id, <type>) or all
    attributes of the observations in a set (set_name: one row per observation)."""
    if obs_type is None and set_name is None:
        raise ValueError("Give obs_type or set_name")
    if obs_type is not None:
        t = obs_type.lstrip(":").replace("clinical-observation/", "")
        attr = ":clinical-observation/" + t
        ins: List[Any] = []
        args: List[Any] = []
        if subjects is not None:
            ins.append(["?subject-id", "..."])
            args.append(list(subjects))
        q = {":find": ["?subject-id", ["pull", "?o", [{":clinical-observation/timepoint": [":timepoint/id"]},
                                                      {":clinical-observation/study-day": [":study-day/id"]},
                                                      attr]]],
             ":in": ins,
             ":where": [["?o", attr], ["?o", ":clinical-observation/subject", "?p"],
                        ["?p", ":subject/id", "?subject-id"]]}
        df = pqq.do_query(q, args, db=db, **kwargs)
        return pqres.keep_provenance(df.rename(columns={pqres.clean_name(attr): pqres.clean_name(t)}), df)
    q = {":find": [["pull", "?o", ["*",
                                   {":clinical-observation/subject": [":subject/id"]},
                                   {":clinical-observation/timepoint": [":timepoint/id"]},
                                   {":clinical-observation/study-day": [":study-day/id"]},
                                   {":clinical-observation/recist": [":db/ident"]},
                                   {":clinical-observation/bor": [":db/ident"]},
                                   {":clinical-observation/pfs-reason": [":db/ident"]},
                                   {":clinical-observation/os-reason": [":db/ident"]},
                                   {":clinical-observation/disease-stage": [":db/ident"]},
                                   {":clinical-observation/metastasis-gdc-anatomic-sites": [":gdc-anatomic-site/name"]}]]],
         ":in": ["?set-name"],
         ":where": [["?cos", ":clinical-observation-set/name", "?set-name"],
                    ["?cos", ":clinical-observation-set/clinical-observations", "?o"]]}
    df = pqq.do_query(q, [set_name], db=db, **kwargs)
    return pqres.keep_provenance(
        df.rename(columns=lambda c: c[len("clinical_observation_"):] if c.startswith("clinical_observation_") else c), df)


def subject_outcomes(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per subject with bor (from :clinical-observation/bor, or derived
    from RECIST observations when absent: CR > PR > SD > PD), pfs, pfs_event,
    os, os_event where present. Raises if a subject has more than one value of a
    single-valued outcome."""
    db = config.ensure_db(db)

    def get1(t):
        try:
            r = clinical_observations(db, obs_type=t, **kwargs)
        except RuntimeError:
            return None
        col = pqres.clean_name(t)
        if not len(r) or col not in r.columns:
            return None
        r = r.loc[r[col].notna(), ["subject_id", col]]
        if r["subject_id"].duplicated().any():
            raise ValueError(f"More than one {t} value for some subjects")
        return r

    bor = get1("bor")
    if bor is None:
        try:
            recist = clinical_observations(db, obs_type="recist", **kwargs)
        except RuntimeError:
            recist = None
        if recist is not None and len(recist) and "recist" in recist.columns:
            rank = {"CR": 1, "PR": 2, "SD": 3, "PD": 4}

            def best(x):
                x = [v for v in x if v in rank]
                return min(x, key=rank.get) if x else "Unknown"

            bor = recist.groupby("subject_id")["recist"].agg(best).reset_index().rename(columns={"recist": "bor"})
    ids = subjects(db)[["subject_id"]]
    out = ids
    for part in (bor, get1("pfs"), get1("pfs-event"), get1("os"), get1("os-event")):
        if part is not None:
            out = out.merge(part, on="subject_id", how="left")
    return pqres.keep_provenance(out, ids)


def adverse_events(db: Optional[str] = None, set_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per adverse event, optionally from one clinical observation set."""
    where: List[Any] = [["?o", ":adverse-event/subject"]]
    ins: List[Any] = []
    args: List[Any] = []
    if set_name is not None:
        where = [["?cos", ":clinical-observation-set/name", "?set-name"],
                 ["?cos", ":clinical-observation-set/adverse-events", "?o"]]
        ins, args = ["?set-name"], [set_name]
    q = {":find": [["pull", "?o", ["*",
                                   {":adverse-event/subject": [":subject/id"]},
                                   {":adverse-event/timepoint": [":timepoint/id"]},
                                   {":adverse-event/meddra-adverse-event": [":meddra-disease/preferred-name"]},
                                   {":adverse-event/ctcae-grade": [":db/ident"]},
                                   {":adverse-event/ae-causality": [":db/ident"]},
                                   {":adverse-event/study-day": [":study-day/id"]}]]],
         ":in": ins, ":where": where}
    return pqq.do_query(q, args, db=db, **kwargs)


def clinical_interventions(db: Optional[str] = None, subjects: Optional[Sequence[str]] = None,
                           **kwargs) -> pd.DataFrame:
    """Clinical interventions (treatments, surgeries, biopsies, ...), one row per
    intervention with treatment regimen and drug names where present."""
    ins: List[Any] = []
    args: List[Any] = []
    if subjects is not None:
        ins.append(["?subject-id", "..."])
        args.append(list(subjects))
    q = {":find": [["pull", "?ci", ["*",
                                    {":clinical-intervention/subject": [":subject/id"]},
                                    {":clinical-intervention/timepoint": [":timepoint/id", ":timepoint/relative-order"]},
                                    {":clinical-intervention/treatment-regimen": [
                                        ":treatment-regimen/name",
                                        {":treatment-regimen/drug-regimens": [
                                            {":drug-regimen/drug": [":drug/preferred-name"]},
                                            ":drug-regimen/freetext-drug"]}]},
                                    {":clinical-intervention/surgery-type": [":db/ident"]},
                                    {":clinical-intervention/cancer-medication-category": [":db/ident"]},
                                    {":clinical-intervention/radiation-therapy-category": [":db/ident"]},
                                    {":clinical-intervention/biospecimen-type": [":db/ident"]},
                                    {":clinical-intervention/biospecimen-collection": [":db/ident"]},
                                    {":clinical-intervention/biospecimen-derived-samples": [":sample/id"]}]]],
         ":in": ins,
         ":where": [["?ci", ":clinical-intervention/subject", "?p"], ["?p", ":subject/id", "?subject-id"]]}
    df = pqq.do_query(q, args, db=db, **kwargs)
    return pqres.keep_provenance(
        df.rename(columns=lambda c: c[len("clinical_intervention_"):] if c.startswith("clinical_intervention_") else c), df)


def clinical_events_for_patients(subject_ids: Sequence[str], db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2 name: clinical interventions for the given subjects."""
    return clinical_interventions(db, subjects=subject_ids, **kwargs)


# -- measurement set entities --------------------------------------------------

def _ms_entities(ref_attr: str, pattern: List[Any], measurement_set: str, db, **kwargs) -> pd.DataFrame:
    q = {":find": [["pull", "?e", pattern]], ":in": ["?ms-name"],
         ":where": [["?ms", ":measurement-set/name", "?ms-name"], ["?ms", ref_attr, "?e"]]}
    return pqq.do_query(q, [measurement_set], db=db, **kwargs)


def cell_populations(measurement_set: str, db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """Cell populations of a measurement set, with cell type and markers."""
    return _ms_entities(":measurement-set/cell-populations",
                        ["*", {":cell-population/cell-type": [":cell-type/co-name"]},
                         {":cell-population/positive-markers": [":epitope/id"]},
                         {":cell-population/negative-markers": [":epitope/id"]},
                         {":cell-population/parent": [":cell-population/name"]}],
                        measurement_set, db, **kwargs)


def tcrs(measurement_set: str, db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """TCRs of a measurement set."""
    return _ms_entities(":measurement-set/tcrs", ["*"], measurement_set, db, **kwargs)


def otus(measurement_set: str, db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """OTUs of a measurement set."""
    return _ms_entities(":measurement-set/otus", ["*"], measurement_set, db, **kwargs)


def sgbs(measurement_set: str, db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """SGBs (metagenomic species) of a measurement set."""
    return _ms_entities(":measurement-set/sgbs", ["*"], measurement_set, db, **kwargs)


def single_cell_populations(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """patternq 0.2: single cell id -> cell population name."""
    return pqq.do_query({":find": ["?single-cell-id", "?cell-population"],
                         ":where": [["?sc", ":single-cell/id", "?single-cell-id"],
                                    ["?sc", ":single-cell/cell-populations", "?cp"],
                                    ["?cp", ":cell-population/name", "?cell-population"]]}, db=db, **kwargs)


# patternq 0.2 name
patient_assays = sample_assays
