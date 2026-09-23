# patternq (Python)

Query and analysis tools for the Pattern Data Commons (Unify / Datomic).
The patternq family: [patternq](https://github.com/RCRF/patternq) (Python), [patternq-r](https://github.com/RCRF/patternq-r) (R), [patternq-clj](https://github.com/RCRF/patternq-clj) (Clojure) and [PatternQ.jl](https://github.com/RCRF/PatternQ.jl) (Julia) share one function catalog, the same result columns and the same plots.

## Install

```
pip install git+ssh://git@github.com/RCRF/patternq.git
```

Dependencies: `requests`, `pandas`, `plotly`. For PNG export of figures, also
install `kaleido`.

## Configure access

```
export PATTERNQ_ENDPOINT=https://data-commons.rcrf-dev.org   # default
export PATTERNQ_API_KEY=...                                   # from the dashboard's user settings
```

or in a session: `pq.set_query_server(url)`, `pq.set_token(token)`.

## Import aliases

```python
import patternq as pq               # config, transport, provenance
import patternq.query as pqq        # raw queries, datoms, matrices
import patternq.dataset as pqd      # samples, subjects, measurements, variants, CNV, clinical
import patternq.reference as pqr    # genes, variant annotations, vocabularies, map_gene_symbols
import patternq.context as pqc      # reshaping and context joins
import patternq.plots as pqp        # plotly figures
import patternq.analysis as pqa     # sample vs cohort, two-sample change, ssGSEA, gene sets
import patternq.survival as pqs     # Kaplan-Meier, log-rank, median splits, change from baseline
```

## Quick start

Every dataset is its own database. Resolve a dataset name to its current
database and set it as the session default (or pass `db=` to any function):

```python
pq.list_datasets()                          # datasets your key can access
pq.set_db(pq.resolve_db("prince-2022"))

pqd.dataset_summary()                       # assays and measurement sets
pqd.measurement_types("PICI CyTOF Immune Profiling")
cytof = pqd.measurements("percent-of-parent", "PICI CyTOF Immune Profiling")
cytof = pqc.add_sample_context(cytof, include_outcomes=True)
pqp.plot_by_timepoint(cytof[cytof.cell_population == "HLA-DR+ Non-Naive CD8 T Cells"], group="bor")

outcomes = pqd.subject_outcomes()           # BOR / PFS / OS per subject
pqp.plot_survival(outcomes, "os", "os_event", group="bor")

pqd.variants(genes=["KRAS", "TP53"])
pqd.gene_expression(genes=["CXCL9", "CXCL10"], measurement="tpm")
pqd.cnv_gene_calls(genes=["MYC"])           # CNV queries require genes, samples or subjects
```

### Survival and longitudinal change

```python
outcomes = pqd.subject_outcomes()
outcomes["status_1y"] = pqs.survival_status(outcomes, "os", "os_event", at=12)

# OS split at the median of a baseline biomarker; log-rank in km.attrs["logrank"]
base = cytof[(cytof.cell_population == "HLA-DR+ Non-Naive CD8 T Cells") & (cytof.timepoint_id == "C1D1")]
km = pqs.survival_by_median(base.groupby("subject_id")["value"].mean(), outcomes)
pqp.plot_survival(km, group="group", levels=["low", "high"])   # annotated with the log-rank p

# change relative to a baseline timepoint (log2 ratio, difference, or ratio)
ch = pqs.change_from_baseline(cytof, baseline="C1D1", method="log2_ratio", pseudocount=0.001)
pqp.plot_by_timepoint(ch, value="change", group="status_1y", lines=True)
```

`logrank_test` is the Mantel-Haenszel test for k groups and matches R's
`survival::survdiff`. Examples as marimo notebooks: `examples/py/`.

### Expression analysis

Broad descriptive statistics backported from the Clojure variant-forensics
reports (`patternq.analysis`, plots in `patternq.plots`):

```python
db, uvm = pq.resolve_db("H37001"), pq.resolve_db("tcga-uvm")

# one sample against a reference cohort: z-scores and percentiles on log2(1 + TPM)
cmp = pqa.compare_to_cohort("H37001-003", db=db, cohort_db=uvm, measurement="tpm")
pqa.top_by_zscore(cmp, 25, direction="up")
pqp.plot_zscores(cmp, n=30)

# samples against cohort distributions for a gene set (list or pqa.genesets() name)
pqa.examine_geneset(["BAP1", "PRAME", "PMEL"], ["H37001-003", "H37001-001"], db=db,
                    cohort_dbs={"TCGA-UVM": uvm})

# two-sample change: log2 fold change, MA plot, fold-change bars
ch = pqa.compare_samples("H37001-003", "H37001-001", db=db)
pqp.plot_ma(ch, highlight=["MALAT1"])
pqp.plot_fold_change(ch)

# cohort-level: ssGSEA (Barbie weighting), most variable genes, nearest samples
m = pqc.to_matrix(pqd.gene_expression(uvm, genes=pqa.geneset("hallmark_hypoxia")), col="hgnc_symbol", aggfunc="sum")
pqa.ssgsea(m, {"hypoxia": pqa.geneset("hallmark_hypoxia")})
pqa.top_varying_genes(m, 50)
pqa.nearest_samples(m.iloc[0], m.iloc[1:])
```

Caveats: measurements must be comparable between the sample's and the cohort's
database (TPM is the usual common ground). `compare_to_cohort` counts genes with
no stored value in a cohort sample as 0 (cohort samples = those with a value for
`anchor_gene`, GAPDH by default); `cohort_observed` reports how many cohort
samples actually had a value, and `top_by_zscore(min_observed=0.5)` drops genes
stored for fewer than half the cohort. Nearest-neighbour distances on raw
expression are sensitive to batch and pipeline effects.

Raw queries use the JSON form of Datomic Datalog that the query service
parses (`"?x"` variables, `":ns/name"` attributes):

```python
pq.do_query({":find": ["?id", "?sex"],
             ":where": [["?s", ":subject/id", "?id"],
                        ["?s", ":subject/sex", "?x"],
                        ["?x", ":db/ident", "?sex"]]})
```

Results are DataFrames with snake_case columns named after the query variables
or attributes; enum values come back as names (`"female"`, `"high"`). Each
result carries provenance (`pq.provenance(df)`: database, basis t, time).

Results are cached by the service (S3) by default; `cache=False` returns
results inline without the cache and `refresh_cache=True` recomputes.

## Tests

```
uv venv .venv && uv pip install -p .venv -e '.[test]'
.venv/bin/pytest tests            # live tests run when PATTERNQ_API_KEY is set
```

Live tests use the H37001, H37004, tcga-uvm, prince-2022 and painter-2025
datasets. All access is read-only.

## Examples

`examples/` has a tutorial and a PRINCE trial demo as marimo notebooks in
Quarto format: `marimo edit examples/tutorial.qmd` (set up with
`pip install -r examples/requirements.txt`).

## Contributing

patternq is one library in four languages: [patternq](https://github.com/RCRF/patternq)
(Python), [patternq-r](https://github.com/RCRF/patternq-r) (R),
[patternq-clj](https://github.com/RCRF/patternq-clj) (Clojure) and
[PatternQ.jl](https://github.com/RCRF/PatternQ.jl) (Julia). All four are generated
from a common source and published here, so this repository does not accept pull
requests.

Please report bugs and feature requests as
[issues](https://github.com/RCRF/patternq/issues). Code is welcome in an issue: a
minimal example (the call or query, the dataset, what you expected and what you
got), or a proposed change as a snippet, is the most useful way to suggest one.

## License

Apache 2.0, Rare Cancer Research Foundation.
