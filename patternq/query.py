"""HTTP transport to the Pattern Data Commons query service.

Queries are dicts in the JSON form parsed by the service's datalog-json-parser:
variables are "?x" strings, attributes/keywords ":ns/name" strings, clauses
lists, pull pattern maps dicts:

    {":find": ["?id"],
     ":where": [["?s", ":subject/id", "?id"]]}

The implicit database "$" is prepended to ":in" if absent.

Endpoints (bearer API token):
  POST /query/<db>   Accept text/plain -> presigned URL of the gzipped, S3-cached
                     result; Accept application/json -> inline JSON, cache skipped
  POST /datoms/<db>  -> {"datoms_chunk": [...], "basis_t": ...}
  POST /matrix/<db>/<key> -> presigned URL of a gzipped TSV matrix
  GET  /api-v1/list/datasets
"""
import copy
import gzip
import io
import json
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

from patternq import config
from patternq import results as pqres

Query = Dict[str, Any]


def _headers(accept: str = "text/plain") -> Dict[str, str]:
    return {"Authorization": f"Bearer {config.api_token()}",
            "Accept": accept,
            "User-Agent": "patternq-python"}


def _raise_for(resp: requests.Response, what: str):
    if resp.status_code == 200:
        return
    msg = resp.text
    try:
        parsed = resp.json()
        if isinstance(parsed, dict) and parsed.get("error"):
            msg = parsed["error"]
    except ValueError:
        pass
    if resp.status_code == 401:
        msg = "not authorized; check PATTERNQ_API_KEY / set_token(). " + msg
    if resp.status_code == 403:
        msg = "forbidden; your API key may not have access to this dataset. " + msg
    raise RuntimeError(f"{what} failed (HTTP {resp.status_code}): {msg}")


def _fetch_presigned(url: str, session=None) -> bytes:
    http = session or requests
    resp = http.get(url.strip(), timeout=300)
    _raise_for(resp, "Result download")
    content = resp.content
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return content


def _normalize(q: Query) -> Query:
    q = copy.deepcopy(q)
    q = {(k if k.startswith(":") else ":" + k): v for k, v in q.items()}
    ins = q.get(":in") or []
    if not ins or ins[0] != "$":
        ins = ["$"] + list(ins)
    q[":in"] = ins
    return q


def query_body(q: Query, args: Optional[List[Any]] = None, timeout: int = 30,
               refresh_cache: bool = False) -> Dict[str, Any]:
    body = {"query": _normalize(q), "timeout": int(timeout * 1000)}
    if args:
        body["args"] = list(args)
    if refresh_cache:
        body["refresh-cache"] = True
    return body


def query(q: Query, args: Optional[List[Any]] = None, db: Optional[str] = None,
          timeout: int = 30, cache: bool = True, refresh_cache: bool = False,
          session: Optional[requests.Session] = None, db_name: Optional[str] = None,
          print_json: bool = False) -> Dict[str, Any]:
    """Run a query and return the parsed response: a dict with "query_result",
    "basis_t" and "db_name". Most users want do_query(), which returns a DataFrame.

    cache: use the service's S3 result cache (default). The service returns a
      presigned URL to the gzipped cached result, computing and caching it on a
      miss. With cache=False the result comes back inline and the cache is skipped.
    refresh_cache: recompute and overwrite the cached result.
    session: a requests.Session, to reuse connections across many queries.
    db_name: deprecated alias for db.
    """
    db = config.ensure_db(db or db_name)
    http = session or requests
    body = query_body(q, args=args, timeout=timeout, refresh_cache=refresh_cache)
    if print_json:
        print(json.dumps(body))
    accept = "text/plain" if cache else "application/json"
    resp = http.post(f"{config.query_server()}/query/{db}",
                     data=json.dumps(body),
                     headers={**_headers(accept), "Content-Type": "application/json"},
                     timeout=timeout + 30)
    _raise_for(resp, "Query")
    payload = resp.text.strip()
    if payload.startswith("{"):
        res = json.loads(payload)
    else:
        res = json.loads(_fetch_presigned(payload, session))
    if res.get("error"):
        raise RuntimeError(f"Query error: {res['error']}")
    res["db_name"] = db
    return res


def do_query(q: Query, args: Optional[List[Any]] = None, db: Optional[str] = None,
             exclude_ids: bool = True, **kwargs) -> pd.DataFrame:
    """Run a query and return a DataFrame.

    Column names come from the :find variables ("?sample-id" -> "sample_id").
    Pull expressions are flattened to one column per attribute (see
    results.flatten_pull). The result carries provenance (db, basis_t,
    timestamp), see results.provenance()."""
    res = query(q, args=args, db=db, **kwargs)
    find = _normalize(q)[":find"]
    rows = res["query_result"]
    if any(pqres._is_pull(e) for e in find):
        rows = _resolve_enum_refs(rows, res["db_name"], session=kwargs.get("session"))
    df = pqres.result_to_df(rows, find, exclude_ids=exclude_ids)
    return pqres.add_provenance(df, res)


def _is_eid_map(x) -> bool:
    return isinstance(x, dict) and list(x.keys()) == [":db/id"]


def _resolve_enum_refs(rows, db: str, session=None):
    """Pulled refs without a nested pattern come back as {":db/id": n}. Resolve
    those that are enums (have a :db/ident) to {":db/ident": ...} with one extra
    query, so enum values read as names whichever pull pattern was used."""
    eids = set()

    def collect(x):
        if _is_eid_map(x):
            eids.add(x[":db/id"])
        elif isinstance(x, dict):
            for v in x.values():
                collect(v)
        elif isinstance(x, list):
            for v in x:
                collect(v)

    collect(rows)
    if not eids:
        return rows
    res = query({":find": ["?e", "?ident"], ":in": [["?e", "..."]],
                 ":where": [["?e", ":db/ident", "?ident"]]},
                args=[sorted(eids)], db=db, session=session)
    idents = {e: i for e, i in res["query_result"]}
    if not idents:
        return rows

    def replace(x):
        if _is_eid_map(x):
            i = idents.get(x[":db/id"])
            return {":db/ident": i} if i is not None else x
        if isinstance(x, dict):
            return {k: replace(v) for k, v in x.items()}
        if isinstance(x, list):
            return [replace(v) for v in x]
        return x

    return replace(rows)


def across_dbs(q: Query, dbs: Iterable[str], args: Optional[List[Any]] = None,
               **kwargs) -> pd.DataFrame:
    """Run the same query against several databases and concatenate the results,
    adding a "db" column."""
    parts = []
    for db in dbs:
        df = do_query(q, args=args, db=db, **kwargs)
        if len(df):
            df["db"] = db
            parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def datoms(index: str, components: Optional[List[Any]] = None, db: Optional[str] = None,
           offset: int = 0, limit: int = 1000, timeout: int = 30,
           session=None, db_name: Optional[str] = None) -> pd.DataFrame:
    """Read raw datoms from an index ("eavt", "aevt", "avet", "vaet")."""
    db = config.ensure_db(db or db_name)
    http = session or requests
    body = {"index": ":" + index.lstrip(":"), "components": components or [],
            "offset": offset, "limit": limit}
    resp = http.post(f"{config.query_server()}/datoms/{db}", data=json.dumps(body),
                     headers={**_headers("application/json"), "Content-Type": "application/json"},
                     timeout=timeout + 2)
    _raise_for(resp, "Datoms request")
    res = resp.json()
    rows = [{k.lstrip(":"): v for k, v in d.items()} for d in res.get("datoms_chunk", [])]
    df = pd.DataFrame(rows, columns=["e", "a", "v", "tx"])
    return pqres.add_provenance(df, {"db_name": db, "basis_t": res.get("basis_t")})


def list_datasets() -> pd.DataFrame:
    """Datasets available to your API key: dataset, db (current database name),
    patient_count, sample_count, assays, tags."""
    resp = requests.get(f"{config.query_server()}/api-v1/list/datasets",
                        headers=_headers("application/json"), timeout=60)
    _raise_for(resp, "Listing datasets")
    rows = []
    for d in resp.json().get("datasets", []):
        rows.append({
            "dataset": d.get("dataset/name"),
            "db": (d.get("dataset/database") or {}).get("database/name"),
            "patient_count": d.get("dataset/patient-count"),
            "sample_count": d.get("dataset/sample-count"),
            "assays": d.get("dataset/assays", []),
            "tags": d.get("dataset/tags", []),
        })
    return pd.DataFrame(rows, columns=["dataset", "db", "patient_count", "sample_count", "assays", "tags"])


def resolve_db(dataset: str) -> str:
    """Resolve a dataset name (stable, e.g. "tcga-uvm") to its current database
    name (changes on re-import). A database name is passed through."""
    ds = list_datasets()
    hit = ds.loc[ds["dataset"] == dataset, "db"]
    if len(hit) and hit.iloc[0]:
        return hit.iloc[0]
    if dataset in set(ds["db"]):
        return dataset
    raise KeyError(f"Unknown dataset '{dataset}'")


def measurement_matrix(matrix_key: str, db: Optional[str] = None,
                       db_name: Optional[str] = None) -> pd.DataFrame:
    """Download a measurement matrix (e.g. single-cell counts) by its backing-file
    key; find keys with dataset.measurement_matrices()."""
    db = config.ensure_db(db or db_name)
    resp = requests.post(f"{config.query_server()}/matrix/{db}/{matrix_key}", data="{}",
                         headers={**_headers("text/plain"), "Content-Type": "application/json"},
                         timeout=120)
    _raise_for(resp, "Matrix request")
    content = _fetch_presigned(resp.text)
    return pd.read_csv(io.BytesIO(content), sep="\t", header=0)


# backwards compatible names from patternq 0.2
get_measurement_matrix = measurement_matrix


def set_db(db_name: str):
    """Deprecated: use patternq.config.set_db / patternq.set_db."""
    config.set_db(db_name)
    return True
