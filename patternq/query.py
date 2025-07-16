import json
import os
import requests
from io import BytesIO
from tempfile import NamedTemporaryFile
import gzip as gz

from typing import Any, List, Dict

import pandas as pd


db = None

def commons_endpoint() -> str:
    default = "https://data-commons.rcrf-dev.org"
    endpoint = os.getenv("PATTERNQ_ENDPOINT")
    if not (endpoint and endpoint.startswith("http")):
        endpoint = default
    return endpoint

def make_headers(accept="text/plain") -> Dict[str, str]:
    bearer_token = os.getenv('PATTERNQ_API_KEY')
    if not bearer_token:
        raise Exception("Must set PATTERNQ_API_KEY in environment to use query!")
    return {"Authorization": f"Bearer {bearer_token}",
            "Accept": accept}


def list_datasets() -> List[str]:
    endpoint = f"{commons_endpoint()}/api-v1/list/datasets"
    headers = make_headers(accept="application/json")
    resp = requests.post(endpoint, headers=headers)
    if resp.status_code != 200:
        resp.raise_for_status()
    else:
        return resp.json()


def set_db(db_name: str):
    """Sets a database as default query target for the duration of the session."""
    global db
    # todo: guard via API call to ensure that db_name exists.
    db = db_name
    return True

def query(q_dict: Dict[str, List[Any]], args:List[Any] or None = None, session: requests.Session or None = None,
          timeout: int = 30, db_name: str or None = None):
    """Issue a query to the Pattern.org Data Commons query service.
    If `session` is provided, will use an existing requests session and its connection pool.
    Use this to batch multiple queries.

    TODO: can strengthen type signature of query by referring to Datomic Datalog
    query grammar."""
    if not session:
        session = requests
    req_body = {"query": q_dict,
                "timeout": timeout * 1000}  # sec -> msec
    if args:
        req_body['args'] = args
    # use default module wide db if no db_name arg is passed.
    if db_name is None:
        db_name = db
    # build request and issue to query server
    headers = make_headers()
    endpoint = f"{commons_endpoint()}/query/{db_name}"
    resp = requests.post(
        endpoint,
        json.dumps(req_body),
        headers=headers,
        timeout=(timeout + 2)  # little buffer past candel query timeout
    )
    if resp.status_code == 200:
        dl_path = resp.content
        dl_resp = requests.get(dl_path)
        qres = json.loads(gz.decompress(dl_resp.content))
        qres["db_name"] = db_name
        return qres
    else:
        print("Query encountered an error:")
        try:
            print(resp.content)
            print(json.loads(resp.content))
        finally:
            resp.raise_for_status()

def datoms(index, components, offset=0, limit=1000,
           session=None, timeout=30, db_name=None):
    if not session:
        session = requests
    req_body = {"index": index,
                "components": components,
                "offset": offset,
                "limit": limit}
    if db_name is None:
        db_name = db
    headers = make_headers(accept="application/json")
    endpoint = f"{commons_endpoint()}/datoms/{db_name}"
    resp = requests.post(
        endpoint,
        json.dumps(req_body),
        headers=headers,
        timeout=(timeout + 2)
    )
    if resp.status_code == 200:
        return json.loads(resp.content)
    else:
        print("Datoms call encountered an error:")
        try:
            print(resp.content)
            print(json.loads(resp.content))
        finally:
            resp.raise_for_status()


def get_measurement_matrix(matrix_key: str, session: requests.Session or None = None,
                           db_name: str or None = None):
    if not session:
        session = requests
    req_body = {}
    if db_name is None:
        db_name = db
    headers = make_headers(accept="text/plain")
    endpoint = f"{commons_endpoint()}/matrix/{db_name}/{matrix_key}"
    # TODO: cache/store to named temporary file?
    fd = NamedTemporaryFile(mode="wb", delete=False)
    print(f"Writing measurement matrix to local disk as: {fd.name}")
    try:
        resp = requests.post(
            endpoint,
            json.dumps(req_body),
            headers=headers,
        )
        s3_presigned_url = resp.content
        r = requests.get(s3_presigned_url, stream=True)
        for chunk in r.iter_content(chunk_size=1024*8):
            fd.write(chunk)
        fd.close()
        return pd.read_csv(fd.name, compression='gzip', header=0, sep='\t')
    except requests.exceptions.RequestException as e:
        # just re-raise until we decide how to handle
        raise e
