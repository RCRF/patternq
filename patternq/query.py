import json
import os
import requests
import gzip as gz


def commons_endpoint():
    default = "http://data-commons-query-dev.us-east-1.elasticbeanstalk.com/query"
    endpoint = os.getenv("PATTERNQ_ENDPOINT")
    if not (endpoint and endpoint.startswith("http")):
        endpoint = default
    return endpoint

def make_headers():
    bearer_token = os.getenv('BEARER_TOKEN')
    if not bearer_token:
        raise Exception("Must set BEARER_TOKEN in environment to use query!")
    return {"Authorization": f"Bearer {bearer_token}",
            "Accept": "text/plain"}


def query(q_dict, args=None, session=None, timeout=30, db_name='tcga-brca'):
    """Issue a query to the Pattern.org Data Commons query service.
    If `session` is provided, will use an existing requests session and its connection pool.
    Use this to batch multiple queries."""
    if not session:
        session = requests
    req_body = {"query": q_dict,
                "timeout": timeout * 1000}  # sec -> msec
    if args:
        req_body['args'] = args
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
        return qres
    else:
        print("Query encountered an error:")
        try:
            print(json.loads(resp.content))
        finally:
            resp.raise_for_status()