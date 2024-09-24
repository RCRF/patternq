import json
import os
import requests
import gzip as gz

db = None

def set_db(db_name):
    """Sets a database as default query target for the duration of the session."""
    global db
    # todo: guard via API call to ensure that db_name exists.
    db = db_name
    return True

# todo: list databases

def commons_endpoint():
    default = "http://data-commons-query-dev.us-east-1.elasticbeanstalk.com"
    endpoint = os.getenv("PATTERNQ_ENDPOINT")
    if not (endpoint and endpoint.startswith("http")):
        endpoint = default
    return endpoint

def make_headers():
    bearer_token = os.getenv('PATTERNQ_BEARER_TOKEN')
    if not bearer_token:
        raise Exception("Must set PATTERNQ_BEARER_TOKEN in environment to use query!")
    return {"Authorization": f"Bearer {bearer_token}",
            "Accept": "text/plain"}


def query(q_dict, args=None, session=None, timeout=30, db_name=None):
    """Issue a query to the Pattern.org Data Commons query service.
    If `session` is provided, will use an existing requests session and its connection pool.
    Use this to batch multiple queries."""
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
