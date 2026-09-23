"""Session configuration: query service endpoint, API token, default database.

Resolution order for the endpoint and token: values set in-session with
set_query_server / set_token, then the PATTERNQ_ENDPOINT / PATTERNQ_API_KEY
environment variables, then (endpoint only) the default dev server.
"""
import os
from typing import Optional

DEFAULT_ENDPOINT = "https://data-commons.rcrf-dev.org"

_endpoint: Optional[str] = None
_token: Optional[str] = None
_db: Optional[str] = None


def query_server() -> str:
    """The query service endpoint in use."""
    if _endpoint:
        return _endpoint
    env = os.getenv("PATTERNQ_ENDPOINT")
    if env and env.startswith("http"):
        return env.rstrip("/")
    return DEFAULT_ENDPOINT


def set_query_server(url: Optional[str]) -> Optional[str]:
    """Set the query service endpoint for this session. Returns the previous value."""
    global _endpoint
    old = _endpoint
    _endpoint = url.rstrip("/") if url else None
    return old


def set_token(token: Optional[str]) -> Optional[str]:
    """Set the API token for this session, overriding PATTERNQ_API_KEY."""
    global _token
    old = _token
    _token = token
    return old


def api_token() -> str:
    token = _token or os.getenv("PATTERNQ_API_KEY")
    if not token:
        raise RuntimeError("No API token: set PATTERNQ_API_KEY in the environment or call set_token()")
    return token


def set_db(db: Optional[str]) -> Optional[str]:
    """Set the default database for this session.

    Every dataset is its own database, so the database name selects the dataset.
    Use resolve_db() to go from a dataset name (e.g. "tcga-uvm") to its current
    database name. Returns the previous value."""
    global _db
    old = _db
    _db = db
    return old


def current_db() -> Optional[str]:
    """The default database for this session, if set."""
    return _db


def ensure_db(db: Optional[str]) -> str:
    db = db or _db
    if not db:
        raise RuntimeError("No database given: pass db=... or call set_db()")
    return db
