"""Client side of the commons API back-pressure contract (unify-central
dev-docs/API_BACKPRESSURE.md), the same in the R, Python, Clojure and Julia
libraries.

- 429 and 503 are retried, up to max_retries times, when the body is a
  retryable application/problem+json, or is not problem+json at all (e.g. from
  a load balancer). Every other status, 502 and 504 included, is returned as is.
- The wait is Retry-After when the server sends it, otherwise exponential
  backoff with full jitter: random(0, min(max_backoff, 2^attempt)) seconds.
- Self-throttling: a RateLimit header with r=0 makes the next call wait t
  seconds, and at most max_concurrency /query and /datoms calls run at once in
  this process (the server's per-key cap is 4).
- Retries are logged at INFO on the "patternq" logger, with the problem type
  and the wait.

Presigned S3 downloads are not API calls and don't go through here.
"""
import logging
import random
import re
import threading
import time
from typing import Callable, Optional

import requests

log = logging.getLogger("patternq")

PROBLEM_PREFIX = "urn:pattern-data-commons:problem:"
DEFAULT_MAX_TIMEOUT_MS = 120000

_lock = threading.Lock()
_policy = {"max_retries": 5, "max_backoff": 60.0, "max_concurrency": 4}
_slots = threading.BoundedSemaphore(_policy["max_concurrency"])
_next_allowed = 0.0  # time.monotonic() before which no call is sent
_max_timeout_ms = DEFAULT_MAX_TIMEOUT_MS

# indirection so tests don't really sleep
_sleep = time.sleep


class ThrottledError(RuntimeError):
    """The commons API kept throttling a call past the retry limit."""

    def __init__(self, message: str, problem_type: Optional[str], status: int):
        super().__init__(message)
        self.problem_type = problem_type
        self.status = status


def set_retry_policy(max_retries: Optional[int] = None, max_backoff: Optional[float] = None,
                     max_concurrency: Optional[int] = None) -> dict:
    """Set how this session handles API back pressure. Arguments left as None
    keep their current value. Defaults: max_retries=5 retries per call,
    max_backoff=60 seconds (cap on the jittered backoff when the server sends no
    Retry-After), max_concurrency=4 concurrent /query and /datoms calls (the
    server's per-key cap). Returns the previous policy."""
    global _slots
    with _lock:
        old = dict(_policy)
        if max_retries is not None:
            _policy["max_retries"] = int(max_retries)
        if max_backoff is not None:
            _policy["max_backoff"] = float(max_backoff)
        if max_concurrency is not None and int(max_concurrency) != _policy["max_concurrency"]:
            if int(max_concurrency) < 1:
                raise ValueError("max_concurrency must be at least 1")
            _policy["max_concurrency"] = int(max_concurrency)
            _slots = threading.BoundedSemaphore(_policy["max_concurrency"])
    return old


def retry_policy() -> dict:
    """The current back-pressure policy (see set_retry_policy)."""
    return dict(_policy)


def max_timeout_ms() -> int:
    """The server's cap on requested query timeouts, from the last
    PDC-Query-Max-Timeout-Ms header seen (120000 until one is)."""
    return _max_timeout_ms


def _problem(resp: requests.Response) -> Optional[dict]:
    if not resp.headers.get("Content-Type", "").startswith("application/problem+json"):
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def _retry_after(resp: requests.Response, problem: Optional[dict]) -> Optional[float]:
    h = resp.headers.get("Retry-After")
    if h is not None:
        try:
            return max(0.0, float(h))
        except ValueError:
            pass  # an HTTP date: fall back to the body or backoff
    if problem and isinstance(problem.get("retry_after"), (int, float)):
        return max(0.0, float(problem["retry_after"]))
    return None


def _note_headers(resp: requests.Response):
    global _next_allowed, _max_timeout_ms
    rl = resp.headers.get("RateLimit")
    if rl:
        r = re.search(r"\br=(\d+)", rl)
        t = re.search(r"\bt=(\d+)", rl)
        if r and t and int(r.group(1)) == 0:
            with _lock:
                _next_allowed = max(_next_allowed, time.monotonic() + int(t.group(1)))
    mt = resp.headers.get("PDC-Query-Max-Timeout-Ms")
    if mt and mt.strip().isdigit():
        _max_timeout_ms = int(mt.strip())


def _wait_for_rate_limit():
    wait = _next_allowed - time.monotonic()
    if wait > 0:
        log.info("patternq: rate limit reached (RateLimit r=0); waiting %.1f s before the next call", wait)
        _sleep(wait)


def send(do_request: Callable[[], requests.Response], what: str,
         limited: bool = False) -> requests.Response:
    """Send an API request (do_request performs it) under the back-pressure
    contract. limited: the call counts toward the per-key concurrency cap
    (/query, /datoms). Returns the final response, which may still be an error
    for the caller to raise; raises ThrottledError when throttling outlasts
    max_retries."""
    attempt = 0
    while True:
        _wait_for_rate_limit()
        if limited:
            slots = _slots
            with slots:
                resp = do_request()
        else:
            resp = do_request()
        _note_headers(resp)
        if resp.status_code not in (429, 503):
            return resp
        problem = _problem(resp)
        if problem is not None and problem.get("retryable") is not True:
            return resp
        ptype = problem.get("type") if problem else None
        label = ptype or f"HTTP {resp.status_code}"
        if attempt >= _policy["max_retries"]:
            detail = (problem or {}).get("detail") or resp.text.strip()[:500]
            raise ThrottledError(
                f"{what} throttled by the commons API ({label}) and still throttled after "
                f"{attempt} retries: {detail}", ptype, resp.status_code)
        attempt += 1
        wait = _retry_after(resp, problem)
        if wait is None:
            wait = random.uniform(0, min(_policy["max_backoff"], 2.0 ** attempt))
        log.info("patternq: %s throttled (%s, HTTP %s); retry %d of %d in %.1f s",
                 what, label, resp.status_code, attempt, _policy["max_retries"], wait)
        _sleep(wait)
