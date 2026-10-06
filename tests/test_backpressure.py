"""API back-pressure contract (unify-central dev-docs/API_BACKPRESSURE.md),
against a local stub server."""
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import patternq as pq
from patternq import backpressure as bp
from patternq import query as pqq

PROBLEM = "urn:pattern-data-commons:problem:"
OK = (200, {"Content-Type": "application/json", "RateLimit": '"api";r=99;t=0'},
      {"query_result": [[1]], "basis_t": 7})


def throttled(kind="rate-limited", status=429, retry_after=None, retryable=True):
    headers = {"Content-Type": "application/problem+json"}
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return (status, headers, {"type": PROBLEM + kind, "title": "throttled", "status": status,
                              "detail": f"{kind} detail", "retryable": retryable})


class Stub:
    """Serves scripted responses in order (the last one repeats) and records
    the paths it was asked for and the peak number of requests in flight."""

    def __init__(self, responses, delay=0.0):
        self.responses = list(responses)
        self.delay = delay
        self.paths = []
        self.inflight = 0
        self.peak = 0
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def handle_one(self):
                n = int(self.headers.get("Content-Length") or 0)
                self.rfile.read(n)
                with stub.lock:
                    stub.paths.append(self.path)
                    stub.inflight += 1
                    stub.peak = max(stub.peak, stub.inflight)
                    resp = stub.responses.pop(0) if len(stub.responses) > 1 else stub.responses[0]
                time.sleep(stub.delay)
                status, headers, body = resp
                data = (json.dumps(body) if not isinstance(body, str) else body).encode()
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                with stub.lock:
                    stub.inflight -= 1

            do_GET = do_POST = handle_one

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def stub(monkeypatch):
    sleeps = []
    monkeypatch.setattr(bp, "_sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(bp, "_next_allowed", 0.0)
    monkeypatch.setattr(bp, "_max_timeout_ms", bp.DEFAULT_MAX_TIMEOUT_MS)
    old_policy = bp.retry_policy()
    servers = []

    def make(responses, delay=0.0):
        s = Stub(responses, delay)
        s.sleeps = sleeps
        servers.append(s)
        pq.set_query_server(s.url)
        pq.set_token("test-token")
        return s

    yield make
    for s in servers:
        s.server.shutdown()
    pq.set_query_server(None)
    pq.set_token(None)
    bp.set_retry_policy(**old_policy)


Q = {":find": ["?x"], ":where": [["?x", ":db/ident"]]}


def test_retries_with_retry_after(stub, caplog):
    s = stub([throttled(retry_after=3), throttled("too-many-inflight", retry_after=1), OK])
    with caplog.at_level(logging.INFO, logger="patternq"):
        res = pqq.query(Q, db="db1", cache=False)
    assert res["query_result"] == [[1]]
    assert s.sleeps == [3.0, 1.0]
    assert len(s.paths) == 3
    assert "rate-limited" in caplog.text and "too-many-inflight" in caplog.text


def test_503_overloaded_is_retried(stub):
    s = stub([throttled("overloaded", status=503, retry_after=2), OK])
    assert pqq.query(Q, db="db1", cache=False)["basis_t"] == 7
    assert s.sleeps == [2.0]


def test_jittered_backoff_without_retry_after(stub):
    # a 429 that isn't ours (no problem+json, no Retry-After)
    s = stub([(429, {"Content-Type": "text/plain"}, "slow down")] * 3 + [OK])
    pq.set_retry_policy(max_backoff=3)
    pqq.query(Q, db="db1", cache=False)
    assert len(s.sleeps) == 3
    for attempt, wait in enumerate(s.sleeps, start=1):
        assert 0 <= wait <= min(3, 2 ** attempt)


def test_gives_up_after_max_retries_naming_the_problem(stub):
    s = stub([throttled(retry_after=1)])
    pq.set_retry_policy(max_retries=2)
    with pytest.raises(pq.ThrottledError) as e:
        pqq.query(Q, db="db1", cache=False)
    assert e.value.problem_type == PROBLEM + "rate-limited"
    assert "rate-limited detail" in str(e.value) and "2 retries" in str(e.value)
    assert len(s.paths) == 3


def test_not_retried(stub):
    for resp in [throttled(retryable=False), (502, {}, "bad gateway"), (504, {}, "timeout"),
                 (400, {"Content-Type": "application/json"}, {"error": "bad query"})]:
        s = stub([resp, OK])
        with pytest.raises(RuntimeError) as e:
            pqq.query(Q, db="db1", cache=False)
        assert not isinstance(e.value, pq.ThrottledError)
        assert len(s.paths) == 1 and s.sleeps == []


def test_query_timeout_400_advises_narrowing(stub):
    stub([(400, {"Content-Type": "application/json"},
           {"error": "Query canceled: timeout elapsed", "timeout": True,
            "type": PROBLEM + "query-timeout"})])
    with pytest.raises(RuntimeError, match="Narrow the query or page it"):
        pqq.query(Q, db="db1", cache=False)


def test_query_too_broad_is_surfaced_not_retried(stub):
    s = stub([(400, {"Content-Type": "application/json"},
               {"error": "Query too broad: clause [?e ?a ?v] binds neither attribute nor entity; bind the attribute", "type": PROBLEM + "query-too-broad",
                "reason": "full-scan", "clause": "[?e ?a ?v]"}), OK])
    with pytest.raises(RuntimeError, match="bind the attribute"):
        pqq.query(Q, db="db1", cache=False)
    assert len(s.paths) == 1 and s.sleeps == []


def test_self_throttles_on_r_zero(stub):
    s = stub([(200, {"Content-Type": "application/json", "RateLimit": '"api";r=0;t=1'},
               {"query_result": [], "basis_t": 1}), OK])
    pqq.query(Q, db="db1", cache=False)
    assert s.sleeps == []
    pqq.query(Q, db="db1", cache=False)
    assert len(s.sleeps) == 1 and 0 < s.sleeps[0] <= 1


def test_list_and_datoms_go_through_back_pressure(stub):
    s = stub([throttled(retry_after=1), (200, {"Content-Type": "application/json"}, {"datasets": []}),
              throttled(retry_after=1), (200, {"Content-Type": "application/json"},
                                         {"datoms_chunk": [], "basis_t": 1})])
    assert len(pq.list_datasets()) == 0
    assert len(pq.datoms("eavt", db="db1")) == 0
    assert s.sleeps == [1.0, 1.0]


def test_concurrency_capped_per_process(stub):
    s = stub([OK], delay=0.15)
    pq.set_retry_policy(max_concurrency=2)
    threads = [threading.Thread(target=pqq.query, args=(Q,), kwargs={"db": "db1", "cache": False})
               for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(s.paths) == 6 and s.peak == 2


def test_warns_above_advertised_timeout_cap(stub):
    stub([(200, {"Content-Type": "application/json", "PDC-Query-Max-Timeout-Ms": "60000"},
           {"query_result": [], "basis_t": 1})])
    with pytest.warns(UserWarning, match="cap of 120 s"):
        pqq.query(Q, db="db1", cache=False, timeout=200)
    assert bp.max_timeout_ms() == 60000
    with pytest.warns(UserWarning, match="cap of 60 s"):
        pqq.query(Q, db="db1", cache=False, timeout=90)


def test_policy_defaults():
    assert pq.retry_policy() == {"max_retries": 5, "max_backoff": 60.0, "max_concurrency": 4}
