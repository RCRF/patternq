"""patternq: query and analysis tools for the Pattern Data Commons.

Recommended import aliases:

    import patternq as pq
    import patternq.query as pqq
    import patternq.dataset as pqd
    import patternq.reference as pqr
    import patternq.plots as pqp
    import patternq.analysis as pqa

Configure access with PATTERNQ_ENDPOINT / PATTERNQ_API_KEY, or set_query_server()
/ set_token(). Every dataset is its own database: select one with set_db() or
pass db= to query functions.
"""
from patternq.config import (current_db, query_server, set_db, set_query_server,
                             set_token)
# note: the query() function is not re-exported here, so that patternq.query
# stays the submodule (import patternq.query as pqq)
from patternq.query import (across_dbs, datoms, do_query, list_datasets,
                            measurement_matrix, resolve_db)
from patternq.results import provenance

__all__ = ["current_db", "query_server", "set_db", "set_query_server", "set_token",
           "across_dbs", "datoms", "do_query", "list_datasets", "measurement_matrix",
           "resolve_db", "provenance"]
