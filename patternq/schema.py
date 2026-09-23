"""Schema name and version of a database."""
from collections import namedtuple
from typing import Optional

from patternq import query as pqq

SchemaInfo = namedtuple("SchemaInfo", ["name", "version"])

schema_info_query = {
    ":find": ["?n", "?v"],
    ":where": [["?s", ":unify.schema/version", "?v"],
               ["?s", ":unify.schema/name", "?n"]]
}


def schema_info(db: Optional[str] = None, **kwargs) -> SchemaInfo:
    """Schema name and version (Unify metadata) of a database."""
    qres = pqq.query(schema_info_query, db=db, **kwargs)
    return SchemaInfo(*qres["query_result"][0])
