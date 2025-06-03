from collections import namedtuple
import patternq.query as pqq

SchemaInfo = namedtuple('SchemaInfo', ['name', 'version'])

schema_info_query = {
    ":find": ["?n", "?v"],
    ":in": ["$"],
    ":where": [
        ["?s", ":unify.schema/version", "?v"],
        ["?s", ":unify.schema/name", "?n"]
    ]
}

def schema_info(db_name=None, **kwargs):
    """
    Return schema version from a CANDEL-style database.
    """
    prov_db_name = db_name if db_name else pqq.db
    qres = pqq.query(schema_info_query, db_name=prov_db_name, **kwargs)
    return SchemaInfo(*qres['query_result'][0])
