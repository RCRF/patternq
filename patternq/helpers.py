from functools import partial
from datetime import datetime
import pandas as pd
from collections import namedtuple

clean_names_dict = {
    ":": "",
    "?": "",
    "/": "-",
    ".": "-"
}

PatternQProvenance = namedtuple(
    "PatternQProvenance",
    [
        'unify_db_name',
        'unify_basis_t',
        'client_timestamp'
    ]
)


# as in clojure walk/postwalk.
def walk(inner, outer, coll):
    if isinstance(coll, list) or isinstance(coll, tuple):
        return outer([inner(e) for e in coll])
    elif isinstance(coll, dict):
        return outer(dict([inner(e) for e in coll.items()]))
    else:
        return outer(coll)


def postwalk(fn, coll):
    return walk(partial(postwalk, fn), fn, coll)


def maybe_flatten_enum(elem):
    if isinstance(elem, dict):
        if ":db/ident" in elem:
            return elem[":db/ident"]
    return elem

def flatten_enum_idents(qres):
    """Given query results as parsed by json, flattens all enums to contain
    only the ident value, i.e. :some/ident rather than a dictionary of
    {:db/ident :some/ident}"""
    return postwalk(maybe_flatten_enum, qres)

def expand_many_nested(qres_df, attribute):
    """Given query results as previously JSON normalized in a data frame, we extract
    the nested entities, normalize them separately, then join them back in appropriately."""
    qres_df = qres_df.explode(column=attribute)
    qres_df["nested-db-id###"] = qres_df[attribute].apply(lambda l: l[":db/id"])
    nested_entities = pd.json_normalize(qres_df[attribute].tolist())
    nested_entities = clean_column_names(nested_entities)
    result = qres_df.merge(
        nested_entities,
        left_on="nested-db-id###",
        right_on="db-id",
        how="left"
    )
    result = result.drop(columns=['db-id_x', 'db-id_y', 'db-id', 'nested-db-id###'], errors='ignore')
    return result

def clean_column_names(df):
    """Clean the column names as returned by common patternq queries, with or
    without json normalize processing from pandas"""
    new_col_names = {}
    for col in df.columns:
        for orig, new in clean_names_dict.items():
            sofar = new_col_names.get(col, col)
            new_col_names[col] = sofar.replace(orig, new)
    # A little clunky, but we do an inplace rename and return to
    # keep the appearance of a functional style while avoiding
    # memcopy. Not an issue for how patternq uses this lib, but
    # external users should be mindful when applying to their
    # own data.
    df.rename(columns=new_col_names, inplace=True)
    return df


def pull2fields(qres):
    """For a query that contains a single 'column' / relation of one that
    where the values are the results of a pull expression, flatten the
    pull expression into fields using common assumptions.
    """
    query_result = qres["query_result"]
    query_result = flatten_enum_idents(query_result)
    flat_df = pd.DataFrame(query_result, columns=["pull"])
    return pd.json_normalize(flat_df['pull'])


def add_provenance(df, qres):
    """Adds provenance ysing pandas documented _metadata field support,
    ref: https://pandas.pydata.org/pandas-docs/stable/development/extending.html#define-original-properties

    _Note_: _metadata will propagate, and despite 'protected member' name
    convention, this is how pandas documents adding metadata that will
    propagate through dataframe transformations."""
    now = datetime.now()
    metadata = PatternQProvenance(qres["db_name"],
                                  qres["basis_t"],
                                  now.strftime("%Y-%m-%d %H:%M:%S"))
    df.patternq_provenance = metadata
    return df


def get_provenance(df):
    """Returns patternq provenance metadata if contained in dataframe,
    otherwise None"""
    try:
        return df.patternq_provenance
    except:
        return None

def print_provenance(df):
    prov = get_provenance(df)
    if prov:
        print(prov)
        return
    print("Did not find PatternQ / Unify metadata attached to DataFrame!")
    return
