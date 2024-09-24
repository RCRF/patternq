import patternq.query as pqq
import patternq.helpers as pqh

gene_symbols_query = {
    ":find": ["?hgnc"],
    ":where":
    [["_", ":gene/hgnc-symbol", "?hgnc"]]
}

def gene_symbols(db_name=None, **kwargs):
    """Returns all gene symbols in the database as a list."""
    qres = pqq.query(gene_symbols_query,
                     db_name=db_name,
                     **kwargs)
    # query returns set of relations, in this case
    # [[gene1],[gene2],[gene3]], so we flatten.
    genes = [relation[0] for relation in qres["query_result"]]
    return genes

genes_query = {
    ":find": [["pull", "?g", ["*"]]],
    ":where":
    [["?g", ":gene/hgnc-symbol"]]
}

def genes(db_name=None, **kwargs):
    """Returns all genes and associated attributes from reference
    data in database specified by db_name, or default database
    for session."""
    qres = pqq.query(genes_query,
                     db_name=db_name,
                     **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df

