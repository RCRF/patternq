from typing import List

import patternq.helpers as pqh
import patternq.query as pqq

gene_symbols_query = {
    ":find": ["?hgnc"],
    ":where":
        [["_", ":gene/hgnc-symbol", "?hgnc"]]
}


def gene_symbols(db_name: str or None = None, **kwargs):
    """Returns all gene symbols in the database as a list."""
    qres = pqq.query(gene_symbols_query,
                     db_name=db_name,
                     **kwargs)
    # query returns set of relations, in this case
    # [[gene1],[gene2],[gene3]], so we flatten.
    gene_syms = [relation[0] for relation in qres["query_result"]]
    return gene_syms


genes_query = {
    ":find": [["pull", "?g", ["*"]]],
    ":where":
        [["?g", ":gene/hgnc-symbol"]]
}


def genes(db_name: str or None = None, **kwargs):
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


gdc_anatomic_sites_query = {
    ":find": ["?gdc-site"],
    ":where":
        [["_", ":gdc-anatomic-site/name", "?gdc-site"]]
}


def gdc_anatomic_sites(db_name: str or None = None, **kwargs):
    qres = pqq.query(gdc_anatomic_sites_query,
                     db_name=db_name,
                     **kwargs)
    gdc_sites = [relation[0] for relation in qres["query_result"]]
    return gdc_sites

variant_pull = [":variant/ref-allele",
                ":variant/alt-allele",
                ":variant/alt-allele-2",
                ":variant/id",
                ":variant/ref-amino-acid",
                ":variant/alt-amino-acid",
                {":variant/classification": [":db/ident"]},
                {":variant/type": [":db/ident"]},
                {":variant/feature": [":db/ident"]},
                {":variant/genomic-coordinates": [":genomic-coordinate/id"]},
                {":variant/so-consequences": [":so-sequence-feature/name", ":db/id"]},
                {":variant/gene": [":gene/hgnc-symbol"]}]

all_variants_q = {
    ":find": [["pull", "?v", variant_pull]],
    ":where":
        [["?v", ":variant/id"]]
}


def all_variants(db_name: str or None = None, **kwargs):
    qres = pqq.query(all_variants_q, db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    if "variant-so-consequences" in qres_df.columns:
        qres_df = pqh.expand_many_nested(qres_df, "variant-so-consequences")
    return qres_df


variant_q = all_variants_q.copy()
variant_q[":in"] = ["$", ["?variant-id", "..."]]
variant_q[":where"][0].append("?variant-id")


def variant_info(variant_ids: List[str], db_name: str or None = None, **kwargs):
    qres = pqq.query(variant_q, args=[variant_ids], db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    # TODO: investigate how this is working, atm it breaks things.
    # if "variant-so-consequences" in qres_df.columns:
    #    qres_df = pqh.expand_many_nested(qres_df, "variant-so-consequences")
    return qres_df

genes2variantsq = {
    ":find": [["pull", "?v", variant_pull]],
    ":in": ["$", ["?hgnc", "..."]],
    ":where":
    [["?g", ":gene/hgnc-symbol", "?hgnc"],
     ["?v", ":variant/gene", "?g"]]
}

def variants_for_genes(genes: List[str], db_name: str or None = None, **kwargs):
    qres = pqq.query(genes2variantsq, args=[genes], db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df
