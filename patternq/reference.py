"""Reference data queries. Reference entities (genes, gene products, proteins,
epitopes, cell types, ...) are included in each dataset database, so these take
a db like every other query. Mirrors R/patternq/R/queries_reference.R."""
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from patternq import query as pqq
from patternq import results as pqres


def _names(attr: str, db, **kwargs) -> List[str]:
    df = pqq.do_query({":find": ["?name"], ":where": [["_", attr, "?name"]]}, db=db, **kwargs)
    return df["name"].tolist()


def gene_symbols(db: Optional[str] = None, **kwargs) -> List[str]:
    """HGNC gene symbols."""
    return _names(":gene/hgnc-symbol", db, **kwargs)


def genes_query():
    return {":find": [["pull", "?g", [":gene/hgnc-symbol", ":gene/hgnc-id", ":gene/hgnc-name",
                                      ":gene/ensembl-id", ":gene/previous-hgnc-symbols",
                                      ":gene/alias-hgnc-symbols",
                                      {":gene/hgnc-locus-group": [":db/ident"]}]]],
            ":where": [["?g", ":gene/hgnc-symbol"]]}, []


def genes(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """One row per gene: gene_hgnc_symbol, gene_hgnc_name, ids, and list columns
    gene_previous_hgnc_symbols, gene_alias_hgnc_symbols."""
    q, args = genes_query()
    return pqq.do_query(q, args, db=db, **kwargs)


def gene_products(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """gene_product_id, hgnc_symbol."""
    return pqq.do_query({":find": ["?gene-product-id", "?hgnc-symbol"],
                         ":where": [["?gp", ":gene-product/id", "?gene-product-id"],
                                    ["?gp", ":gene-product/gene", "?g"],
                                    ["?g", ":gene/hgnc-symbol", "?hgnc-symbol"]]}, db=db, **kwargs)


def gene_coordinates(db: Optional[str] = None, genes: Optional[Sequence[str]] = None, **kwargs) -> pd.DataFrame:
    """hgnc_symbol, assembly, contig, strand, start, end."""
    q = {":find": ["?hgnc-symbol", "?assembly", "?contig", "?strand", "?start", "?end"],
         ":where": [["?g", ":gene/hgnc-symbol", "?hgnc-symbol"],
                    ["?g", ":gene/genomic-coordinates", "?gc"],
                    ["?gc", ":genomic-coordinate/assembly", "?a"],
                    ["?a", ":db/ident", "?assembly"],
                    ["?gc", ":genomic-coordinate/contig", "?contig"],
                    ["?gc", ":genomic-coordinate/strand", "?strand"],
                    ["?gc", ":genomic-coordinate/start", "?start"],
                    ["?gc", ":genomic-coordinate/end", "?end"]]}
    args: List[Any] = []
    if genes is not None:
        q[":in"] = [["?hgnc-symbol", "..."]]
        args = [list(genes)]
    df = pqq.do_query(q, args, db=db, **kwargs)
    df["assembly"] = df["assembly"].map(pqres.ident_name)
    return df


def variant_annotations_query(variant_ids: Optional[Sequence[str]] = None,
                              genes: Optional[Sequence[str]] = None):
    where: List[Any] = [["?v", ":variant/id", "?variant-id"]]
    ins: List[Any] = []
    args: List[Any] = []
    if variant_ids is not None:
        ins.append(["?variant-id", "..."])
        args.append(list(variant_ids))
    if genes is not None:
        where += [["?v", ":variant/gene", "?g"], ["?g", ":gene/hgnc-symbol", "?gene"]]
        ins.append(["?gene", "..."])
        args.append(list(genes))
    q = {":find": [["pull", "?v", [
        ":variant/id", ":variant/HGVSp", ":variant/HGVSc", ":variant/ref-allele", ":variant/alt-allele",
        ":variant/coordinate-string", ":variant/dbSNP", ":variant/max-af", ":variant/external-ids",
        {":variant/gene": [":gene/hgnc-symbol"]},
        {":variant/impact": [":db/ident"]},
        {":variant/classification": [":db/ident"]},
        {":variant/type": [":db/ident"]},
        {":variant/so-consequences": [":so-sequence-feature/name"]}]]],
         ":in": ins, ":where": where}
    return q, args


def variant_annotations(db: Optional[str] = None, variant_ids: Optional[Sequence[str]] = None,
                        genes: Optional[Sequence[str]] = None, **kwargs) -> pd.DataFrame:
    """Variant reference entities (not measurements): variant_id, hgnc_symbol,
    HGVSp, HGVSc, impact, so_consequences, classification, ..."""
    q, args = variant_annotations_query(variant_ids, genes)
    df = pqq.do_query(q, args, db=db, **kwargs)
    for col in ("variant_HGVSp", "variant_HGVSc", "variant_so_consequences", "variant_external_ids"):
        if col in df.columns:
            df[col] = df[col].map(pqres.join_many)
    out = df.rename(columns=lambda c: c[len("variant_"):] if c.startswith("variant_") else c)
    out = out.rename(columns={"gene_hgnc_symbol": "hgnc_symbol", "id": "variant_id"})
    return pqres.keep_provenance(pqres.order_columns(out, ["variant_id", "hgnc_symbol", "HGVSp", "HGVSc", "impact"]), df)


def cnvs(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """CNV segment entities: cnv_id, coordinates, list column genes (gene_hgnc_symbol)."""
    return pqq.do_query({":find": [["pull", "?c", [
        ":cnv/id",
        {":cnv/genomic-coordinates": [":genomic-coordinate/contig", ":genomic-coordinate/start",
                                      ":genomic-coordinate/end"]},
        {":cnv/genes": [":gene/hgnc-symbol"]}]]],
        ":where": [["?c", ":cnv/id"]]}, db=db, **kwargs)


def gdc_anatomic_sites(db: Optional[str] = None, **kwargs) -> List[str]:
    return _names(":gdc-anatomic-site/name", db, **kwargs)


def epitopes(db: Optional[str] = None, **kwargs) -> List[str]:
    return _names(":epitope/id", db, **kwargs)


def cell_types(db: Optional[str] = None, **kwargs) -> List[str]:
    return _names(":cell-type/co-name", db, **kwargs)


def meddra_diseases(db: Optional[str] = None, **kwargs) -> List[str]:
    return _names(":meddra-disease/preferred-name", db, **kwargs)


def drugs(db: Optional[str] = None, **kwargs) -> List[str]:
    return _names(":drug/preferred-name", db, **kwargs)


def proteins(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    return pqq.do_query({":find": [["pull", "?p", [":protein/preferred-name", ":protein/uniprot-name",
                                                   ":protein/uniprot-accessions",
                                                   {":protein/gene": [":gene/hgnc-symbol"]}]]],
                         ":where": [["?p", ":protein/preferred-name"]]}, db=db, **kwargs)


def db_idents(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """db_id, db_ident."""
    return pqq.do_query({":find": ["?db-id", "?db-ident"], ":where": [["?db-id", ":db/ident", "?db-ident"]]},
                        db=db, **kwargs)


def map_gene_symbols(symbols: Sequence[str], all_genes: Optional[pd.DataFrame] = None,
                     db: Optional[str] = None, warn: bool = True) -> Dict[str, Optional[str]]:
    """Map gene symbols (current, previous or alias; case insensitive) to
    current HGNC symbols. Returns {input symbol: HGNC symbol or None}."""
    import warnings
    if all_genes is None:
        all_genes = genes(db)
    lookup = {s.upper(): s for s in all_genes["gene_hgnc_symbol"]}
    for col in ("gene_previous_hgnc_symbols", "gene_alias_hgnc_symbols"):
        if col not in all_genes.columns:
            continue
        for current, olds in zip(all_genes["gene_hgnc_symbol"], all_genes[col]):
            if isinstance(olds, str):
                olds = [olds]
            if not isinstance(olds, list):
                continue
            for o in olds:
                lookup.setdefault(o.upper(), current)  # current symbols take precedence
    out = {s: lookup.get(s.upper()) for s in symbols}
    missing = sum(v is None for v in out.values())
    if warn and missing:
        warnings.warn(f"{missing} of {len(out)} gene symbols could not be mapped")
    return out


# -- patternq 0.2 names --------------------------------------------------------

def all_variants(db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    return variant_annotations(db, **kwargs)


def variant_info(variant_ids: Sequence[str], db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    return variant_annotations(db, variant_ids=variant_ids, **kwargs)


def variants_for_genes(genes: Sequence[str], db: Optional[str] = None, **kwargs) -> pd.DataFrame:
    return variant_annotations(db, genes=genes, **kwargs)
