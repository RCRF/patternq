import pandas as pd
import patternq.query as pqq
import patternq.dataset as pqd
import patternq.reference as pqr

sub_id = "TCGA-A7-A0DB"
assays = pqd.assays_for_patient(sub_id, db_name='tcga-brca')
assays

samples = pqd.samples("tcga-brca", db_name='tcga-brca')
samples

datasets = pqd.datasets(db_name="tcga-brca")
datasets
datasets.columns

gsyms = pqr.gene_symbols(db_name='tcga-brca')
gsyms
len(gsyms)

genes = pqr.genes(db_name='tcga-brca', timeout=90)
genes

measurements = pqd.measurements("gx", "rsem_normalized_count")
measurements

sites = pqr.gdc_anatomic_sites(db_name='tcga-brca')
sites


resp = pqq.datoms(":aevt", [":measurement/rsem-normalized-count"], db_name='tcga-brca')
sample_chunk = resp["datoms_chunk"]

def measurement_generator(meas_attr=":measurement/rsem-normalized-count", db_name='tcga-brca',
                          chunk_size=5000):
    offset = 0
    api_resp = pqq.datoms(":aevt", [meas_attr], db_name=db_name,
                          offset=offset, limit=chunk_size)
    chunk = api_resp["datoms_chunk"]
    while len(chunk) >= 1:
        meas_eids = [elem[":e"] for elem in chunk]
        meas_res = pqq.query({
            ":find": ["?m", "?hgnc", "?samp-id"],
            ":in": ["$", ["?m", "..."]],
            ":where":
                [["?m", ":measurement/gene-product", "?gp"],
                 ["?gp", ":gene-product/gene", "?g"],
                 ["?g", ":gene/hgnc-symbol", "?hgnc"],
                 ["?m", ":measurement/sample", "?s"],
                 ["?s", ":sample/id", "?samp-id"]]
            },
            args=[meas_eids],
            db_name='tcga-brca'
        )
        lookup = dict([(meas[0], (meas[1], meas[2])) for meas in meas_res["query_result"]])
        for datom in chunk:
            eid = datom[":e"]
            match = lookup[eid]
            yield eid, match[0], match[1], datom[":v"]
        offset += chunk_size
        api_resp = pqq.datoms(":aevt", [meas_attr], db_name=db_name,
                              offset=offset, limit=chunk_size)
        chunk = api_resp["datoms_chunk"]
    raise StopIteration

meas_gen = measurement_generator()
next(meas_gen)


for i in range(1000000):
    print(next(meas_gen))
