import pandas as pd
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
