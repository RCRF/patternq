import pandas as pd
import patternq.query as pqq
import patternq.dataset as pqd
import patternq.reference as pqr
import patternq.helpers as pqh

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


assays = pqd.assay_summary("tcga-brca", db_name='tcga-brca')
assays[["assay-name", "measurement-set-name"]]

clin_sum = pqd.clinical_summary("tcga-brca", db_name='tcga-brca')
clin_sum

result
result["measurement-set-measurements"]


patients = pqd.all_subjects(dataset="tcga-brca", db_name="tcga-brca")
patients


