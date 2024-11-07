import pandas as pd
import patternq.query as pqq
import patternq.dataset as pqd
import patternq.reference as pqr
import patternq.helpers as pqh
from patternq.helpers import flatten_enum_idents

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


meas_gen = pqd.measurement_generator()
next(meas_gen)


for i in range(1000000):
    print(next(meas_gen))


patients = pqd.all_subjects(dataset="tcga-brca", db_name="tcga-brca")
patients


