import pandas as pd
import patternq.dataset as pqd


sub_id = "TCGA-A7-A0DB"
assays = pqd.assays_for_patient(sub_id, db_name='tcga-brca')
assays

samples = pqd.samples("tcga-brca", db_name='tcga-brca')
samples

datasets = pqd.datasets(db_name="tcga-brca")
datasets
datasets.columns

measurements = pqd.measurements("gx", "rsem_normalized_count")
measurements
