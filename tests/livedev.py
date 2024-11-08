import pandas as pd
import patternq.query as pqq
import patternq.dataset as pqd
import patternq.reference as pqr
import patternq.helpers as pqh

sub_id = "TCGA-A7-A0DB"
assays_for_sub = pqd.assays_for_patient(sub_id, db_name='tcga-brca')
assays_for_sub

samples = pqd.samples("tcga-brca", db_name='tcga-brca')
samples
samples_sub = samples[samples["sample-subject-subject-id"] == sub_id]

datasets = pqd.datasets(db_name="tcga-brca")
datasets

gsyms = pqr.gene_symbols(db_name='tcga-brca')
gsyms

genes = pqr.genes(db_name='tcga-brca', timeout=90)
genes

# -- this takes a little while, so uncomment with care --
# measurements = pqd.measurements("gx", "rsem_normalized_count")
# measurements

sites = pqr.gdc_anatomic_sites(db_name='tcga-brca')
sites


assays = pqd.assay_summary("tcga-brca", db_name='tcga-brca')
assays[["assay-name", "measurement-set-name"]]

clin_sum = pqd.clinical_summary("tcga-brca", db_name='tcga-brca')
clin_sum

sample_for_meas = assays_for_sub[assays_for_sub['measurement-set-name'] == 'baseline mutations']
sample_for_meas = sample_for_meas.iloc[0]['sample-id']
sample_meas = pqd.sample_measurements('tcga-brca', 'baseline mutations',
                                      [sample_for_meas], db_name='tcga-brca')
sample_meas

var = pqr.variants(sample_meas['measurement-variant-variant-id'].tolist(), db_name='tcga-brca')
var
# -- for tcga, this is massive, but not always --
# vars = pqr.variants("tcga-brca", timeout=120)
# vars


# -- for tcga, too large
# meas = pqd.all_measurements("tcga-brca", "baseline mutations", db_name="tcga-brca",
                            timeout=120)
#.meas


patients = pqd.all_subjects("tcga-brca", db_name="tcga-brca")
patients

