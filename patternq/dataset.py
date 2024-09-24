import pandas as pd
import patternq.query as pqq
import patternq.helpers as pqh
import patternq.trino as trino

samplesq = {
    ":find": [["pull", "?s", ["*",
                              {":sample/specimen": [":db/ident"]},
                              {":sample/timepoint": [":timepoint/id",
                                                     {":timepoint/treatment-regiment":
                                                          [":treatment-regiment/name"]}]},
                              {":sample/study-day": [":study-day/id"]},
                              {":sample/subject": [":subject/id"]},
                              {":sample/container": [":db/ident"]},
                              {":sample/gdc-anatomic-site": [":gdc-anatomic-site/name"]}]]],
    ":in": ["$", "?dataset-name"],
    ":where": [["?d", ":dataset/name", "?dataset-name"],
               ["?d", ":dataset/samples", "?s"]]
}


def samples(dataset, db_name=None, **kwargs):
    """Return all samples"""
    qres = pqq.query(samplesq, args=[dataset], db_name=db_name, **kwargs)
    prov_db_name = db_name if db_name else pqq.db
    basis_t = qres["basis_t"]
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    qres_df = pqh.add_provenance(qres_df, qres)
    return qres_df


datasetsq = {
    ":find": [["pull", "?d", [":dataset/name", ":dataset/description",
                              ":dataset/url", ":dataset/doi"]]],
    ":where": [["?d", ":dataset/name"]]
}


def datasets(db_name=None, **kwargs):
    """Returns all datasets contained in a database"""
    qres = pqq.query(datasetsq, db_name=db_name, **kwargs)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    qres_df = pqh.add_provenance(qres_df, qres)
    return qres_df


def assays_for_patient(id, **kwargs):
    qres = pqq.query(
        {":find" : ["?subject-id", "?sample-id", "?a-tech", "?a-name", "?ms-name"],
         ":in" : ["$", "?subject-id"],
         ":where" : [
             ["?p", ":subject/id", "?subject-id"],
             ["?s", ":sample/subject", "?p"],
             ["?s", ":sample/id", "?sample-id"],
             ["?m", ":measurement/sample", "?s"],
             ["?ms", ":measurement-set/measurements", "?m"],
             ["?a", ":assay/measurement-sets", "?ms"],
             ["?ms", ":measurement-set/name", "?ms-name"],
             ["?a", ":assay/technology", "?at"],
             ["?a", ":assay/name", "?a-name"],
             ["?at", ":db/ident", "?a-tech"]]},
        args=["TCGA-A7-A0DB"],
        **kwargs
    )
    col_vars = ["subject-id", "sample-id", "assay-tech",
                "assay-name", "measurement-set-name"]
    return pd.DataFrame(qres["query_result"], columns=col_vars)


# TBD: query builder pattern lab that's presto SQL compatible to handle
#      avoiding injection, programmatic patterns, etc.
def measurements_sql(measurement_set, measurement_attr):
    return f"""select sample.id, gene.hgnc_symbol, m.{measurement_attr}
               from measurement m
               join measurement_set_x_measurements msxm
                 on msxm.db__id = m.db__id
               join measurement_set ms
                 on ms.db__id = msxm.db__id
               join sample
                 on m.sample = sample.db__id
               join gene_product gp
                on m.gene_product = gp.db__id
               join gene
                 on gp.gene = gene.db__id
               where ms.name = '{measurement_set}'
"""


def measurements(measurement_set, measurement_attr):
    sql_query = measurements_sql(measurement_set, measurement_attr)
    rows = trino.query(sql_query)
    return pd.DataFrame(rows, columns=["sample-id", "hgnc", "rsem"])
