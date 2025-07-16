from typing import List

import pandas as pd

import patternq.helpers as pqh
import patternq.query as pqq

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


def samples(dataset: str, db_name: str or None = None, **kwargs):
    """Return all samples"""
    qres = pqq.query(samplesq, args=[dataset], db_name=db_name, **kwargs)
    prov_db_name = db_name if db_name else pqq.db
    basis_t = qres["basis_t"]
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


datasetsq = {
    ":find": [["pull", "?d", [":dataset/name", ":dataset/description",
                              ":dataset/url", ":dataset/doi"]]],
    ":where": [["?d", ":dataset/name"]]
}


def datasets(db_name: str or None = None, **kwargs):
    """Returns all datasets contained in a database"""
    raise NotImplementedError("Will be re-implemented after Unify Central changes re: credentials conveyance.")
    # qres = pqq.query(datasetsq, db_name=db_name, **kwargs)
    # qres_df = pqh.pull2fields(qres)
    # qres_df = pqh.clean_column_names(qres_df)
    # return qres_df


assay_summary_q = {
    ":find": [["pull", "?a",
               ["*",
                {":assay/technology": [":db/ident"]},
                {":assay/measurement-sets": [":db/id",
                                             ":measurement-set/name",
                                             ":measurement-set/description"]}]]],
    ":in": ["$", "?dataset-name"],
    ":where":
        [["?d", ":dataset/name", "?dataset-name"],
         ["?d", ":dataset/assays", "?a"]]
}


def assay_summary(dataset: str, db_name: str or None = None, **kwargs):
    qres = pqq.query(assay_summary_q, db_name=db_name, args=[dataset])
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    qres_df = pqh.expand_many_nested(qres_df, "assay-measurement-sets")
    return qres_df


clinical_summary_q = {
    ":find": [["pull", "?co", [":clinical-observation-set/name",
                               ":clinical-observation-set/description"]]],
    ":in": ["$", "?dataset-name"],
    ":where":
        [["?d", ":dataset/name", "?dataset-name"],
         ["?d", ":dataset/clinical-observation-sets", "?co"]]
}


def clinical_summary(dataset: str, db_name: str or None = None, **kwargs):
    qres = pqq.query(clinical_summary_q, db_name=db_name, args=[dataset])
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


clinical_observations_q = {
    ":find": [["pull", "?co", ["*",
                               {":clinical-observation/timepoint": [":timepoint/id"]},
                               {":clinical-observation/subject": [":subject/id"]},
                               {":clinical-observation/study-day": [":study-day/id",
                                                                    ":study-day/day"]},
                               {":clinical-observation/bor": [":db/ident"]},
                               {":clinical-observation/dfi-reason": [":db/ident"]},
                               {":clinical-observation/ttf-reason": [":db/ident"]},
                               {":clinical-observation/ir-recist": [":db/ident"]},
                               {":clinical-observation/os-reason": [":db/ident"]},
                               {":clinical-observation/rano": [":db/ident"]},
                               {":clinical-observation/recist": [":db/ident"]},
                               {":clinical-observation/pfs-reason": [":db/ident"]},
                               {":clinical-observation/disease-stage": [":db/ident"]}]]]
}


def clinical_observations(dataset, clinical_observation_set, db_name=None, **kwargs):
    raise NotImplementedError("Clinical data queries will be implemented after next Pattern schema update.")


def clinical_observations_for_patients(dataset, subject_ids,
                                       db_name=None, **kwargs):
    raise NotImplementedError("Clinical data queries will be implemented after next Pattern schema update.")


patient_assays_q = {
    ":find": ["?subject-id", "?sample-id", "?a-tech", "?a-name", "?ms-name"],
    ":in": ["$", "?dataset", ["?subject-id", "..."]],
    ":where": [
        ["?p", ":subject/id", "?subject-id"],
        ["?s", ":sample/subject", "?p"],
        ["?s", ":sample/id", "?sample-id"],
        ["?m", ":measurement/sample", "?s"],
        ["?ms", ":measurement-set/measurements", "?m"],
        ["?a", ":assay/measurement-sets", "?ms"],
        ["?ms", ":measurement-set/name", "?ms-name"],
        ["?a", ":assay/technology", "?at"],
        ["?a", ":assay/name", "?a-name"],
        ["?at", ":db/ident", "?a-tech"]],
}


def patient_assays(dataset: str, patient_ids: List[str],
                   db_name: str or None = None, **kwargs):
    qres = pqq.query(patient_assays_q, db_name=db_name,
                     args=[dataset, patient_ids],
                     **kwargs
                     )
    col_vars = ["subject-id", "sample-id", "assay-tech",
                "assay-name", "measurement-set-name"]
    return pd.DataFrame(qres["query_result"], columns=col_vars)


subjects_q = {
    ":find": [["pull",
               "?s",
               ["*",
                {":subject/sex": [":db/ident"]},
                {":subject/race": [":db/ident"]},
                {":subject/ethnicity": [":db/ident"]},
                {":subject/meddra-disease": [":meddra-disease/preferred-name"]},
                {":subject/disease-stage": [":db/ident"]},
                {":subject/smoker": [":db/ident"]},
                {":subject/cause-of-death": [":db/ident"]},
                {":subject/therapies": [":therapy/order",
                                        {":therapy/treatment-regimen":
                                             [":treatment-regimen/name"]}]}]]],
    ":in": ["$", "?dataset-name"],
    ":where": [
        ["?d", ":dataset/name", "?dataset-name"],
        ["?d", ":dataset/subjects", "?s"]
    ]
}


def subjects(dataset: str, db_name: str or None = None, **kwargs):
    qres = pqq.query(subjects_q, args=[dataset],
                     db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    if "subject-race" in qres_df.columns:
        qres_df = qres_df.explode(column="subject-race")
    return qres_df


measurements_q = {
    ":find": [["pull", "?m",
               ["*",
                {":measurement/variant": [":variant/id"]},
                {":measurement/cnv": [":cnv/id"]},
                {":measurement/sample": [":sample/id"]},
                {":measurement/epitope": [":epitope/id",
                                          {":epitope/protein":
                                               [":protein/preferred-name"]}]},
                {":measurement/gene-product": [":gene-product/id",
                                               {":gene-product/gene":
                                                    [":gene/hgnc-symbol"]}]}]]],
    ":in": ["$", "?dataset-name", "?ms-name"],
    ":where":
        [["?d", ":dataset/name", "?dataset-name"],
         ["?d", ":dataset/assays", "?a"],
         ["?a", ":assay/measurement-sets", "?ms"],
         ["?ms", ":measurement-set/name", "?ms-name"],
         ["?ms", ":measurement-set/measurements", "?m"]]
}


def measurements(dataset: str, measurement_set: str,
                 db_name: str or None = None, **kwargs):
    qres = pqq.query(measurements_q,
                     args=[dataset, measurement_set],
                     db_name=db_name, **kwargs)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


sample_q = measurements_q.copy()
sample_q[":in"].append(["?sample-id", "..."])
sample_q[":where"] = sample_q[":where"][:4] + \
                     [["?d", ":dataset/samples", "?s"],
                      ["?s", ":sample/id", "?sample-id"]] + \
                     sample_q[":where"][4:] + \
                     [["?m", ":measurement/sample", "?s"]]
sample_measurements_q = sample_q


def sample_measurements(dataset: str, measurement_set: str, sample_ids: List[str],
                        db_name: str or None = None, **kwargs):
    qres = pqq.query(sample_measurements_q, args=[dataset, measurement_set, sample_ids],
                     db_name=db_name, **kwargs)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


measurement_matrices_q = {
    ":find": ["?assay-name","?ms-name","?mm-name", "?mm-type-name", "?matrix-key"],
    ":in": ["$", "?dataset-name"],
    ":where": [
        ["?d", ":dataset/name", "?dataset-name"],
        ["?d", ":dataset/assays", "?a"],
        ["?a", ":assay/name", "?assay-name"],
        ["?a", ":assay/measurement-sets", "?ms"],
        ["?ms", ":measurement-set/name", "?ms-name"],
        ["?ms", ":measurement-set/measurement-matrices", "?mm"],
        ["?mm", ":measurement-matrix/name", "?mm-name"],
        ["?mm", ":measurement-matrix/measurement-type", "?mm-type"],
        ["?mm-type", ":db/ident", "?mm-type-name"],
        ["?mm", ":measurement-matrix/backing-file", "?matrix-key"]
    ]
}

def measurement_matrices(dataset: str, db_name: str or None = None, **kwargs):
    qres = pqq.query(measurement_matrices_q, args=[dataset], db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    columns = ["assay-name", "measurement-set-name", "measurement-matrix-name",
               "measurement-matrix-measurement-type", "measurement-matrix-key"]
    return pd.DataFrame(qres["query_result"], columns=columns)


# TBD: query builder that's presto SQL compatible to handle
#      avoiding injection, programmatic patterns, etc, this should
#      go in separate namespace when available.
# def measurements_sql(measurement_set, measurement_attr):
#    return f"""select sample.id, gene.hgnc_symbol, m.{measurement_attr}
#               from measurement m
#               join measurement_set_x_measurements msxm
#                 on msxm.db__id = m.db__id
#               join measurement_set ms
#                 on ms.db__id = msxm.db__id
#               join sample
#                 on m.sample = sample.db__id
#               join gene_product gp
#                on m.gene_product = gp.db__id
#               join gene
#                 on gp.gene = gene.db__id
#               where ms.name = '{measurement_set}'
# """
#
# def sql_measurements(measurement_set, measurement_attr):
#    sql_query = measurements_sql(measurement_set, measurement_attr)
#    rows = trino.query(sql_query)
#    return pd.DataFrame(rows, columns=["sample-id", "hgnc", "rsem"])
#
#
#
# TODO: prove out pattern in another iteration, for now simplify
# def measurement_generator(meas_attr=":measurement/rsem-normalized-count", db_name=None,
#                           chunk_size=5000):
#     """Another approach to iterating through all measurements: using
#     a generator with the datoms API. Ideally we would read ahead, re-use session,
#     and load and iterate concurrently, but this is Python so that's not
#     as trivial as e.g. a buffered channel and concurrency in Clojure."""
#     offset = 0
#     api_resp = pqq.datoms(":aevt", [meas_attr], db_name=db_name,
#                           offset=offset, limit=chunk_size)
#     chunk = api_resp["datoms_chunk"]
#     # -- todo: moving a pull pattern option to the client side for
#     #.   the datoms API would save us a ton of time, so we don't have
#     #.   to rehydrate (1) in Python, and (2) via client/server call
#     while len(chunk) >= 1:
#         meas_eids = [elem[":e"] for elem in chunk]
#         meas_res = pqq.query({
#             ":find": ["?m", "?hgnc", "?samp-id"],
#             ":in": ["$", ["?m", "..."]],
#             ":where":
#                 [["?m", ":measurement/gene-product", "?gp"],
#                  ["?gp", ":gene-product/gene", "?g"],
#                  ["?g", ":gene/hgnc-symbol", "?hgnc"],
#                  ["?m", ":measurement/sample", "?s"],
#                  ["?s", ":sample/id", "?samp-id"]]
#         },
#             args=[meas_eids],
#             db_name=db_name
#         )
#         lookup = dict([(meas[0], (meas[1], meas[2])) for meas in meas_res["query_result"]])
#         for datom in chunk:
#             eid = datom[":e"]
#             match = lookup[eid]
#             yield eid, match[0], match[1], datom[":v"]
#         offset += chunk_size
#         api_resp = pqq.datoms(":aevt", [meas_attr], db_name=db_name,
#                               offset=offset, limit=chunk_size)
#         chunk = api_resp["datoms_chunk"]
#     raise StopIteration
#
