import copy
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
    qres = pqq.query(datasetsq, db_name=db_name, **kwargs)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


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

clinical_query = {
    ":find": [[
        "pull", "?ce", ["*",
            {":clinical-observation/timepoint": [
                ":timepoint/id",
                ":timepoint/name",
                ":timepoint/relative-order"
            ]},
            {":clinical-intervention/anti-cancer-vaccine": [":db/ident"]},
            {":clinical-observation/imaging": [":db/ident"]},
            {":clinical-intervention/cancer-medication-category": [":db/ident"]},
            {":clinical-intervention/surgery-type": [":db/ident"]},
            {":clinical-intervention/treatment-regimen": [
                "*",
                {":clinical-intervention/drug-regimens": ["*"]}
            ]},
            {":clinical-intervention/biospecimen-collection": [":db/ident"]},
            {":clinical-intervention/biospecimen-collection-site": [":db/ident"]},
            {":clinical-intervention/biospecimen-type": [":db/ident"]},
            {":clinical-intervention/biospecimen-derived-samples": [":sample/id"]},
            {":clinical-intervention/timepoint": [
                ":timepoint/id",
                ":timepoint/name",
                ":timepoint/relative-order"
            ]}
        ]
    ]],
    ":in": ["$", "?dataset", ["?patient-id", "..."]],
    ":where": [
        ["?d", ":dataset/name", "?dataset"],
        ["?d", ":dataset/subjects", "?p"],
        ["?p", ":subject/id", "?patient-id"],
        ["or-join", ["?ce"],
            ["?ce", ":clinical-intervention/subject", "?p"],
            ["?ce", ":clinical-observation/subject", "?p"]
        ]
    ]
}

def clinical_events_for_patients(dataset: str, subject_ids: List[str],
                                   db_name=str or None, **kwargs):
    qres = pqq.query(clinical_query, db_name=db_name, args=[dataset, subject_ids])
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


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


sample_q = copy.deepcopy(measurements_q)
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
    ":find": ["?assay-name", "?ms-name", "?mm-name", "?mm-type-name", "?matrix-key"],
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

variant_measurements_q = {
    ":find": [
        ["pull", "?m", [":measurement/vaf", {":measurement/sample": [":sample/id"],
                                             ":measurement/variant": [":variant/id"]}]]
    ],
    ":in": [
        "$",
        "?ms-name"
    ],
    ":where": [
        ["?ms", ":measurement-set/name", "?ms-name"],
        ["?ms", ":measurement-set/measurements", "?m"],
        ["?m", ":measurement/variant", "?v"],
        ["?v", ":variant/id", "?var-id"]
    ]
}

def variant_measurements(measurement_set: str, db_name: str or None = None, **kwargs):
    qres = pqq.query(variant_measurements_q, args=[measurement_set], db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    qres_df = pqh.clean_column_names(qres_df)
    return qres_df


var2measq = {
    ":find": [
        ["pull", "?m", [":measurement/vaf", {":measurement/sample": [":sample/id"],
                                             ":measurement/variant": [":variant/id"],
                                             ":measurement-set/_measurements": [":measurement-set/name"]}]]
    ],
    ":in": ["$", ["?variant-id", "..."]],
    ":where":[
        ["?v", ":variant/id", "?variant-id"],
        ["?m", ":measurement/variant", "?v"]
    ]
}

def measurements_of_variants(variant_ids: List[str], db_name: str or None = None, **kwargs):
    qres = pqq.query(var2measq, args=[variant_ids], db_name=db_name, **kwargs)
    qres = pqh.flatten_enum_idents(qres)
    qres_df = pqh.pull2fields(qres)
    return qres_df
