import json
import os

import pytest
from google.cloud.exceptions import NotFound

from common_utils import table_filter

MOCK_CONFIGS_DIR_NAME = "mock"
f = open(
    os.path.join(
        os.path.dirname(__file__),
        MOCK_CONFIGS_DIR_NAME,
        "sampleconfig_for_ddl_sql_teradata.json",
    )
)
f2 = open(
    os.path.join(
        os.path.dirname(__file__),
        MOCK_CONFIGS_DIR_NAME,
        "sample_config_for_sql.json",
    ),
    "r",
)

# Define input and expected outputs that covers:
#     1: File Status: Success
#     2: File Status: Fail with table already exists
#     3: File Status: Fail with NotFound
files_mapping = [
    (
        [
            {
                "unique_id": "unique_id_to_filter_bq_result_table",
                "sql_file_name": "sample_sql1",
                "job_id": "sample_job_id1",
                "status": "success",
                "error_details": "",
                "execution_start_time": "20230310",
                "gcs_source_path": "gs://edw-ams-translation-dmlcustomer/input/dml",
            }
        ],
        "success",
        ["DMT_DATASET.EMPLOYEE1=DMT_DATASET.EMPLOYEE1_TARGET"],
    ),
    (
        [
            {
                "unique_id": "unique_id_to_filter_bq_result_table_failed",
                "sql_file_name": "sample_sql1",
                "job_id": "sample_job_id2",
                "status": "fail",
                "error_details": "",
                "execution_start_time": "20230310",
                "gcs_source_path": "gs://edw-ams-translation-dmlcustomer/input/dml",
            }
        ],
        "success",
        ["DMT_DATASET.EMPLOYEE1=DMT_DATASET.EMPLOYEE1_TARGET"],
    ),
    (
        [
            {
                "unique_id": "unique_id_to_filter_bq_result_table_failed2",
                "sql_file_name": "sample_sql1",
                "job_id": "sample_job_id3",
                "status": "fail",
                "error_details": "",
                "execution_start_time": "20230310",
                "gcs_source_path": "gs://edw-ams-translation-dmlcustomer/input/dml",
            }
        ],
        NotFound("File Not Found"),
        [],
    ),
]
config = json.load(f)


@pytest.mark.parametrize(
    "files, get_client_side_effect,  expected_results", files_mapping
)
def test_filter(mocker, files, get_client_side_effect, expected_results):
    # Mocker Patch calls
    path = os.path.join(os.path.dirname(__file__), "mock/sql/", "sample_sql1.sql")
    mocker.patch("os.path.join", return_value=path)
    mocker.patch(
        "google.cloud.bigquery.Client.insert_rows_json", return_value="success"
    )
    mocker.patch(
        "google.cloud.bigquery.Client.get_table", side_effect=get_client_side_effect
    )
    mocker.patch("common_utils.storage_utils.storage")
    mocker.patch(
        "common_utils.storage_utils.StorageUtils.get_validation_params_from_gcs",
        return_value={
            "DMT_DATASET.EMPLOYEE1": {"target-table": "DMT_DATASET.EMPLOYEE1_TARGET"}
        },
    )
    mocker.patch(
        "common_utils.storage_utils.StorageUtils.parse_bucket_and_blob_from_path",
        return_value=["demo-bucket-1", "output/ddl/sample-folder-1"],
    )

    results = table_filter.filter(files, config)
    assert results == expected_results


def test_filter_sql_config():
    files = [
        {
            "unique_id": "unique_id_to_filter_bq_result_table",
            "sql_file_name": "sample_sql1",
            "job_id": "sample_job_id1",
            "status": "success",
            "error_details": "",
            "execution_start_time": "20230310",
            "gcs_source_path": "gs://edw-ams-translation-dmlcustomer/input/dml",
        }
    ]

    results = table_filter.filter(files, json.load(f2))
    expected_result = []
    assert results == expected_result


input_expected_output_mapping = [
    (
        [
            "testdb.EMPLOYEE=dvt.EMPLOYEE",
            "testdb.DEPARTMENT=dvt.DEPARTMENT",
            "testdb.EMPLOYEE5=dvt.EMPLOYEE5",
        ],
        "EMPLOYEE;DEPARTMENT;EMPLOYEE5",
        "testdb",
        [
            "testdb.EMPLOYEE=dvt.EMPLOYEE",
            "testdb.DEPARTMENT=dvt.DEPARTMENT",
            "testdb.EMPLOYEE5=dvt.EMPLOYEE5",
        ],
    ),
    ([], "EMPLOYEE;DEPARTMENT;EMPLOYEE5", "testdb", []),
    ([], "", "testdb", []),
    ([], None, "testdb", []),
    ([], None, "", []),
    ([], None, None, []),
    (
        [
            "testdb.EMPLOYEE=dvt1.EMPLOYEE",
            "testdb.DEPARTMENT=dvt.DEPARTMENT",
            "testdb.EMPLOYEE5=dvt.EMPLOYEE5",
        ],
        "EMPLOYEE1;DEPARTMENT;EMPLOYEE5",
        "testdb",
        ["testdb.DEPARTMENT=dvt.DEPARTMENT", "testdb.EMPLOYEE5=dvt.EMPLOYEE5"],
    ),
]


@pytest.mark.parametrize(
    "config_table_mapping, tables, schema, expected_results",
    input_expected_output_mapping,
)
def test_filter_valid_table_mappings(
    config_table_mapping, tables, schema, expected_results
):
    config_table_mapping = [
        "testdb.EMPLOYEE=dvt.EMPLOYEE",
        "testdb.DEPARTMENT=dvt.DEPARTMENT",
        "testdb.EMPLOYEE5=dvt.EMPLOYEE5",
    ]
    tables = "EMPLOYEE;DEPARTMENT;EMPLOYEE5"
    schema = "testdb"
    result = table_filter.filter_valid_table_mappings(
        config_table_mapping, tables, schema
    )
    expected_results = [
        "testdb.EMPLOYEE=dvt.EMPLOYEE",
        "testdb.DEPARTMENT=dvt.DEPARTMENT",
        "testdb.EMPLOYEE5=dvt.EMPLOYEE5",
    ]

    assert result == expected_results
