import json
import os

import numpy as np
import pytest
import xarray as xr
from datamigration_utils import hive_bq_load_utils_inc

MOCK_CONFIGS_DIR_NAME = "mock"
f = open(
    os.path.join(
        os.path.dirname(__file__),
        MOCK_CONFIGS_DIR_NAME,
        "sampleconfig_data_gke_column_hive.json",
    )
)
f2 = open(
    os.path.join(
        os.path.dirname(__file__),
        MOCK_CONFIGS_DIR_NAME,
        "sampleconfig_data_gke_column_hive2.json",
    )
)

input_expected_output_mapping = [
    ("edw_logs", "hive_pubsub_audit", "2023-05-17 22:30:00.000+00:00", 2, 6),
    ("edw_logs", "hive_pubsub_audit", "2023-05-17 22:30:00.000+00:00", 0, 0),
]


@pytest.mark.parametrize(
    " bq_audit_dataset_id, bq_pubsub_audit_tbl, schedule_runtime, row_count ,expected_len",
    input_expected_output_mapping,
)
def test_get_inc_gcs_files(
    mocker,
    bq_audit_dataset_id,
    bq_pubsub_audit_tbl,
    schedule_runtime,
    row_count,
    expected_len,
):

    data = xr.DataArray(
        np.random.randn(row_count, 3), coords={"x": ["a", "b", "c"]}, dims=("y", "x")
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    results = hive_bq_load_utils_inc.get_inc_gcs_files(
        bq_audit_dataset_id, bq_pubsub_audit_tbl, schedule_runtime
    )
    assert len(results) == expected_len


config_without_hive_gcs_staging_path = json.load(f)
config_with_hive_gcs_staging_path = json.load(f2)

input_expected_output_mapping = [
    (
        str(config_without_hive_gcs_staging_path),
        [
            "databasename.bq_dataset1/tablename/*",
            "databasename.bq_dataset1/tablename1/*",
            "databasename2.bq_dataset3/tablename/*",
        ],
        [
            ["databasename,bq_dataset1.tablename,gcs_path1"],
            ["databasename,bq_dataset1.tablename1,gcs_path2"],
            ["databasename,bq_dataset3.tblnam3,gcs_path3"],
        ],
    ),
    (str(config_without_hive_gcs_staging_path), [], []),
    (
        str(config_with_hive_gcs_staging_path),
        [
            "hive_data/hive_db1.db/databasename.bq_dataset1/tablename",
            "hive_data/hive_db1.db/databasename.bq_dataset1/tablename1",
            "hive_data/hive_db1.db/databasename2.bq_dataset3/tablename",
        ],
        [[]],
    ),
    (
        str(config_with_hive_gcs_staging_path),
        [
            "hive_data/hive_db1.db/databasename.bq_dataset1/tablename/*",
            "hive_data/hive_db1.db/databasename.bq_dataset1/tablename1/*",
            "hive_data/hive_db1.db/databasename2.bq_dataset3/tablename/*",
        ],
        [
            ["databasename,bq_dataset1.tablename,gcs_path1"],
            ["databasename,bq_dataset1.tablename1,gcs_path2"],
            ["databasename,bq_dataset3.tblnam3,gcs_path3"],
        ],
    ),
    (
        str(config_with_hive_gcs_staging_path),
        [
            "databasename.bq_dataset1/tablename/*",
            "databasename.bq_dataset1/tablename1/*",
            "databasename2.bq_dataset3/tablename/*",
        ],
        [
            ["databasename,bq_dataset1.tablename,gcs_path1"],
            ["databasename,bq_dataset1.tablename1,gcs_path2"],
            ["databasename,bq_dataset3.tblnam3,gcs_path3"],
        ],
    ),
    (str(config_with_hive_gcs_staging_path), [], []),
]


@pytest.mark.parametrize(
    " config, inc_gcs_files_list, expected_output", input_expected_output_mapping
)
def test_get_inc_table_list_for_copy(
    mocker,
    config,
    inc_gcs_files_list,
    expected_output
    # mock_storage, translation_type, validation_type, source_table
):
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.check_bq_table",
        side_effect=[
            str(
                "databasename"
                + ","
                + "bq_dataset1"
                + "."
                + "tablename"
                + ","
                + "gcs_path1"
            ),
            str(
                "databasename"
                + ","
                + "bq_dataset1"
                + "."
                + "tablename1"
                + ","
                + "gcs_path2"
            ),
            str(
                "databasename"
                + ","
                + "bq_dataset3"
                + "."
                + "tblnam3"
                + ","
                + "gcs_path3"
            ),
        ],
    )
    result = hive_bq_load_utils_inc.get_inc_table_list_for_copy(
        config, inc_gcs_files_list
    )

    assert result == expected_output
