import json
import os

import numpy as np
import pytest
import xarray as xr
from datamigration_utils import hive_dvt_check_utils

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
f3 = open(
    os.path.join(
        os.path.dirname(__file__),
        MOCK_CONFIGS_DIR_NAME,
        "empty_config_data_gke_column_hive.json",
    )
)

config_without_hive_gcs_staging_path = json.load(f)
config_with_hive_gcs_staging_path = json.load(f2)
empty_config = json.load(f3)


input_expected_output_mapping = [
    (
        config_with_hive_gcs_staging_path,
        {
            "rerun_flag": "N",
            "hive_db_name": "hive_db1",
            "dvt_check_flag": "N",
            "temp_bucket": "dmt-temp-<project-id>",
            "bq_dataset": "bq_dataset1",
            "hive_gcs_staging_bucket_id": "dmt-teradata-data-<project-id>",
            "hive_gcs_staging_path": "hive_data/hive_db1.db",
            "project_id": "<project-id>",
            "validation_mode": "gke",
            "batch_distribution": 4,
            "hive_ddl_metadata": "hive_ddl_metadata",
            "bq_dataset_audit": "dmt_logs",
            "bq_load_audit": "hive_bqload_audit",
            "dvt_results": "dmt_dvt_results",
            "schema_results_tbl": "dmt_schema_results",
        },
    )
]


@pytest.mark.parametrize(
    " translation_config, expected_results",
    input_expected_output_mapping,
)
def test_read_config_file(
    translation_config,
    expected_results,
):
    result = hive_dvt_check_utils.read_config_file(translation_config)
    assert result == expected_results


input_expected_output_mapping = [(config_without_hive_gcs_staging_path), (empty_config)]


@pytest.mark.parametrize(
    " translation_config",
    input_expected_output_mapping,
)
def test_read_config_file_EC1(
    translation_config,
):
    with pytest.raises(KeyError):
        assert hive_dvt_check_utils.read_config_file(translation_config) is None


def test_get_dvt_table_list(mocker):
    data = xr.DataArray(np.random.randn(1, 1), coords={"x": ["a"]}, dims=("y", "x"))
    mock_results = xr.Dataset({"tablename": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    result = hive_dvt_check_utils.get_dvt_table_list(
        hive_dvt_check_utils.read_config_file(config_with_hive_gcs_staging_path)
    )
    assert type(result) is list


input_expected_output_mapping = [
    ("gke", "validation_dag"),
    ("cloudrun", "validation_crun_dag"),
    (None, "validation_dag"),
    ("Not_Supported", "validation_dag"),
]


@pytest.mark.parametrize(
    "validation_mode, expected_result",
    input_expected_output_mapping,
)
def test_get_validation_dag_id(validation_mode, expected_result):
    result = hive_dvt_check_utils.get_validation_dag_id(validation_mode)
    assert result == expected_result
