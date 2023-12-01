import json
import os
from subprocess import CompletedProcess

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from datamigration_utils import hive_bq_load_utils

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
            "unique_id": "run-16-Mar",
            "rerun_flag": "N",
            "hive_db_name": "hive_db1",
            "dvt_check_flag": "N",
            "temp_bucket": "dmt-temp-<project-id>",
            "bq_dataset": "bq_dataset1",
            "hive_gcs_staging_bucket_id": "dmt-teradata-data-<project-id>",
            "hive_gcs_staging_path": "hive_data/hive_db1.db",
            "project_id": "<project-id>",
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
    result = hive_bq_load_utils.read_config_file(translation_config)
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
        assert hive_bq_load_utils.read_config_file(translation_config) is None


input_expected_output_mapping = [(str(config_with_hive_gcs_staging_path), None)]


@pytest.mark.parametrize(
    " config, expected_results",
    input_expected_output_mapping,
)
def test_get_hive_tables(
    mocker,
    config,
    expected_results,
):

    data = xr.DataArray(
        np.random.randn(3, 3), coords={"table": ["a", "b", "c"]}, dims=("y", "table")
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "google.cloud.bigquery.Client.query.to_dataframe.merge",
        return_value=mock_results,
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.write_pd_to_gcs", return_value=None
    )

    result = hive_bq_load_utils.get_hive_tables(config)
    assert result == expected_results


input_expected_output_mapping = [
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"], ["c", "e", "c.e"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        [["b"], ["d"], ["e"]],
    ),
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        [["b"], ["d"]],
    ),
    (
        pd.DataFrame(columns=["bq_dataset", "table", "concat_db_tbl"]),
        config_with_hive_gcs_staging_path,
        [],
    ),
]


@pytest.mark.parametrize(
    " df, config,  expected_results",
    input_expected_output_mapping,
)
def test_get_partition_clustering_info(mocker, df, config, expected_results):

    print(df)
    data = xr.DataArray(
        np.random.randn(1, 3),
        coords={"bq_dataset": ["table1", "table2", "table3"]},
        dims=("y", "bq_dataset"),
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.read_pd_from_gcs", return_value=df
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils.get_partition_clustering_info(str(config))
    assert result == expected_results


input_expected_output_mapping = [
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"], ["c", "e", "c.e"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        None,
    ),
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        None,
    ),
    (
        pd.DataFrame(columns=["bq_dataset", "table", "concat_db_tbl"]),
        config_with_hive_gcs_staging_path,
        None,
    ),
]


@pytest.mark.parametrize(
    " df, config,  expected_results",
    input_expected_output_mapping,
)
def test_get_text_format_schema(mocker, df, config, expected_results):

    data = xr.DataArray(
        np.random.randn(1, 3),
        coords={"bq_dataset": ["s", "b", "c"]},
        dims=("y", "bq_dataset"),
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.read_pd_from_gcs", return_value=df
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils.get_text_format_schema(str(config))
    assert result == expected_results


input_expected_output_mapping = [
    (
        "tbl1",
        "gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="8            gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv\n",
            stderr="",
        ),
        True,
    ),
    (
        "tbl1",
        "gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="100000000000000            gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv\n",
            stderr="stderr1 stderr2 bq_job_id",
        ),
        False,
    ),
    (
        "tbl1",
        "gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="",
            stderr="",
        ),
        False,
    ),
]


@pytest.mark.parametrize(
    "tbl, table_gs_path, subprocess_run_result,  expected_results",
    input_expected_output_mapping,
)
def test_get_table_size(
    mocker, tbl, table_gs_path, subprocess_run_result, expected_results
):
    mocker.patch(
        "subprocess.run",
        return_value=subprocess_run_result,
    )
    result = hive_bq_load_utils.get_table_size(tbl, table_gs_path)
    assert result == expected_results


input_expected_output_mapping = [
    (
        "partition_column1, partition_column2",
        "clustering_column1",
        "CSV",
        " --hive_partitioning_mode=AUTO  --clustering_fields='clustering_column1' ",
    ),
    (
        "partition_column1, partition_column2",
        "clustering_column1",
        None,
        " --hive_partitioning_mode=AUTO --time_partitioning_field=partition_column1, partition_column2 --clustering_fields='clustering_column1' ",
    ),
    (None, "clustering_column1", None, " --clustering_fields='clustering_column1' "),
    (
        "partition_column1, partition_column2",
        None,
        None,
        " --hive_partitioning_mode=AUTO --time_partitioning_field=partition_column1, partition_column2 ",
    ),
    (
        "partition_column1, partition_column2",
        None,
        "CSV",
        " --hive_partitioning_mode=AUTO  ",
    ),
    (None, None, None, ""),
]


@pytest.mark.parametrize(
    " partition_columns, clustering_columns,file_format,  expected_results",
    input_expected_output_mapping,
)
def test_partition_cluster_col_subcmd_1(
    partition_columns, clustering_columns, file_format, expected_results
):
    result = hive_bq_load_utils.partition_cluster_col_subcmd_1(
        partition_columns, clustering_columns, file_format
    )
    assert result == expected_results


input_expected_output_mapping = [
    (
        "CSV",
        "Y",
        "field_delimiter_value",
        "e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table_name", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, "schema_string"),
    ),
    (
        "CSV",
        "N",
        "field_delimiter_value",
        "e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table_name", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, ""),
    ),
    (
        "CSV",
        None,
        "field_delimiter_value",
        "e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table_name", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, ""),
    ),
    (
        "PARQUET",
        "Y",
        None,
        "e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table_name", "concat_db_tbl", "schema_string"],
        ),
        (" --parquet_enable_list_inference=true ", 1, 1, ""),
    ),
    (
        "AVRO",
        "Y",
        None,
        "e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table_name", "concat_db_tbl", "schema_string"],
        ),
        (" --use_avro_logical_types=true ", 1, 0, ""),
    ),
    ("AVRO", None, None, None, None, (" --use_avro_logical_types=true ", 1, 0, "")),
    ("ORC", None, None, None, None, ("", 1, 0, "")),
    (None, None, None, None, None, ("", 0, 0, "")),
]


@pytest.mark.parametrize(
    "file_format, partition_flag, field_delimiter, concat_db_tbl, df_text_format_schema,  expected_results",
    input_expected_output_mapping,
)
def test_file_format_subcmd_2(
    file_format,
    partition_flag,
    field_delimiter,
    concat_db_tbl,
    df_text_format_schema,
    expected_results,
):
    result = hive_bq_load_utils.file_format_subcmd_2(
        file_format,
        partition_flag,
        field_delimiter,
        concat_db_tbl,
        df_text_format_schema,
    )
    assert result == expected_results


input_expected_output_mapping = [
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="8            gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv\n",
            stderr="",
        ),
        ("PASS", "NA", "NA"),
    ),
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="8            gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv\n",
            stderr="stderr1 stderr2 bq_job_id",
        ),
        ("PASS", "NA", "bq_job_id"),
    ),
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=0,
            stdout="stdoutput1:stdoutput2:bq_job_id",
            stderr="",
        ),
        ("PASS", "NA", "bq_job_id"),
    ),
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=400,
            stdout="stdoutput1:reason_for_failure",
            stderr="",
        ),
        ("FAIL", "stdoutput1:reason_for_failure", "NA"),
    ),
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=400,
            stdout="stdoutput1:reason_for_failure:bq_job_id",
            stderr="",
        ),
        ("FAIL", "stdoutput1:reason_for_failure:bq_job_id", "bq_job_id"),
    ),
    (
        "tbl1",
        CompletedProcess(
            args="gsutil du -s  gs://dmt-teradata-data-oracle-dev-testing/files/teradata/teradata_data_mig_table_list_integration_testing_cloudrun.csv",
            returncode=400,
            stdout="stdoutput1:reason_for_failure:bq_job_id",
            stderr="stderr1 stderr2 bq_job_id",
        ),
        ("FAIL", "stdoutput1:reason_for_failure:bq_job_id", "bq_job_id"),
    ),
]


@pytest.mark.parametrize(
    "tbl, result,  expected_results",
    input_expected_output_mapping,
)
def test_get_job_status(tbl, result, expected_results):
    result = hive_bq_load_utils.get_job_status(tbl, result)
    assert result == expected_results


input_expected_output_mapping = [
    (
        pd.DataFrame(
            [
                ["bq_tbl1", "Y", "PARQUET", "database1", None],
                ["bq_tbl2", "N", "CSV", "database2", None],
                ["bq_tbl3", "Y", "AVRO", "database2", None],
            ],
            columns=[
                "table",
                "partition_flag",
                "format",
                "database",
                "field_delimiter",
            ],
        ),
        pd.DataFrame(
            [
                ["bq_tbl1", "tbl1_partition", "tbl1_clustering"],
                ["bq_tbl2", "tbl2_partition"],
                ["bq_tbl3", None, "tbl3_clustering"],
            ],
            columns=["table_name", "partition_column", "clustering_column"],
        ),
        pd.DataFrame([["a"], ["c"], ["c"]], columns=["text_format_schema"]),
        pd.DataFrame(
            [
                [
                    "incremental_table1",
                    "destination_path1/incremental_table1/",
                ],
                [
                    "incremental_table2",
                    "destination_path2/incremental_table2/",
                ],
                [
                    "incremental_table3",
                    "destination_path3/incremental_table3/",
                ],
                ["bq_tbl1", "destination_path4/bq_tbl1/"],
            ],
            columns=["table", "destination_path"],
        ),
        True,
        None,
    ),
    (
        pd.DataFrame(
            [
                ["bq_tbl1", "Y", "PARQUET", "database1", None],
                ["bq_tbl2", "N", "CSV", "database2", None],
                ["bq_tbl3", "Y", "AVRO", "database2", None],
            ],
            columns=[
                "table",
                "partition_flag",
                "format",
                "database",
                "field_delimiter",
            ],
        ),
        pd.DataFrame(
            [
                ["bq_tbl1", "tbl1_partition", "tbl1_clustering"],
                ["bq_tbl2", "tbl2_partition"],
                ["bq_tbl3", None, "tbl3_clustering"],
            ],
            columns=["table_name", "partition_column", "clustering_column"],
        ),
        pd.DataFrame([["a"], ["c"], ["c"]], columns=["text_format_schema"]),
        pd.DataFrame(
            [
                [
                    "incremental_table1",
                    "destination_path1/incremental_table1/",
                ],
                [
                    "incremental_table2",
                    "destination_path2/incremental_table2/",
                ],
                [
                    "incremental_table3",
                    "destination_path3/incremental_table3/",
                ],
                ["bq_tbl1", "destination_path4/bq_tbl1/"],
            ],
            columns=["table", "destination_path"],
        ),
        False,
        None,
    ),
    (
        pd.DataFrame(
            [
                ["bq_tbl1", "N", "NOT_SUPPORTED_FORMAT", "database1", None],
                ["bq_tbl2", "N", "CSV", "database2", None],
                ["bq_tbl3", "Y", "AVRO", "database2", None],
            ],
            columns=[
                "table",
                "partition_flag",
                "format",
                "database",
                "field_delimiter",
            ],
        ),
        pd.DataFrame(
            [
                ["bq_tbl1", "tbl1_partition", "tbl1_clustering"],
                ["bq_tbl2", "tbl2_partition"],
                ["bq_tbl3", None, "tbl3_clustering"],
            ],
            columns=["table_name", "partition_column", "clustering_column"],
        ),
        pd.DataFrame([["a"], ["c"], ["c"]], columns=["text_format_schema"]),
        pd.DataFrame(
            [
                [
                    "incremental_table1",
                    "destination_path1/incremental_table1/",
                ],
                [
                    "incremental_table2",
                    "destination_path2/incremental_table2/",
                ],
                [
                    "incremental_table3",
                    "destination_path3/incremental_table3/",
                ],
                ["bq_tbl1", "destination_path4/bq_tbl1/"],
            ],
            columns=["table", "destination_path"],
        ),
        True,
        None,
    ),
]


@pytest.mark.parametrize(
    "df_hive_tbls, df_partition_clustering, df_text_format_schema, df_incremental_table_list,table_size_status_value, expected_results",
    input_expected_output_mapping,
)
def test_load_bq_tables(
    mocker,
    df_hive_tbls,
    df_partition_clustering,
    df_text_format_schema,
    df_incremental_table_list,
    table_size_status_value,
    expected_results,
):
    # df_hive_tbls=pd.DataFrame([['bq_dataset.bq_tbl1','Y','PARQUET','database1',None], ['bq_dataset.bq_tbl2','N','CSV','database2',None], ['bq_dataset.bq_tbl3','Y','AVRO','database2',None]], columns=['concat_db_tbl','partition_flag','format','database','field_delimiter'])
    # df_partition_clustering = pd.DataFrame([['bq_dataset.bq_tbl1','tbl1_partition', 'tbl1_clustering'], ['bq_dataset.bq_tbl2', 'tbl2_partition'], ['bq_dataset.bq_tbl3',None,'tbl3_clustering']], columns=['concat_db_tbl', 'partition_column','clustering_column' ])
    # df_text_format_schema=pd.DataFrame([['a'], ['c'], ['c']], columns=['text_format_schema'])
    # df_incremental_table_list=pd.DataFrame([['bq_dataset.incremental_table1','destination_path1/incremental_table1/'], ['bq_dataset.incremental_table2','destination_path2/incremental_table2/'], ['bq_dataset.incremental_table3','destination_path3/incremental_table3/'],['bq_dataset.bq_tbl1','destination_path4/bq_tbl1/']], columns=['concat_db_tbl', 'destination_path'])

    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.read_pd_from_gcs",
        side_effect=[
            df_hive_tbls,
            df_partition_clustering,
            df_text_format_schema,
            df_incremental_table_list,
        ],
    )
    mocker.patch(
        "subprocess.run",
        return_value=CompletedProcess(
            args="bq load command", returncode=0, stdout="stdoutput", stderr=""
        ),
    )

    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.save_load_status_bq",
        return_value="Success",
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils.get_table_size",
        return_value=table_size_status_value,
    )

    result = hive_bq_load_utils.load_bq_tables(
        "bq_tbl1", str(config_with_hive_gcs_staging_path), "op_load_dtm"
    )
    assert result == expected_results
