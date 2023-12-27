import json
import os
from subprocess import CompletedProcess

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from datamigration_utils import hive_bq_load_utils_inc
from google.cloud.exceptions import NotFound

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


input_expected_output_mapping = [
    (
        "gcs_path",
        "dbname",
        "tblname",
        {
            "bq_dataset_audit": "bq_dataset_audit_value",
            "hive_ddl_metadata": "hive_ddl_metadata_value",
        },
        "success",
        "dbname,bq_dataset.tblname,gcs_path",
    ),
    (
        "gcs_path",
        "dbname",
        "tblname",
        {
            "bq_dataset_audit": "bq_dataset_audit_value",
            "hive_ddl_metadata": "hive_ddl_metadata_value",
        },
        NotFound("Table Not Found"),
        "",
    ),
    (
        "gcs_path",
        "dbname",
        "tblname",
        {
            "bq_dataset_audit": "bq_dataset_audit_value",
            "hive_ddl_metadata": "hive_ddl_metadata_value",
        },
        ValueError("Exception"),
        None,
    ),
    (
        None,
        "dbname",
        "tblname",
        {
            "bq_dataset_audit": "bq_dataset_audit_value",
            "hive_ddl_metadata": "hive_ddl_metadata_value",
        },
        "success",
        None,
    ),
    (
        "",
        "dbname",
        "tblname",
        {
            "bq_dataset_audit": "bq_dataset_audit_value",
            "hive_ddl_metadata": "hive_ddl_metadata_value",
        },
        "success",
        "dbname,bq_dataset.tblname,",
    ),
]


@pytest.mark.parametrize(
    " gcs_path, dbname, tblname, dict ,side_effect_value, expected_results",
    input_expected_output_mapping,
)
def test_check_bq_table(
    mocker, gcs_path, dbname, tblname, dict, side_effect_value, expected_results
):

    d = {
        "t": {"dims": ("query_results"), "data": ["bq_dataset"]},
    }
    mock_results = xr.Dataset.from_dict(d)
    print(mock_results)
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "google.cloud.bigquery.Client.get_table", side_effect=side_effect_value
    )
    result = hive_bq_load_utils_inc.check_bq_table(gcs_path, dbname, tblname, dict)
    assert result == expected_results


d = {
    "t": {"dims": ("query_results"), "data": ["bq_dataset"]},
}
mock_results = xr.Dataset.from_dict(d)

input_expected_output_mapping = [
    (
        "dbname,bq_dataset.tblname,gcs_path",
        config_with_hive_gcs_staging_path,
        "2023-05-17 22:30:00.000+00:00",
        mock_results,
        "success",
        None,
    ),
    (
        "dbname,bq_dataset.tblname,gcs_path",
        config_without_hive_gcs_staging_path,
        "2023-05-17 22:30:00.000+00:00",
        mock_results,
        "success",
        None,
    ),
    (
        "dbname,bq_dataset.tblname,gcs_path",
        config_without_hive_gcs_staging_path,
        "2023-05-17 22:30:00.000+00:00",
        mock_results,
        ValueError("Exception"),
        None,
    ),
    (
        "dbname,bq_dataset.tblname,gcs_path",
        config_with_hive_gcs_staging_path,
        None,
        mock_results,
        "success",
        None,
    ),
    (
        "dbname,bq_dataset.tblname,gcs_path",
        config_with_hive_gcs_staging_path,
        "",
        mock_results,
        "success",
        None,
    ),
]


@pytest.mark.parametrize(
    " tbl_data, config_data, job_run_time ,side_effect_query_results,side_effect_copy_blob, expected_results",
    input_expected_output_mapping,
)
def test_copy_inc_files(
    mocker,
    tbl_data,
    config_data,
    job_run_time,
    side_effect_query_results,
    side_effect_copy_blob,
    expected_results,
):
    print("side_effect_query_results")
    print(side_effect_query_results.to_dataframe().values[0][0])
    mocker.patch(
        "google.cloud.bigquery.Client.query", return_value=side_effect_query_results
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.copy_blob",
        side_effect=side_effect_copy_blob,
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.save_file_copy_status",
        side_effect="success",
    )
    result = hive_bq_load_utils_inc.copy_inc_files(
        tbl_data, str(config_data), job_run_time
    )
    hive_bq_load_utils_inc.save_file_copy_status.assert_called_once()
    assert expected_results == result


def test_copy_inc_files_EC1_table_data():
    with pytest.raises(IndexError):
        assert (
            hive_bq_load_utils_inc.copy_inc_files(
                "",
                str(config_with_hive_gcs_staging_path),
                "2023-05-17 22:30:00.000+00:00",
            )
            is None
        )


def test_copy_inc_files_EC2_table_data():
    with pytest.raises(TypeError):
        assert (
            hive_bq_load_utils_inc.copy_inc_files(
                None,
                str(config_with_hive_gcs_staging_path),
                "2023-05-17 22:30:00.000+00:00",
            )
            is None
        )


def test_copy_inc_files_EC3_config_data():
    with pytest.raises(ValueError):
        assert (
            hive_bq_load_utils_inc.copy_inc_files(
                "dbname,bq_dataset.tblname,gcs_path",
                None,
                "2023-05-17 22:30:00.000+00:00",
            )
            is None
        )


def test_get_inc_table_list(
    mocker,
):
    data = xr.DataArray(
        np.random.randn(1, 3), coords={"x": ["a", "b", "c"]}, dims=("y", "x")
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils_inc.get_inc_table_list(
        str(config_with_hive_gcs_staging_path)
    )
    assert result is None


def test_get_inc_table_list_EC1_config_data():
    with pytest.raises(KeyError):
        assert hive_bq_load_utils_inc.get_inc_table_list(str(empty_config)) is None


def test_get_table_info_from_metadata(
    mocker,
):
    data = xr.DataArray(
        np.random.randn(1, 3),
        coords={"concat_db_tbl": ["table1", "table2", "table3"]},
        dims=("y", "concat_db_tbl"),
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.read_pd_from_gcs",
        return_value=mock_results,
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils_inc.get_table_info_from_metadata(
        str(config_with_hive_gcs_staging_path)
    )
    assert result is None


def test_get_table_info_from_metadata_EC1_config_data():
    with pytest.raises(KeyError):
        assert (
            hive_bq_load_utils_inc.get_table_info_from_metadata(str(empty_config))
            is None
        )


input_expected_output_mapping = [
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"], ["c", "e", "c.e"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        [["a.b"], ["c.d"], ["c.e"]],
    ),
    (
        pd.DataFrame(
            [["a", "b", "a.b"], ["c", "d", "c.d"]],
            columns=["bq_dataset", "table", "concat_db_tbl"],
        ),
        config_with_hive_gcs_staging_path,
        [["a.b"], ["c.d"]],
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
        "datamigration_utils.hive_bq_load_utils_inc.read_pd_from_gcs", return_value=df
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils_inc.get_partition_clustering_info(str(config))
    assert result == expected_results


def test_get_partition_clustering_info_EC1_config_data():
    with pytest.raises(KeyError):
        assert (
            hive_bq_load_utils_inc.get_partition_clustering_info(str(empty_config))
            is None
        )


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

    print(df)
    data = xr.DataArray(
        np.random.randn(1, 3),
        coords={"bq_dataset": ["s", "b", "c"]},
        dims=("y", "bq_dataset"),
    )
    mock_results = xr.Dataset({"foo": data})
    mocker.patch("google.cloud.bigquery.Client.query", return_value=mock_results)
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.read_pd_from_gcs", return_value=df
    )
    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.write_pd_to_gcs",
        side_effect="success",
    )
    result = hive_bq_load_utils_inc.get_text_format_schema(str(config))
    assert result == expected_results


def test_get_text_format_schema_EC1_config_data():
    with pytest.raises(KeyError):
        assert hive_bq_load_utils_inc.get_text_format_schema(str(empty_config)) is None


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
    result = hive_bq_load_utils_inc.partition_cluster_col_subcmd_1(
        partition_columns, clustering_columns, file_format
    )
    assert result == expected_results


input_expected_output_mapping = [
    (
        "CSV",
        "Y",
        "field_delimiter_value",
        "c.e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, "schema_string"),
    ),
    (
        "CSV",
        "N",
        "field_delimiter_value",
        "c.e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, ""),
    ),
    (
        "CSV",
        None,
        "field_delimiter_value",
        "c.e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table", "concat_db_tbl", "schema_string"],
        ),
        (" --field_delimiter=field_delimiter_value", 1, 0, ""),
    ),
    (
        "PARQUET",
        "Y",
        None,
        "c.e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table", "concat_db_tbl", "schema_string"],
        ),
        (" --parquet_enable_list_inference=true ", 1, 1, ""),
    ),
    (
        "AVRO",
        "Y",
        None,
        "c.e",
        pd.DataFrame(
            [
                ["a", "b", "a.b", "schema_string"],
                ["c", "d", "c.d", "schema_string"],
                ["c", "e", "c.e", "schema_string"],
            ],
            columns=["bq_dataset", "table", "concat_db_tbl", "schema_string"],
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
    result = hive_bq_load_utils_inc.file_format_subcmd_2(
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
    result = hive_bq_load_utils_inc.get_job_status(tbl, result)
    assert result == expected_results


input_expected_output_mapping = [
    (
        pd.DataFrame(
            [
                ["bq_dataset.bq_tbl1", "Y", "PARQUET", "database1", None],
                ["bq_dataset.bq_tbl2", "N", "CSV", "database2", None],
                ["bq_dataset.bq_tbl3", "Y", "AVRO", "database2", None],
            ],
            columns=[
                "concat_db_tbl",
                "partition_flag",
                "format",
                "database",
                "field_delimiter",
            ],
        ),
        pd.DataFrame(
            [
                ["bq_dataset.bq_tbl1", "tbl1_partition", "tbl1_clustering"],
                ["bq_dataset.bq_tbl2", "tbl2_partition"],
                ["bq_dataset.bq_tbl3", None, "tbl3_clustering"],
            ],
            columns=["concat_db_tbl", "partition_column", "clustering_column"],
        ),
        pd.DataFrame([["a"], ["c"], ["c"]], columns=["text_format_schema"]),
        pd.DataFrame(
            [
                [
                    "bq_dataset.incremental_table1",
                    "destination_path1/incremental_table1/",
                ],
                [
                    "bq_dataset.incremental_table2",
                    "destination_path2/incremental_table2/",
                ],
                [
                    "bq_dataset.incremental_table3",
                    "destination_path3/incremental_table3/",
                ],
                ["bq_dataset.bq_tbl1", "destination_path4/bq_tbl1/"],
            ],
            columns=["concat_db_tbl", "destination_path"],
        ),
        None,
    ),
    (
        pd.DataFrame(
            [
                ["bq_dataset.bq_tbl1", "N", "NOT_SUPPORTED_FORMAT", "database1", None],
                ["bq_dataset.bq_tbl2", "N", "CSV", "database2", None],
                ["bq_dataset.bq_tbl3", "Y", "AVRO", "database2", None],
            ],
            columns=[
                "concat_db_tbl",
                "partition_flag",
                "format",
                "database",
                "field_delimiter",
            ],
        ),
        pd.DataFrame(
            [
                ["bq_dataset.bq_tbl1", "tbl1_partition", "tbl1_clustering"],
                ["bq_dataset.bq_tbl2", "tbl2_partition"],
                ["bq_dataset.bq_tbl3", None, "tbl3_clustering"],
            ],
            columns=["concat_db_tbl", "partition_column", "clustering_column"],
        ),
        pd.DataFrame([["a"], ["c"], ["c"]], columns=["text_format_schema"]),
        pd.DataFrame(
            [
                [
                    "bq_dataset.incremental_table1",
                    "destination_path1/incremental_table1/",
                ],
                [
                    "bq_dataset.incremental_table2",
                    "destination_path2/incremental_table2/",
                ],
                [
                    "bq_dataset.incremental_table3",
                    "destination_path3/incremental_table3/",
                ],
                ["bq_dataset.bq_tbl1", "destination_path4/bq_tbl1/"],
            ],
            columns=["concat_db_tbl", "destination_path"],
        ),
        None,
    ),
]


@pytest.mark.parametrize(
    "df_hive_tbls, df_partition_clustering, df_text_format_schema, df_incremental_table_list, expected_results",
    input_expected_output_mapping,
)
def test_load_bq_tables(
    mocker,
    df_hive_tbls,
    df_partition_clustering,
    df_text_format_schema,
    df_incremental_table_list,
    expected_results,
):
    # df_hive_tbls=pd.DataFrame([['bq_dataset.bq_tbl1','Y','PARQUET','database1',None], ['bq_dataset.bq_tbl2','N','CSV','database2',None], ['bq_dataset.bq_tbl3','Y','AVRO','database2',None]], columns=['concat_db_tbl','partition_flag','format','database','field_delimiter'])
    # df_partition_clustering = pd.DataFrame([['bq_dataset.bq_tbl1','tbl1_partition', 'tbl1_clustering'], ['bq_dataset.bq_tbl2', 'tbl2_partition'], ['bq_dataset.bq_tbl3',None,'tbl3_clustering']], columns=['concat_db_tbl', 'partition_column','clustering_column' ])
    # df_text_format_schema=pd.DataFrame([['a'], ['c'], ['c']], columns=['text_format_schema'])
    # df_incremental_table_list=pd.DataFrame([['bq_dataset.incremental_table1','destination_path1/incremental_table1/'], ['bq_dataset.incremental_table2','destination_path2/incremental_table2/'], ['bq_dataset.incremental_table3','destination_path3/incremental_table3/'],['bq_dataset.bq_tbl1','destination_path4/bq_tbl1/']], columns=['concat_db_tbl', 'destination_path'])

    mocker.patch(
        "datamigration_utils.hive_bq_load_utils_inc.read_pd_from_gcs",
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
        "datamigration_utils.hive_bq_load_utils_inc.save_load_status_bq",
        return_value="Success",
    )

    result = hive_bq_load_utils_inc.load_bq_tables(
        "bq_dataset.bq_tbl1",
        str(config_without_hive_gcs_staging_path),
        "op_load_dtm",
        "op_run_id",
    )
    assert result == expected_results
