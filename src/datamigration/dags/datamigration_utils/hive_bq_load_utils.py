import ast
import subprocess
from datetime import datetime

import datamigration_utils.constants as constants
import pandas as pd
from google.cloud import bigquery, storage


def write_pd_to_gcs(df, bucket, path):
    """
    Save Pandas dataframe as CSV in GCS path provided
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    bucket.blob(path).upload_from_string(df.to_csv(index=False, sep="|"), "text/csv")


def read_pd_from_gcs(bucket, path):
    """
    Read Pandas dataframe from CSV file saved in GCS
    """
    return pd.read_csv(
        f"gs://{bucket}/{path}", header=0, sep="|", keep_default_na=False
    )


def read_config_file(translation_config):
    """
    Set required variables as dictionary variables from translation config
    """
    config_dict = {}
    config_dict["unique_id"] = translation_config["unique_id"]
    config_dict["rerun_flag"] = translation_config["transfer_config"][
        "rerun_flag"
    ].upper()
    config_dict["hive_db_name"] = translation_config["transfer_config"]["params"][
        "hive_db_name"
    ]
    config_dict["dvt_check_flag"] = translation_config["dvt_check"].upper()
    config_dict["temp_bucket"] = translation_config["transfer_config"]["params"][
        "gcs_temp_bucket"
    ]
    config_dict["bq_dataset"] = translation_config["transfer_config"]["params"][
        "bq_dataset_id"
    ]
    config_dict["hive_gcs_staging_bucket_id"] = translation_config["transfer_config"][
        "params"
    ]["hive_gcs_staging_bucket_id"]
    config_dict["hive_gcs_staging_path"] = translation_config["transfer_config"][
        "params"
    ]["hive_gcs_staging_path"]
    config_dict["project_id"] = translation_config["transfer_config"]["params"][
        "project_id"
    ]
    config_dict["hive_ddl_metadata"] = "hive_ddl_metadata"
    config_dict["bq_dataset_audit"] = "dmt_logs"
    config_dict["bq_load_audit"] = "hive_bqload_audit"
    config_dict["dvt_results"] = "dmt_dvt_results"
    config_dict["schema_results_tbl"] = "dmt_schema_results"
    return config_dict


def get_hive_tables(config):
    """
    Get HIVE tables including format and partition info from hive_ddl_metadata table
    """
    dict = read_config_file(ast.literal_eval(config))
    if dict["rerun_flag"] == "N":
        query = constants.query_rerun_n.format(
            bq_dataset_audit=dict["bq_dataset_audit"],
            hive_ddl_metadata=dict["hive_ddl_metadata"],
            hive_db_name=dict["hive_db_name"],
        )
    else:
        query = constants.query_rerun_y.format(
            bq_dataset_audit=dict["bq_dataset_audit"],
            hive_ddl_metadata=dict["hive_ddl_metadata"],
            hive_db_name=dict["hive_db_name"],
            bq_load_audit=dict["bq_load_audit"],
        )
    print(query)
    client = bigquery.Client()
    query_job_df1 = client.query(query).to_dataframe()
    print(query_job_df1)
    if dict["dvt_check_flag"] == "Y":
        query = constants.query_dvt_y.format(
            bq_dataset_audit=dict["bq_dataset_audit"], dvt_results=dict["dvt_results"]
        )
    else:
        query = constants.query_dvt_n.format(
            bq_dataset_audit=dict["bq_dataset_audit"],
            schema_results_tbl=dict["schema_results_tbl"],
        )
    print(query)
    query_job_df2 = client.query(query).to_dataframe()
    query_job_df = query_job_df1.merge(query_job_df2, on="table")
    write_pd_to_gcs(
        query_job_df,
        dict["temp_bucket"],
        constants.df_hive_tables_path.format(hive_db_name=dict["hive_db_name"]),
    )


def get_partition_clustering_info(config):
    """
    Get Partitioning and Clustering info about the tables from information schema
    """
    client = bigquery.Client()
    dict = read_config_file(ast.literal_eval(config))
    df = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_hive_tables_path.format(hive_db_name=dict["hive_db_name"]),
    )
    hive_tables = "'" + "','".join(df["table"].values.tolist()) + "'"
    query = constants.query_partition_clustering_info.format(
        bq_dataset_name=dict["bq_dataset"], table_names=hive_tables
    )
    print(query)
    partition_cluster_df = client.query(query).to_dataframe()
    write_pd_to_gcs(
        partition_cluster_df,
        dict["temp_bucket"],
        constants.df_partition_clustering_path.format(
            hive_db_name=dict["hive_db_name"]
        ),
    )
    print(partition_cluster_df)
    return df[["table"]].values.tolist()


def get_text_format_schema(config):
    """
    Get schema of tables with TEXT file format
    """
    client = bigquery.Client()
    dict = read_config_file(ast.literal_eval(config))
    df = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_hive_tables_path.format(hive_db_name=dict["hive_db_name"]),
    )
    hive_tables = "'" + "','".join(df["table"].values.tolist()) + "'"
    query = constants.query_text_format_schema.format(
        bq_dataset_name=dict["bq_dataset"], table_names=hive_tables
    )
    text_table_df = client.query(query).to_dataframe()
    write_pd_to_gcs(
        text_table_df,
        dict["temp_bucket"],
        constants.df_text_format_schema_path.format(hive_db_name=dict["hive_db_name"]),
    )


def get_table_size(tbl, table_gs_path):
    """
    get Table size in TBs
    """
    cmd = "gsutil du -s {}".format(table_gs_path)
    truncate_result = subprocess.run(
        cmd, capture_output=True, shell=True, encoding="utf-8"
    )
    res = truncate_result.stdout.split(" ")
    if len(res) > 1:
        tbl_size_tb = int(res[0]) / (1024**4)
        print("Size of Table {} is {} TB".format(tbl, str(tbl_size_tb)))
        if tbl_size_tb > 16:
            return False
        else:
            return True
    else:
        print("Cloud not determine Table Size")
        tbl_size_tb = 0
        return False


def partition_cluster_col_subcmd_1(partition_column, clustering_column, file_format):
    """
    Creates a BQ subcommand for partitioned and clustered tables
    """
    if (
        partition_column is not None and clustering_column is not None
    ):  # Table is partitioned & clusterd
        pccmd = (
            " --hive_partitioning_mode=AUTO --time_partitioning_field={partition_column} --clustering_fields='{clustering_column}' "
        ).format(partition_column=partition_column, clustering_column=clustering_column)
    elif partition_column is not None:  # Table is only partitioned
        pccmd = (
            " --hive_partitioning_mode=AUTO --time_partitioning_field={partition_column} "
        ).format(partition_column=partition_column)
    elif clustering_column is not None:  # Table is only clusterd
        pccmd = (" --clustering_fields='{clustering_column}' ").format(
            clustering_column=clustering_column
        )
    else:
        pccmd = ""
    if file_format == "CSV":
        pccmd = pccmd.replace(f"--time_partitioning_field={partition_column}", "")
    return pccmd


def file_format_subcmd_2(
    file_format, partition_flag, field_delimiter, tbl, df_text_format_schema
):
    """
    Create subcommand to be include in the final BQ load command based on different formats
    """
    formatcmd = ""
    checkformat = 0
    droptable = 0
    text_tbl_schema_string = ""
    if file_format == "CSV":
        formatcmd = f" --field_delimiter={field_delimiter}"
        if partition_flag == "Y":
            text_tbl_schema_string = df_text_format_schema[
                (df_text_format_schema["table_name"] == tbl)
            ]["schema_string"].values[0]
        checkformat = 1
    elif file_format == "PARQUET":
        formatcmd = " --parquet_enable_list_inference=true "
        checkformat = 1
        droptable = 1
    elif file_format == "AVRO":
        formatcmd = " --use_avro_logical_types=true "
        checkformat = 1
    elif file_format == "ORC":
        checkformat = 1
    return formatcmd, checkformat, droptable, text_tbl_schema_string


def truncate_or_drop_tbl(tbl, droptable, dict):
    """
    DROP table if the format is PARQUET else TRUNCATE
    """
    if droptable == 0:
        print(f"\n\nTruncating table: {dict['bq_dataset']}.{tbl}")
        truncate_cmd = f"bq query --use_legacy_sql=false --project_id={dict['project_id']} 'truncate table {dict['bq_dataset']}.{tbl}'"
        result = subprocess.run(
            truncate_cmd, capture_output=True, shell=True, encoding="utf-8"
        )
    else:
        print(f"Drop parquet table: {dict['bq_dataset']}.{tbl}")
        drop_cmd = f"bq query --use_legacy_sql=false --project_id={dict['project_id']} 'drop table {dict['bq_dataset']}.{tbl}'"
        result = subprocess.run(
            drop_cmd, capture_output=True, shell=True, encoding="utf-8"
        )
    return result


def get_job_status(tbl, result):
    """
    Get Job status of BQ load command
    """
    if result.returncode == 0:
        print("Loaded table: {} ".format(tbl))
        load_status = "PASS"
        reason_for_failure = "NA"
        if len(result.stderr.split(" ")) > 2:
            bq_job_id = result.stderr.split(" ")[2]
            print("BigQuery job id: {}".format(result.stderr.split(" ")[2]))
        elif len(result.stdout.split(":")) > 2:
            bq_job_id = result.stdout.split(":")[2].replace("'", "")
            print("BigQuery job id: {}".format(result.stdout.split(":")[2]))
        else:
            bq_job_id = "NA"
    else:
        print("Error Ocuured while loading table: {table} ".format(table=tbl))
        reason_for_failure = result.stdout.strip()
        load_status = "FAIL"
        if len(result.stderr.split(" ")) > 2:
            bq_job_id = result.stderr.split(" ")[2]
        elif len(result.stdout.split(":")) > 2:
            bq_job_id = result.stdout.split(":")[2].replace("'", "")
        else:
            bq_job_id = "NA"
        print("Reason: {}".format(reason_for_failure))
        print("Printing STD ERROR")
        print(result.stderr)
        print("BigQuery job id: {}".format(bq_job_id))
    return load_status, reason_for_failure, bq_job_id


def save_load_status_bq(
    tablename, load_status, reason_for_failure, bq_job_id, dict, op_load_dtm, op_run_id
):
    """
    Save BQ Load status in Audit table
    """
    client = bigquery.Client()
    metadata_list = [
        {
            "load_dtm": str(op_load_dtm),
            "run_id": str(op_run_id),
            "hive_db_name": dict["hive_db_name"],
            "bq_datatset": dict["bq_dataset"],
            "tablename": tablename,
            "bq_job_id": bq_job_id,
            "load_status": load_status,
            "reason_for_failure": reason_for_failure,
        }
    ]

    client.insert_rows_json(
        f"{dict['bq_dataset_audit']}.{dict['bq_load_audit']}", metadata_list
    )
    print("Audit table loaded for table: {}".format(tablename))


def load_bq_tables(tbl, config, op_load_dtm):
    """
    Load BigQuery table using BQ load command
    """
    start = datetime.now()
    print(f"Loading Table: {tbl} Start Time: {str(start)}")
    dict = read_config_file(ast.literal_eval(config))
    op_run_id = dict["unique_id"]
    df_hive_tbls = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_hive_tables_path.format(hive_db_name=dict["hive_db_name"]),
    )
    df_partition_clustering = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_partition_clustering_path.format(
            hive_db_name=dict["hive_db_name"]
        ),
    )
    df_text_format_schema = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_text_format_schema_path.format(hive_db_name=dict["hive_db_name"]),
    )
    partition_flag = (
        df_hive_tbls[df_hive_tbls["table"] == tbl]["partition_flag"].values[0].upper()
    )
    file_format = df_hive_tbls[df_hive_tbls["table"] == tbl]["format"].values[0].upper()
    field_delimiter = df_hive_tbls[df_hive_tbls["table"] == tbl][
        "field_delimiter"
    ].values[0]
    table_gs_path = constants.hive_tbl_gcs_path.format(
        bkt_id=dict["hive_gcs_staging_bucket_id"],
        path=dict["hive_gcs_staging_path"],
        tbl=tbl,
    )
    # TODO filter out tables larger than 15 TB
    table_size_status = get_table_size(tbl, table_gs_path)
    if table_size_status:
        if partition_flag == "Y":
            partition_column = df_partition_clustering[
                (df_partition_clustering["table_name"] == tbl)
            ]["partition_column"].values[0]
            clustering_column = df_partition_clustering[
                (df_partition_clustering["table_name"] == tbl)
            ]["clustering_column"].values[0]
            hive_partition_source_uri = (
                " --hive_partitioning_source_uri_prefix={table_gs_path}"
            ).format(table_gs_path=table_gs_path)
            pccmd = partition_cluster_col_subcmd_1(
                partition_column, clustering_column, file_format
            )
        else:
            pccmd = ""
            hive_partition_source_uri = ""
            print(f"Table {tbl} is not partitioned")

        (
            formatcmd,
            checkformat,
            droptable,
            text_tbl_schema_string,
        ) = file_format_subcmd_2(
            file_format, partition_flag, field_delimiter, tbl, df_text_format_schema
        )
        if checkformat == 1:
            result = truncate_or_drop_tbl(tbl, droptable, dict)
            bqloadcmd = (
                f"bq load --source_format={file_format} {formatcmd} "
                f"{pccmd} --project_id={dict['project_id']} "
                f"{hive_partition_source_uri} {dict['bq_dataset']}.{tbl} "
                f"{table_gs_path}/* "
                f"{text_tbl_schema_string}"
            )
            print(bqloadcmd)
            result = subprocess.run(
                bqloadcmd, capture_output=True, shell=True, encoding="utf-8"
            )
            load_status, reason_for_failure, bq_job_id = get_job_status(tbl, result)
            save_load_status_bq(
                tbl,
                load_status,
                reason_for_failure,
                bq_job_id,
                dict,
                op_load_dtm,
                op_run_id,
            )
        else:
            print("Incorrect Source File Format for table: {}".format(tbl))
            save_load_status_bq(
                tbl,
                "FAIL",
                "Incorrect Source Format",
                "NA",
                dict,
                op_load_dtm,
                op_run_id,
            )
        end = datetime.now()
        print("\nTotal Time Taken to load the table : {}".format(end - start))
    else:
        print(f"Skipping the table from load since it is more than 16TB: {tbl}")
        save_load_status_bq(
            tbl,
            "FAIL",
            "Cannot load table greater than 16TB",
            "NA",
            dict,
            op_load_dtm,
            op_run_id,
        )
