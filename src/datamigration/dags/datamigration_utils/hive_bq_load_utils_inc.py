import ast
import subprocess
from datetime import date, datetime

import datamigration_utils.constants as constants
import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound


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
    config_dict["temp_bucket"] = translation_config["transfer_config"]["params"][
        "gcs_temp_bucket"
    ]
    config_dict["gcs_inc_staging_bucket"] = translation_config["transfer_config"][
        "params"
    ]["gcs_inc_staging_bucket"]
    config_dict["project_id"] = translation_config["transfer_config"]["params"][
        "project_id"
    ]
    config_dict["hive_gcs_staging_path"] = translation_config["transfer_config"][
        "params"
    ]["hive_gcs_staging_path"]
    config_dict["unique_id"] = translation_config["unique_id"]
    config_dict["source_bucket_name"] = translation_config["transfer_config"]["params"][
        "hive_gcs_staging_bucket_id"
    ]
    config_dict["hive_ddl_metadata"] = "hive_ddl_metadata"
    config_dict["bq_dataset_audit"] = "dmt_logs"
    config_dict["bq_load_audit"] = "hive_inc_bqload_audit"
    config_dict["dvt_results"] = "dmt_dvt_results"
    config_dict["schema_results_tbl"] = "dmt_schema_results"
    config_dict["hive_inc_load_tbl"] = "hive_inc_load_table_list"
    config_dict["dt"] = datetime.today().strftime("%Y%m%d")
    return config_dict


def get_inc_gcs_files(bq_audit_dataset_id, bq_pubsub_audit_tbl, schedule_runtime):
    """
    Get GCS path from pubsub audit table for incremental data
    """
    client = bigquery.Client()
    query = constants.hive_pubsub_audit_files.format(
        bq_dataset_audit=bq_audit_dataset_id,
        bq_pubsub_audit_tbl=bq_pubsub_audit_tbl,
        schedule_runtime=schedule_runtime,
    )
    print(query)
    query_job_res = client.query(query).to_dataframe().values.tolist()
    if len(query_job_res) > 0:
        print("New Data Found for")
        print("Table List:")
        query_job_lst = [item for sublist in query_job_res for item in sublist]
        print(query_job_lst)
        return query_job_lst
    else:
        return ""


def get_inc_table_list_for_copy(config, inc_gcs_files_list):
    """
    Get List of incremental tables using GCS path
    Expected path: hive_gcs_staging_path/databasename/tablename/*
    """
    dict = read_config_file(ast.literal_eval(config))
    hive_gcs_staging_path = dict["hive_gcs_staging_path"]
    client = bigquery.Client()
    tbl_list = []
    print("Incremental File list")
    print(inc_gcs_files_list)
    if len(hive_gcs_staging_path) > 0:
        for gcs_path in inc_gcs_files_list:
            gcs_str = (
                gcs_path.replace(hive_gcs_staging_path + "/", "")
                if len(hive_gcs_staging_path) > 0
                else gcs_path
            )
            if len(gcs_str.split("/")) > 2:
                dbname = gcs_str.split("/")[0].split(".")[0]
                tblname = gcs_str.split("/")[1]
                tbl_gcs_str = check_bq_table(client, gcs_path, dbname, tblname, dict)
                tbl_list.append(tbl_gcs_str) if len(tbl_gcs_str) > 0 else None
            else:
                print("Issue in parsing inc GCS string")
                return [[]]
    else:
        for gcs_str in inc_gcs_files_list:
            if len(gcs_str.split("/")) > 2:
                dbname = gcs_str.split("/")[0].split(".")[0]
                tblname = gcs_str.split("/")[1]
                tbl_gcs_str = check_bq_table(client, gcs_str, dbname, tblname, dict)
                tbl_list.append(tbl_gcs_str) if len(tbl_gcs_str) > 0 else None
    return [[tbl] for tbl in tbl_list]


def check_bq_table(gcs_path, dbname, tblname, dict):
    """
    Check if table is present in BQ
    """
    try:
        get_bq_dataset_query = constants.query_get_bq_dataset.format(
            bq_dataset_audit=dict["bq_dataset_audit"],
            hive_ddl_metadata=dict["hive_ddl_metadata"],
            table=tblname,
            hive_db=dbname,
        )
        print(get_bq_dataset_query)
        client = bigquery.Client()
        bq_dataset = client.query(get_bq_dataset_query).to_dataframe().values[0][0]
        print(bq_dataset + "." + tblname)
        client.get_table(bq_dataset + "." + tblname)
        return str(dbname + "," + bq_dataset + "." + tblname + "," + gcs_path)
    except NotFound:
        return ""
    except Exception as e:
        print(e)


def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    print(blob_copy)


def save_file_copy_status(
    unique_id,
    job_run_time,
    load_start_time,
    load_end_time,
    hive_db,
    bq_dataset,
    table_name,
    source_bucket_name,
    source_gcs_path,
    dest_bucket_name,
    destination_gcs_path,
    file_copy_status,
    bq_dataset_audit,
    hive_inc_load_tbl,
):
    source_path = f"gs://{source_bucket_name}/{source_gcs_path}"
    destination_path = f"gs://{dest_bucket_name}/{destination_gcs_path}"
    metadata_list = [
        {
            "unique_id": unique_id,
            "job_run_time": job_run_time,
            "load_start_time": str(load_start_time),
            "load_end_time": str(load_end_time),
            "hive_db": hive_db,
            "bq_dataset": bq_dataset,
            "table_name": table_name,
            "source_path": source_path,
            "destination_path": destination_path,
            "file_copy_status": file_copy_status,
        }
    ]
    print(metadata_list)
    client = bigquery.Client()
    status = client.insert_rows_json(
        bq_dataset_audit + "." + hive_inc_load_tbl,
        metadata_list,
    )
    print(status)


def copy_inc_files(tbl_data, config_data, job_run_time):
    """
    Copy incremental data to temporary bucket
    """
    print("Table Data: " + tbl_data)
    load_start_time = datetime.now()
    dict = read_config_file(ast.literal_eval(config_data))
    tbl_data_lst = tbl_data.split(",")
    hive_db = tbl_data_lst[0].split(".")[0]
    bq_table = tbl_data_lst[1]
    source_gcs_path = tbl_data_lst[2]
    hive_gcs_staging_path = dict["hive_gcs_staging_path"]
    source_bucket_name = dict["source_bucket_name"]
    gcs_inc_staging_bucket = dict["gcs_inc_staging_bucket"]
    get_bq_dataset_query = constants.query_get_bq_dataset.format(
        bq_dataset_audit=dict["bq_dataset_audit"],
        hive_ddl_metadata=dict["hive_ddl_metadata"],
        table=tbl_data_lst[1].split(".")[1],
        hive_db=hive_db,
    )
    print(get_bq_dataset_query)
    client = bigquery.Client()
    bq_dataset_df = client.query(get_bq_dataset_query).to_dataframe()
    if len(bq_dataset_df) > 0:
        bq_dataset = bq_dataset_df.values[0][0]
        print(
            f"BQ Dataset corresponding to table {bq_table} and hive db {hive_db} is {bq_dataset}"
        )
        # bq_dataset = dict["hive_bq_dataset_mapping"][hive_db.split(".")[0]]
        if len(hive_gcs_staging_path) > 0:
            destination_gcs_path = (
                str(date.today())
                + "/"
                + source_gcs_path.replace(hive_gcs_staging_path + "/", "")
            )
        else:
            destination_gcs_path = str(date.today()) + "/" + source_gcs_path
        try:
            copy_blob(
                source_bucket_name,
                source_gcs_path,
                gcs_inc_staging_bucket,
                destination_gcs_path,
            )
            file_copy_status = "PASS"
        except Exception as e:
            print(f"Failed to copy file: gs://{source_bucket_name}/{source_gcs_path}")
            print(e)
            file_copy_status = "FAIL"
        load_end_time = datetime.now()
        save_file_copy_status(
            dict["unique_id"],
            job_run_time,
            load_start_time,
            load_end_time,
            hive_db,
            bq_dataset,
            bq_table,
            source_bucket_name,
            source_gcs_path,
            gcs_inc_staging_bucket,
            destination_gcs_path,
            file_copy_status,
            dict["bq_dataset_audit"],
            dict["hive_inc_load_tbl"],
        )
    else:
        print(
            f"Could not retrieve BQ dataset id for  table {bq_table} and hive db {hive_db} from hive ddl audit table "
        )
        save_file_copy_status(
            dict["unique_id"],
            job_run_time,
            load_start_time,
            load_end_time,
            hive_db,
            "NA",
            bq_table,
            source_bucket_name,
            source_gcs_path,
            gcs_inc_staging_bucket,
            "NA",
            "FAIL",
            dict["bq_dataset_audit"],
            dict["hive_inc_load_tbl"],
        )


def get_inc_table_list(config):
    dict = read_config_file(ast.literal_eval(config))
    dt = dict["dt"]
    client = bigquery.Client()
    query = constants.query_get_incremental_list_of_tables.format(
        bq_dataset_audit=dict["bq_dataset_audit"],
        bq_inc_copy_status_tbl=dict["hive_inc_load_tbl"],
    )
    print(query)
    incremental_table_list_df = client.query(query).to_dataframe()
    write_pd_to_gcs(
        incremental_table_list_df,
        dict["temp_bucket"],
        constants.df_inc_tables_list.format(dt=dt),
    )
    print(incremental_table_list_df)


def get_table_info_from_metadata(config):
    dict = read_config_file(ast.literal_eval(config))
    dt = dict["dt"]
    client = bigquery.Client()
    df = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_inc_tables_list.format(dt=dt),
    )
    hive_tables_list = "'" + "','".join(df["concat_db_tbl"].values.tolist()) + "'"
    query = constants.query_get_inc_table_metadata.format(
        bq_dataset_audit=dict["bq_dataset_audit"],
        hive_ddl_metadata=dict["hive_ddl_metadata"],
        tbl_list=hive_tables_list,
    )
    print(query)
    metadata_table_list_df = client.query(query).to_dataframe()
    write_pd_to_gcs(
        metadata_table_list_df,
        dict["temp_bucket"],
        constants.df_inc_table_list_metadata.format(dt=dt),
    )


def get_partition_clustering_info(config):
    """
    Get Partitioning and Clustering info about the tables from information schema
    """
    client = bigquery.Client()
    dict = read_config_file(ast.literal_eval(config))
    dt = dict["dt"]
    df = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_inc_table_list_metadata.format(dt=dt),
    )
    database_list_array = df["bq_dataset"].unique()
    database_list = sorted(database_list_array)
    print(database_list)
    df_list = []
    for dbname in database_list:
        hive_tables = (
            "'"
            + "','".join(df.loc[df["bq_dataset"] == dbname]["table"].values.tolist())
            + "'"
        )
        query = constants.query_inc_tbl_partition_clustering_info.format(
            bq_dataset_name=dbname, table_names=hive_tables
        )
        print(query)
        partition_cluster_sub_df = client.query(query).to_dataframe()
        df_list.append(partition_cluster_sub_df)
    if len(df_list) > 0:
        partition_cluster_df = pd.concat(df_list)
        write_pd_to_gcs(
            partition_cluster_df,
            dict["temp_bucket"],
            constants.df_partition_clustering_inc_tbl_path.format(dt=dt),
        )
        print(partition_cluster_df)
    else:
        print(
            "No database / partition clustering table list found. So nothing to concatenate"
        )
    return df[["concat_db_tbl"]].values.tolist()


def get_text_format_schema(config):
    """
    Get schema of tables with TEXT file format
    """
    client = bigquery.Client()
    dict = read_config_file(ast.literal_eval(config))
    dt = dict["dt"]
    df = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_inc_table_list_metadata.format(dt=dt),
    )
    database_list_array = df["bq_dataset"].unique()
    database_list = sorted(database_list_array)
    print(database_list)
    df_list = []
    for dbname in database_list:
        hive_tables = (
            "'"
            + "','".join(df.loc[df["bq_dataset"] == dbname]["table"].values.tolist())
            + "'"
        )
        query = constants.query_inc_tbl_text_format_schema_info.format(
            bq_dataset_name=dbname, table_names=hive_tables
        )
        print(query)
        text_table_sub_df = client.query(query).to_dataframe()
        df_list.append(text_table_sub_df)
    if len(df_list) > 0:
        text_table_df = pd.concat(df_list)
        write_pd_to_gcs(
            text_table_df,
            dict["temp_bucket"],
            constants.df_text_format_schema_inc_tbl_path.format(dt=dt),
        )
        print(text_table_df)
    else:
        print(
            "No database / partition clustering table list found. So nothing to concatenate"
        )


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
    file_format, partition_flag, field_delimiter, concat_db_tbl, df_text_format_schema
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
                (df_text_format_schema["concat_db_tbl"] == concat_db_tbl)
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
            print("BigQuery job id: {}".format(bq_job_id))
        elif len(result.stdout.split(":")) > 2:
            bq_job_id = result.stdout.split(":")[2].replace("'", "")
            print("BigQuery job id: {}".format(bq_job_id))
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
    tablename,
    load_status,
    reason_for_failure,
    bq_job_id,
    bq_dataset,
    hive_db_name,
    dict,
    op_load_dtm,
    op_run_id,
):
    """
    Save BQ Load status in Audit table
    """
    client = bigquery.Client()
    metadata = []
    metadata.append(
        [
            op_load_dtm,
            op_run_id,
            hive_db_name,
            bq_dataset,
            tablename,
            bq_job_id,
            load_status,
            reason_for_failure,
        ]
    )
    df = pd.DataFrame(
        metadata,
        columns=[
            "load_dtm",
            "run_id",
            "hive_db_name",
            "bq_datatset",
            "tablename",
            "bq_job_id",
            "load_status",
            "reason_for_failure",
        ],
    )

    df["load_dtm"] = df["load_dtm"].astype("datetime64[ns]")
    df["run_id"] = df["run_id"].astype("int64")
    client.load_table_from_dataframe(
        df, f"{dict['bq_dataset_audit']}.{dict['bq_load_audit']}"
    )
    print("Audit table loaded for table: {}".format(tablename))


def load_bq_tables(concat_db_tbl, config, op_load_dtm, op_run_id):
    """
    Load BigQuery table using BQ load command
    """
    start = datetime.now()
    print(f"Loading Table: {concat_db_tbl} Start Time: {str(start)}")
    dict = read_config_file(ast.literal_eval(config))
    dt = dict["dt"]
    df_hive_tbls = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_inc_table_list_metadata.format(dt=dt),
    )
    df_partition_clustering = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_partition_clustering_inc_tbl_path.format(dt=dt),
    )
    df_text_format_schema = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_text_format_schema_inc_tbl_path.format(dt=dt),
    )

    df_incremental_table_list = read_pd_from_gcs(
        dict["temp_bucket"],
        constants.df_inc_tables_list.format(dt=dt),
    )
    partition_flag = (
        df_hive_tbls[df_hive_tbls["concat_db_tbl"] == concat_db_tbl]["partition_flag"]
        .values[0]
        .upper()
    )
    file_format = (
        df_hive_tbls[df_hive_tbls["concat_db_tbl"] == concat_db_tbl]["format"]
        .values[0]
        .upper()
    )
    hive_db_name = df_hive_tbls[df_hive_tbls["concat_db_tbl"] == concat_db_tbl][
        "database"
    ].values[0]
    field_delimiter = df_hive_tbls[df_hive_tbls["concat_db_tbl"] == concat_db_tbl][
        "field_delimiter"
    ].values[0]
    table_gs_path_list = list(
        df_incremental_table_list[
            df_incremental_table_list["concat_db_tbl"] == concat_db_tbl
        ]["destination_path"].values
    )
    bq_dataset = concat_db_tbl.split(".")[0]
    tbl = concat_db_tbl.split(".")[1]
    print(f"Loading Table: {concat_db_tbl}")
    print(f"partition_flag : {partition_flag}")
    print(f"file_format : {file_format}")
    print(f"bq_dataset : {bq_dataset}")
    print(f"tbl : {tbl}")
    for table_gs_path in table_gs_path_list:
        print(f"Appending {tbl} from: {table_gs_path}".format(tbl, table_gs_path))
        hive_table_gs_path = table_gs_path.split(f"/{tbl}/")[0] + f"/{tbl}"
        if partition_flag == "Y":
            partition_column = df_partition_clustering[
                (df_partition_clustering["concat_db_tbl"] == concat_db_tbl)
            ]["partition_column"].values[0]
            clustering_column = df_partition_clustering[
                (df_partition_clustering["concat_db_tbl"] == concat_db_tbl)
            ]["clustering_column"].values[0]
            hive_partition_source_uri = (
                f" --hive_partitioning_source_uri_prefix={hive_table_gs_path}"
            ).format(table_gs_path)
            pccmd = partition_cluster_col_subcmd_1(
                partition_column, clustering_column, file_format
            )
        else:
            pccmd = ""
            hive_partition_source_uri = ""
            print(f"Table {concat_db_tbl} is not partitioned")
        (
            formatcmd,
            checkformat,
            droptable,
            text_tbl_schema_string,
        ) = file_format_subcmd_2(
            file_format,
            partition_flag,
            field_delimiter,
            concat_db_tbl,
            df_text_format_schema,
        )
        if checkformat == 1:
            # result = truncate_or_drop_tbl(tbl, droptable, dict)
            bqloadcmd = (
                f"bq load --source_format={file_format} {formatcmd} "
                f"{pccmd} --project_id={dict['project_id']} "
                f"{hive_partition_source_uri} {bq_dataset}.{tbl} "
                f"{table_gs_path} "
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
                bq_dataset,
                hive_db_name,
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
                bq_dataset,
                hive_db_name,
                dict,
                op_load_dtm,
                op_run_id,
            )
        end = datetime.now()
        print("\nTotal Time Taken to load the table : {}".format(end - start))
