#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os
import sys
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession


def get_spark_session(host_ip):
    """
    Get Spark Session using HIVE THRIFT SERVER IP provided
    """
    spark = (
        SparkSession.builder.appName("extract_hove_ddl")
        .config("spark.sql.debug.maxToStringFields", 2000)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def read_json_file(translation_config_file):
    with open(translation_config_file) as json_file:
        data = json.load(json_file)
    json_file.close()
    return data


def read_translation_config(translation_config):
    """
    Convert JSON config to dictionary
    """
    dict = {}
    dict["bigquery_audit_table"] = "hive_ddl_metadata"
    dict["bq_dataset_audit"] = "dmt_logs"
    dict["columns"] = [
        "run_id",
        "start_time",
        "database",
        "bq_dataset",
        "table",
        "field_delimiter",
        "partition_flag",
        "cluster_flag",
        "format",
        "ddl_extracted",
    ]
    dict["host_ip"] = translation_config["hive_config"]["server_config"]["connection"][
        "host"
    ]
    source_path = translation_config["migrationTask"]["translationConfigDetails"][
        "gcsSourcePath"
    ]
    dict["bucket_name"] = source_path.split("/")[2]
    dict["gcs_ddl_output_path"] = source_path.split("/", 3)[-1]
    nm_map_list = translation_config["migrationTask"]["translationConfigDetails"][
        "nameMappingList"
    ]["name_map"]
    dict["hive_db"] = list(set(d["source"]["schema"] for d in nm_map_list))[0]
    dict["bq_dataset"] = list(set(d["target"]["schema"] for d in nm_map_list))[0]
    input_tables = translation_config["source_ddl_extract_table_list"]
    dict["input_tables_list"] = [x.lower() for x in input_tables.split(",")]
    return dict


def get_table_list(dict, spark):
    """
    Create list of tables to be loaded
    """
    table_list = []
    tables = spark.catalog.listTables(dict["hive_db"])
    for tbl in tables:
        table_list.append(tbl.name.lower())
    if dict["input_tables_list"][0] != "*":
        table_list = dict["input_tables_list"]
    else:
        table_list = table_list
    return table_list


def get_table_format(tbl, hive_db, spark):
    """
    Get table format
    """
    df = spark.sql(f"describe formatted {hive_db}.{tbl}")
    format_str = (
        df.filter("col_name == 'InputFormat'").select("data_type").first()[0].upper()
    )
    if "AVRO" in format_str:
        return "AVRO"
    elif "PARQUET" in format_str:
        return "PARQUET"
    elif "ORC" in format_str:
        return "ORC"
    elif "TEXT" in format_str:
        return "CSV"
    else:
        return "OTHER"


def get_partition_cluster_info(ddl_hive):
    """
    Get Partitioning and Clustering info
    """
    partitioning_flag = ""
    clustering_flag = ""

    if "PARTITIONED BY" in ddl_hive:
        partitioning_flag = "Y"
    else:
        partitioning_flag = "N"
    if "CLUSTERED BY" in ddl_hive:
        clustering_flag = "Y"
    else:
        clustering_flag = "N"

    return partitioning_flag, clustering_flag


def get_tbl_delimiter(hive_ddl_str):
    """
    Get Field Delimiter for TEXT tables
    Default Value:'\001' (default HIVE table delimiter)
    """
    if "field.delim' = " in hive_ddl_str:
        delim = repr(hive_ddl_str.split("field.delim' = ")[1].split("'")[1])
    else:
        delim = "\001"
    return delim


def get_paths(dict, run_time):
    ddl_path = (
        os.getcwd() + "/" + dict["hive_db"] + "_" + str(run_time).replace(" ", "_")
    )
    os.mkdir(ddl_path)
    # Create DIR for HiveDB table metadata
    metadata_path = (
        os.getcwd()
        + "/"
        + dict["hive_db"]
        + "_metadata_"
        + str(run_time).replace(" ", "_")
    )
    os.mkdir(metadata_path)
    return ddl_path, metadata_path


def WriteToLocal(ddl, ddl_path, table_name):
    print("Writing DDL to GCS: " + table_name)
    with open(f"{ddl_path}/{table_name}.sql", "w") as text_file:
        text_file.write(ddl)
    text_file.close()


def get_hive_ddls(dict, run_id, spark):
    """
    extract HIVE DDls and table metadata
    """
    metadata = []
    run_time = datetime.now()
    dbCheck = spark.catalog._jcatalog.databaseExists(dict["hive_db"])
    hive_db = dict["hive_db"]
    bq_dataset = dict["bq_dataset"]
    if dbCheck:
        table_list = get_table_list(dict, spark)
        ddl_path, metadata_path = get_paths(dict, run_time)
        for tbl in table_list:
            print(f"Extracting DDL for Table {tbl}")
            ddl_hive = ""
            try:
                ddl_hive_df = spark.sql(f"show create table {hive_db}.{tbl} as serde")
                ddl_hive = (
                    ddl_hive_df.first()[0]
                    .split("\nLOCATION '")[0]
                    .split("\nSTORED AS")[0]
                )
            except Exception:
                print(f"Could not get DDL for table: {tbl}, trying without SERDE now..")
            if len(ddl_hive) < 1:
                try:
                    ddl_hive_df = spark.sql(f"show create table  {hive_db}.{tbl}")
                    ddl_hive = ddl_hive_df.first()[0].split("\nUSING ")[0]
                except Exception as e:
                    print(e)
            if len(ddl_hive) > 1:
                ddl_hive = (
                    ddl_hive.replace(f"{hive_db}.", "").replace(
                        f" TABLE {tbl}", f" TABLE IF NOT EXISTS {hive_db}.{tbl}"
                    )
                    + ";"
                )
                WriteToLocal(ddl_hive, ddl_path, tbl)
                storage_format = get_table_format(tbl, hive_db, spark)
                partition_flag, cluster_flag = get_partition_cluster_info(ddl_hive)
                if storage_format == "CSV":
                    field_delimiter = get_tbl_delimiter(ddl_hive_df.first()[0])
                else:
                    field_delimiter = "NA"
                ddl_extracted = "YES"
            else:
                (
                    ddl_extracted,
                    partition_flag,
                    cluster_flag,
                    storage_format,
                    field_delimiter,
                ) = ("NO", "", "", "", "")

            metadata.append(
                [
                    run_id,
                    run_time,
                    hive_db,
                    bq_dataset,
                    tbl,
                    field_delimiter,
                    partition_flag,
                    cluster_flag,
                    storage_format,
                    ddl_extracted,
                ]
            )

            df = pd.DataFrame(
                metadata,
                columns=[
                    "run_id",
                    "start_time",
                    "database",
                    "bq_dataset",
                    "table",
                    "field_delimiter",
                    "partition_flag",
                    "cluster_flag",
                    "format",
                    "ddl_extracted",
                ],
            )
        df["start_time"] = df["start_time"].astype(
            "string"
        )  # .astype("datetime64[ns]")
        df["run_id"] = df["run_id"].astype("string")
        df.to_csv(
            metadata_path + f"/{hive_db}.csv", index=False, sep=str("\t"), header=True
        )
        # df.to_parquet(metadata_path+f"/{hive_db}.parquet",index=False)
        print("Extracted DDL path: " + ddl_path)
        print("Metadata Path: " + metadata_path)
        spark.stop()


def main(translation_config, run_id):
    dict = read_translation_config(translation_config)
    spark = get_spark_session(dict["host_ip"])
    get_hive_ddls(dict, run_id, spark)


if __name__ == "__main__":
    translation_config_file = sys.argv[1]
    translation_config = read_json_file(translation_config_file)
    run_id = translation_config["unique_id"]
    main(translation_config, run_id)
