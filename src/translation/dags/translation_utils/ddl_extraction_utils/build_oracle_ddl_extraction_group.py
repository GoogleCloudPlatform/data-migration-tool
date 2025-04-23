import ast
import json
import logging
import os
import re
from datetime import datetime

import oracledb
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery, storage

from common_utils import constants, storage_utils

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


# Storage utils object
gcs_util = storage_utils.StorageUtils()

# DDL Extraction result table
DDL_EXTRACTION_TABLE = f"{PROJECT_ID}.dmt_logs.dmt_extract_ddl_results"

BATCH_TRANSLATOR_DAG_ID = "batch_sql_translation"

METADATA_FOLDER = "ddl"


# Function to Convert CLOB to String
def output_type_handler(cursor, name, defaultType, size, precision, scale):
    if defaultType == oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)


"""
Method: _extract_ddl
Description: Extract DDL
Arguments: **kwargs
"""


def _extract_ddl(ti, **kwargs):
    jsonString = json.dumps(kwargs["dag_run"].conf["config"])
    ti.xcom_push(key="next_dag_config", value=jsonString)
    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    try:
        user = config["validation_config"]["source_config"]["user"]
        password = config["validation_config"]["source_config"]["password"]
        host = config["validation_config"]["source_config"]["host"]
        port = config["validation_config"]["source_config"]["port"]
        serviceName = config["validation_config"]["source_config"]["database"]
        if password.startswith(constants.SECRET_PREFIX):
            password = Variable.get(password.removeprefix(constants.SECRET_PREFIX))
        con = oracledb.connect(
            user=user, password=password, host=host, port=port, service_name=serviceName
        )
        logging.info("Connected")
        translationSourcePath = config["migrationTask"]["translationConfigDetails"][
            "gcsSourcePath"
        ]
        (
            bucket,
            sourceFolder,
        ) = storage_utils.StorageUtils().parse_bucket_and_blob_from_path(
            translationSourcePath
        )
        sourceSchema = config["migrationTask"]["translationConfigDetails"][
            "nameMappingList"
        ]["name_map"][0]["source"]["schema"]
        cursor = con.cursor()

    except oracledb.DatabaseError as e:
        logging.info("Connection to oracle failed")
        logging.info(str(e))
        raise Exception(str(e))
    else:
        try:
            con.outputtypehandler = output_type_handler
            query = """
                        WITH cte_sql AS
                        (
                        select  table_name table_name, 0 seq, \
                            'CREATE TABLE ' ||rtrim(owner)||'.'||\
                                rtrim(table_name) || '(' AS sql_out
                        from all_tab_columns where owner = upper('{0}')
                        union
                        select table_name table_name,
                        column_id seq,
                        decode(column_id,1,' ',' ,')||
                        rtrim(column_name)||' '||
                        rtrim(data_type) ||' '||
                        rtrim(decode(data_type,'DATE',null,'LONG',null,
                            'NUMBER',decode(to_char(data_precision),null,null,'('),
                            '(')) ||
                        rtrim(decode(data_type,
                            'DATE',null,
                            'CHAR',data_length,
                            'VARCHAR2',data_length,
                            'NUMBER',decode(to_char(data_precision),null,null,
                                to_char(data_precision) || ',' || \
                                    to_char(data_scale)),
                            'LONG',null,
                            '')) ||
                        rtrim(decode(data_type,'DATE',null,'LONG',null,
                            'NUMBER',decode(to_char(data_precision),null,null,')'),
                            ')')) ||' '||
                        rtrim(decode(nullable,'N','NOT NULL',null)) AS sql_out
                        from all_tab_columns where owner = upper('{0}')
                        union
                        select  table_name table_name,
                                999999 seq,
                                ')' AS sql_out
                        from all_tab_columns
                        where owner = upper('{0}')
                        )
                        select
                        xmlagg (xmlelement (e, sql_out || '') ORDER BY seq)\
                            .extract ('//text()').getclobval() sql_output
                        from
                        cte_sql
                        group by
                        table_name""".format(
                user
            )

            cursor.execute(query)
            output = cursor.fetchall()
            extractionResult = []

            for table in output:
                execTime = datetime.utcnow()
                regex = r"(\w+)+\.(\w+)"
                getTableName = re.search(regex, table[0])

                if getTableName:
                    sourceDataset = getTableName[0].split(".")[0]
                    tableName = getTableName[0].split(".")[1]
                else:
                    logging.info("Table Name not found")

                regex1 = r".+;\s*$"
                searchSemiColon = re.search(regex1, table[0])
                addSemiColon = table[0]

                if searchSemiColon is None:
                    semiColon = ";"
                    addSemiColon = "".join([table[0], semiColon])

                fileName = f"{sourceDataset}-{tableName}.sql"
                gcsClient = storage.Client(PROJECT_ID)
                bucketName = gcsClient.bucket(bucket)

                opType = config["type"]
                objectName = ""
                if opType in ["sql", "dml"]:
                    objectName = sourceFolder + "/ddl/" + fileName
                else:
                    objectName = sourceFolder + "/" + fileName
                blob = bucketName.blob(objectName)
                blob.upload_from_string(addSemiColon)

                extractionResult.append(
                    {
                        "unique_id": config["unique_id"],
                        "source_db": "Oracle",
                        "database": sourceSchema,
                        "schema": sourceSchema,
                        "table_name": tableName,
                        "file_path": f"{bucket}/{objectName}",
                        "extraction_time": str(execTime),
                    }
                )

            insertResult = bigquery.Client().insert_rows_json(
                DDL_EXTRACTION_TABLE, extractionResult
            )
            logging.info(f"extraction insertion result: {insertResult}")
            logging.info("Done")

        except Exception as e:
            logging.info("Extraction failed")
            logging.info(str(e))
            raise Exception(str(e))
        else:
            logging.info("DDL Generated Successfully")


def build_oracle_ddl_extraction_group(dag: DAG) -> TaskGroup:
    oracle_extraction_taskgroup = TaskGroup(group_id="oracle_extraction_taskgroup")

    extract_ddl = PythonOperator(
        task_id="extract_ddl",
        python_callable=_extract_ddl,
        task_group=oracle_extraction_taskgroup,
        dag=dag,
    )

    invoke_batch_translator_dag = TriggerDagRunOperator(
        task_id="invoke_batch_translator_dag_task",
        trigger_dag_id=BATCH_TRANSLATOR_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(key='next_dag_config', task_ids='oracle_extraction_taskgroup.extract_ddl') }}"
        },
        task_group=oracle_extraction_taskgroup,
        dag=dag,
    )
    extract_ddl >> invoke_batch_translator_dag

    return oracle_extraction_taskgroup
