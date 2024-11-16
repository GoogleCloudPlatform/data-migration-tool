import ast
import json
import logging
import os
from datetime import datetime

import redshift_connector
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

from common_utils import constants, storage_utils

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


# Storage utils object
gcs_util = storage_utils.StorageUtils()

# DDL Extraction result table
DDL_EXTRACTION_TABLE = f"{PROJECT_ID}.dmt_logs.dmt_extract_ddl_results"

METADATA_FOLDER = "ddl"


def _extract_redshift_ddl(ti, **kwargs):
    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]

    source_config = config["validation_config"]["source_config"]

    hostname = source_config["host"].lower()
    dbname = source_config["database"].lower()
    portno = source_config["port"]
    username = source_config["user"].lower()
    password = source_config["password"]
    if password.startswith(constants.SECRET_PREFIX):
        password = Variable.get(password.removeprefix(constants.SECRET_PREFIX))

    source_schema = config["migrationTask"]["translationConfigDetails"][
        "nameMappingList"
    ]["name_map"][0]["source"]["schema"]

    translation_source_path = config["migrationTask"]["translationConfigDetails"][
        "gcsSourcePath"
    ]
    bucket, folder = gcs_util.parse_bucket_and_blob_from_path(translation_source_path)

    op_type = config["type"].lower()
    if op_type == "sql" or op_type == "dml":
        folder = folder + "/" + METADATA_FOLDER

    conn = redshift_connector.connect(
        host=hostname, database=dbname, port=portno, user=username, password=password
    )

    cursor = conn.cursor()

    cursor.execute(
        f"select tablename from pg_tables where schemaname='{source_schema}' and tableowner='{username}'"
    )

    # Retrieve the query result set
    result: tuple = cursor.fetchall()

    redshift_ddl_extraction_result = []
    for table in result:
        exec_time = datetime.utcnow()

        cursor.execute(f"show table {source_schema}.{table[0]}")

        table_ddl: tuple = cursor.fetchall()
        logging.info(f"DDL extraction for table - {str(table_ddl[0][0])}")

        filename = source_schema + "_" + table[0] + ".sql"

        object_name = folder + "/" + filename
        gcs_util.write_object_in_gcsbucket(
            bucket, object_name, str.encode(table_ddl[0][0])
        )

        redshift_ddl_extraction_result.append(
            {
                "unique_id": config["unique_id"],
                "source_db": "Redshift",
                "database": dbname,
                "schema": source_schema,
                "table_name": table[0],
                "file_path": f"gs://{bucket}/{object_name}",
                "extraction_time": str(exec_time),
            }
        )

    logging.info(f"Lentgth of records - {len(redshift_ddl_extraction_result)}")

    if len(redshift_ddl_extraction_result) > 0:
        insert_result = bigquery.Client().insert_rows_json(
            DDL_EXTRACTION_TABLE, redshift_ddl_extraction_result
        )
        logging.info(f"extraction insertion result: {insert_result}")

        logging.info(
            f"Redshift DDL are stored in source path {translation_source_path}"
        )

    ti.xcom_push(key="config", value=json.dumps(kwargs["dag_run"].conf["config"]))


def build_redshift_ddl_extraction_group(dag: DAG) -> TaskGroup:
    redshift_extraction_taskgroup = TaskGroup(group_id="redshift_extraction_taskgroup")

    extract_redshift_ddl = PythonOperator(
        task_id="extract_redshift_ddl",
        python_callable=_extract_redshift_ddl,
        task_group=redshift_extraction_taskgroup,
        dag=dag,
    )

    invoke_translation_dag = TriggerDagRunOperator(
        task_id="invoke_batch_sql_translation",
        trigger_dag_id="batch_sql_translation",
        conf={
            "config": "{{ ti.xcom_pull(task_ids='redshift_extraction_taskgroup.extract_redshift_ddl', key='config') }}",
        },
        task_group=redshift_extraction_taskgroup,
        dag=dag,
    )

    extract_redshift_ddl >> invoke_translation_dag

    return redshift_extraction_taskgroup
