import ast
import csv
import json
import logging
import os
import shutil
import uuid
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from translation_utils import csv_utils

from common_utils import constants, storage_utils

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# Script bucket name and folder name to copy in data directory
CONFIG_BUCKET_NAME = os.environ.get("CONFIG_BUCKET_NAME")
SCRIPT_FOLDER = "scripts/translation/teradata"
SOFTWARE_FOLDER = "software/teradata"
LIB_FOLDER = "lib"

# Data folder path, script folder path and bash file path
DATA_FOLDER = "/home/airflow/gcs/data"
CP_SCRIPT_FOLDER = "/home/airflow/genmetadata"
BASH_FILE = "extract_teradata_ddls.sh"
BASH_FILE_LOCAL_PATH = f"{CP_SCRIPT_FOLDER}/{BASH_FILE}"
JDBC_JAR_FILE = "terajdbc4.jar"
JDBC_JAR_FILE_LOCAL_PATH = f"{CP_SCRIPT_FOLDER}/{JDBC_JAR_FILE}"

# Storage utils object
gcs_util = storage_utils.StorageUtils()

# DDL Extraction result table
DDL_EXTRACTION_TABLE = f"{PROJECT_ID}.dmt_logs.dmt_extract_ddl_results"

METADATA_FOLDER = "ddl"


def _check_teradata_jdbc_jar_present():
    jar_status = gcs_util.check_object_exist_in_bucket(
        CONFIG_BUCKET_NAME, f"{SOFTWARE_FOLDER}/{JDBC_JAR_FILE}"
    )
    if jar_status is True:
        logging.info(
            f"Teradata JAR {JDBC_JAR_FILE} is present in gs://{CONFIG_BUCKET_NAME}/{SOFTWARE_FOLDER} location"
        )
    else:
        logging.error(
            f"Error[Missing teradata JDBC]: Please upload teradata JDBC Jar({JDBC_JAR_FILE}) in the GCS location - gs://{CONFIG_BUCKET_NAME}/{SOFTWARE_FOLDER}/{JDBC_JAR_FILE}"
        )
        raise AirflowFailException(
            f"Error[Missing teradata JDBC]: Please upload teradata JDBC Jar({JDBC_JAR_FILE}) in the GCS location - gs://{CONFIG_BUCKET_NAME}/{SOFTWARE_FOLDER}/{JDBC_JAR_FILE}"
        )


def _download_files():
    if not os.path.exists(CP_SCRIPT_FOLDER):
        logging.info(f"{CP_SCRIPT_FOLDER} directory does not exist")
        os.makedirs(CP_SCRIPT_FOLDER)

    logging.info(f"Copying {BASH_FILE} file")
    GCSHook().download(
        bucket_name=CONFIG_BUCKET_NAME,
        object_name=str(f"{SCRIPT_FOLDER}/{BASH_FILE}"),
        filename=BASH_FILE_LOCAL_PATH,
    )

    logging.info(f"Copying {JDBC_JAR_FILE} file")
    GCSHook().download(
        bucket_name=CONFIG_BUCKET_NAME,
        object_name=str(f"{SOFTWARE_FOLDER}/{JDBC_JAR_FILE}"),
        filename=JDBC_JAR_FILE_LOCAL_PATH,
    )

    logging.info(f"file download completed {os.listdir(CP_SCRIPT_FOLDER)}")


def _prepare_arguments(ti, **kwargs) -> None:
    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]

    source_config = config["validation_config"]["source_config"]

    source_db = source_config["source_type"].lower()
    connection_args = "connector:" + source_db

    connection_args += ",host:" + source_config["host"]
    connection_args += ",user:" + source_config["user-name"]

    password = source_config["password"]
    if password.startswith(constants.SECRET_PREFIX):
        password = Variable.get(password.removeprefix(constants.SECRET_PREFIX))
    connection_args += ",password:" + password

    source_schema = config["migrationTask"]["translationConfigDetails"][
        "nameMappingList"
    ]["name_map"][0]["source"]["schema"]
    connection_args += ",database:" + source_schema
    connection_args += ",schema:" + source_schema
    connection_args += ",port:" + str(source_config["port"])
    connection_args += f",driver:{CP_SCRIPT_FOLDER}/terajdbc4.jar"

    metadata_storage_path = config["migrationTask"]["translationConfigDetails"][
        "gcsSourcePath"
    ]

    metadata_folder_name = str(uuid.uuid4())

    arg_string = (
        connection_args
        + " "
        + metadata_storage_path
        + " "
        + source_db
        + " "
        + metadata_folder_name
    )

    logging.info(
        f"Bash script arguments are prepared and metadata_folder_name is {metadata_folder_name}"
    )

    ti.xcom_push(key="arg_string", value=arg_string)
    ti.xcom_push(key="metadata_folder_name", value=metadata_folder_name)
    ti.xcom_push(key="config", value=json.dumps(kwargs["dag_run"].conf["config"]))


def _store_ddl(ti, **kwargs) -> None:
    metadata_folder_name = ti.xcom_pull(
        key="metadata_folder_name",
        task_ids="teradata_extraction_taskgroup.prepare_arguments",
    )
    logging.info(f"In store ddl : {metadata_folder_name}")

    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]

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

    csv_file_path = f"{DATA_FOLDER}/{metadata_folder_name}/dbc.TablesV.csv"

    # opening the CSV file
    ddl_file = open(csv_file_path, mode="r")

    # reading the CSV file
    csvTableVFile = csv.reader(ddl_file)

    headers = next(csvTableVFile)
    # displaying the contents of the CSV file

    td_ddl_extraction_result = []
    for lines in csvTableVFile:
        exec_time = datetime.utcnow()

        row_dict = csv_utils.row_to_dict(headers, lines)
        logging.info(f"DDL for - {row_dict['TableName']}")

        filename = row_dict["DataBaseName"] + "_" + row_dict["TableName"] + ".sql"

        object_name = folder + "/" + filename
        gcs_util.write_object_in_gcsbucket(bucket, object_name, row_dict["RequestText"])

        td_ddl_extraction_result.append(
            {
                "unique_id": config["unique_id"],
                "source_db": "Teradata",
                "database": source_schema,
                "schema": source_schema,
                "table_name": row_dict["TableName"],
                "file_path": f"gs://{bucket}/{object_name}",
                "extraction_time": str(exec_time),
            }
        )

    insert_result = bigquery.Client().insert_rows_json(
        DDL_EXTRACTION_TABLE, td_ddl_extraction_result
    )
    logging.info(f"extraction insertion result: {insert_result}")

    logging.info(f"Teradata DDL are stored in source path {translation_source_path}")


def _remove_metadata_folder(ti) -> None:
    metadata_folder_name = ti.xcom_pull(
        key="metadata_folder_name",
        task_ids="teradata_extraction_taskgroup.prepare_arguments",
    )
    metadata_folder_path = DATA_FOLDER + "/" + metadata_folder_name

    shutil.rmtree(metadata_folder_path)

    logging.info(f"Folder {metadata_folder_path} deleted.")


def build_teradata_ddl_extraction_group(dag: DAG) -> TaskGroup:
    teradata_extraction_taskgroup = TaskGroup(group_id="teradata_extraction_taskgroup")

    check_teradata_jdbc_jar_present = PythonOperator(
        task_id="check_teradata_jdbc_jar_present",
        python_callable=_check_teradata_jdbc_jar_present,
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    download_script_files = PythonOperator(
        task_id="download_script_files",
        python_callable=_download_files,
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    prepare_arguments = PythonOperator(
        task_id="prepare_arguments",
        python_callable=_prepare_arguments,
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    run_dwh_tool = BashOperator(
        task_id="run_dwh_tool",
        bash_command=f"bash {BASH_FILE_LOCAL_PATH} $arg_string",
        # bash_command="pwd",
        env={
            "arg_string": "{{ ti.xcom_pull(task_ids='teradata_extraction_taskgroup.prepare_arguments', key='arg_string') }}"
        },
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    store_ddl = PythonOperator(
        task_id="store_ddl",
        python_callable=_store_ddl,
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    invoke_translation_dag = TriggerDagRunOperator(
        task_id="invoke_batch_sql_translation",
        trigger_dag_id="batch_sql_translation",
        conf={
            "config": "{{ ti.xcom_pull(task_ids='teradata_extraction_taskgroup.prepare_arguments', key='config') }}",
        },
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    remove_metadata_folder = PythonOperator(
        task_id="remove_metadata_folder",
        python_callable=_remove_metadata_folder,
        task_group=teradata_extraction_taskgroup,
        dag=dag,
    )

    (
        check_teradata_jdbc_jar_present
        >> download_script_files
        >> prepare_arguments
        >> run_dwh_tool
        >> store_ddl
        >> [remove_metadata_folder, invoke_translation_dag]
    )

    return teradata_extraction_taskgroup
