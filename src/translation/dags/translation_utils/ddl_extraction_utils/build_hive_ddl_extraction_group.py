import ast
import json
import random
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

batch_id = (
    "hive2bq-"
    + str(random.randint(1, 999999))
    + "-"
    + str(datetime.now().strftime("%Y%m%d%H%M%S"))
)
SPARK_BIGQUERY_JAR_FILE = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
BATCH_TRANSLATOR_DAG_ID = "batch_sql_translation"
INTEGRATION_DAG_ID = "integration_dag"
EXTRACT_DDL_HIVE_DAG_ID = "extract_hive_ddl_dag"


def _set_required_vars(ti, **kwargs):
    """
    TODO
    get bucket id from kwargs and then push as env variable
    Check parallelization_dag for reference
    """

    # Read the entire config as a response
    translation_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    ti.xcom_push(
        key="next_dag_data", value=json.dumps(kwargs["dag_run"].conf["config"])
    )
    bq_dataset_audit = "dmt_logs"
    region = translation_config["hive_config"]["transfer-configuration"]["region"]
    Variable.set("region", region)
    Variable.set(
        "input_ddl_bucket",
        translation_config["migrationTask"]["translationConfigDetails"]["gcsSourcePath"]
        .split("//")[1]
        .split("/")[0],
    )
    Variable.set(
        "input_ddl_path",
        translation_config["migrationTask"]["translationConfigDetails"]["gcsSourcePath"]
        .split("//")[1]
        .split("/", 1)[1]
        + "/global_typeconvert.config.yaml",
    )
    ti.xcom_push(key="gcs_temp_bucket", value=translation_config["gcs_temp_bucket"])
    nm_map_list = translation_config["migrationTask"]["translationConfigDetails"][
        "nameMappingList"
    ]["name_map"]
    hive_db = list(set(d["source"]["schema"] for d in nm_map_list))[0]
    ti.xcom_push(key="hive_db", value=hive_db)
    hive_ddl_tbl = "{proj_id}.{dataset}.{tbl}".format(
        proj_id=translation_config["hive_config"]["transfer-configuration"][
            "project_id"
        ],
        dataset=bq_dataset_audit,
        tbl="hive_ddl_metadata",
    )
    print("hive_ddl_tbl: " + hive_ddl_tbl)
    ti.xcom_push(key="hive_ddl_tbl", value=hive_ddl_tbl)


def _next_task(ti, **kwargs):
    translation_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    flg = translation_config["extract_ddl"].upper()
    print(flg)
    return (
        "hive_extraction_taskgroup.extract_hive_ddls"
        if flg == "YES"
        else "hive_extraction_taskgroup.load_hive_ddl_metadata"
    )


def build_hive_ddl_extraction_group(dag: DAG) -> TaskGroup:
    hive_extraction_taskgroup = TaskGroup(group_id="hive_extraction_taskgroup")

    set_required_variables = PythonOperator(
        task_id="set_required_vars",
        python_callable=_set_required_vars,
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    copy_global_config_file = GCSToGCSOperator(
        task_id="copy_global_config_file",
        source_bucket=Variable.get("hive_config_bucket_id", default_var=" "),
        source_object="scripts/translation/hive/global_typeconvert.config.yaml",
        destination_bucket=Variable.get("input_ddl_bucket", default_var=""),
        destination_object=Variable.get("input_ddl_path", default_var=""),
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=_next_task,
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    load_hive_ddl_metadata = GCSToBigQueryOperator(
        task_id="load_hive_ddl_metadata",
        bucket="{{ ti.xcom_pull(key='gcs_temp_bucket')}}",
        source_objects="files/{{ti.xcom_pull(key='hive_db')}}.csv",
        destination_project_dataset_table="{{ti.xcom_pull(key='hive_ddl_tbl')}}",
        source_format="CSV",
        field_delimiter="\t",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        schema_fields=[
            {"name": "run_id", "type": "INTEGER"},
            {"name": "start_time", "type": "TIMESTAMP"},
            {"name": "database", "type": "STRING"},
            {"name": "table", "type": "STRING"},
            {"name": "field_delimiter", "type": "STRING"},
            {"name": "partition_flag", "type": "STRING"},
            {"name": "cluster_flag", "type": "STRING"},
            {"name": "format", "type": "STRING"},
            {"name": "ddl_extracted", "type": "STRING"},
        ],
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    extract_hive_ddls = DataprocCreateBatchOperator(
        task_id="extract_hive_ddls",
        region=Variable.get("region", default_var=0),
        batch_id=batch_id,
        # Variable.get("temp_gcs_bucket_id",default_var=0)
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "gs://"
                + Variable.get("hive_config_bucket_id", default_var=" ")
                + "/scripts/translation/hive/extract_hive_ddls.py",
                "args": [
                    "--build_config",
                    "{{ ti.xcom_pull(key='next_dag_data', task_ids='hive_extraction_taskgroup.set_required_vars') }}",
                ],
            }
        },
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    invoke_batch_translator_dag = TriggerDagRunOperator(
        task_id="invoke_batch_translator_dag_task",
        trigger_dag_id=BATCH_TRANSLATOR_DAG_ID,
        conf={
            "config": "{{ ti.xcom_pull(key='next_dag_data', task_ids='hive_extraction_taskgroup.set_required_vars') }}"
        },
        trigger_rule="one_success",
        task_group=hive_extraction_taskgroup,
        dag=dag,
    )

    (
        set_required_variables
        >> copy_global_config_file
        >> branch_task
        >> [load_hive_ddl_metadata, extract_hive_ddls]
        >> invoke_batch_translator_dag
    )

    return hive_extraction_taskgroup
