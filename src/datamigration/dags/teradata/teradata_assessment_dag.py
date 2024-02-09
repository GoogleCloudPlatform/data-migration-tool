"""
Airflow DAG for creating the table_list.csv file from BQ Assessment.
"""
from __future__ import annotations

from datetime import datetime
from google.cloud import bigquery
from airflow import models
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

TD_ASSESSMENT_TABLE="TableInfo"
TD_ASSESSMENT_QUERY_LOGS_TABLE = "QueryLogs"
DMT_CONFIG_TABLE_NAME="table_list.csv"

default_dag_args = {"start_date": datetime(2022, 1, 1)}

# The the params in the UI to trigger the dag
# {
#     "project_id": "assessment-dmt-integration",
#     "dataset": "assessment_dmt",
#     "bucket": "dvt-afleisc"
# }

def upload_tables_to_gcs(project_id, dataset, bucket):
    """ Queries the assessment dataset and uploads the table list as a CSV to GCS """
    client = bigquery.Client(project=project_id)
    sql = f"SELECT TableNameOriginal FROM `{project_id}.{dataset}.{TD_ASSESSMENT_TABLE}`"
    tables = client.query(sql).to_dataframe().to_csv()
    GCSHook().upload(
        bucket_name=bucket,
        object_name=DMT_CONFIG_TABLE_NAME,
        data=tables
    )

def upload_ddls(project_id, dataset, bucket):
    """ Queries the assessment dataset for source DDLs and uploads SQLs to GCS """
    client = bigquery.Client(project=project_id)
    sql = f"SELECT QueryId, QueryText FROM `{project_id}.{dataset}.{TD_ASSESSMENT_QUERY_LOGS_TABLE}` WHERE QueryType = 'DDL'"
    ddls = client.query(sql).to_dataframe()
    for index, row in ddls.iterrows():
        GCSHook().upload(
        bucket_name=bucket,
        object_name="ddl/"+row['QueryId']+'.sql',
        data=row['QueryText']
        )

with models.DAG(
"teradata_asessment",
schedule_interval=None,
default_args=default_dag_args,
params={
        "project_id": Param("defaut", type="string"),
        "dataset": Param("default", type="string"),
        "bucket": Param("default", type="string")
    },
catchup=False,
) as dag:
    generate_table_list = PythonOperator(
    task_id="generate_table_list",
    python_callable=upload_tables_to_gcs,
    op_kwargs={"project_id": "{{ params.project_id }}", "dataset": "{{ params.dataset }}", "bucket": "{{ params.bucket }}"},
    )
    generate_ddl_files = PythonOperator(
        task_id="generate_ddl_files",
        python_callable=upload_ddls,
        op_kwargs={"project_id": "{{ params.project_id }}", "dataset": "{{ params.dataset }}", "bucket": "{{ params.bucket }}"}
    )
    generate_table_list << generate_ddl_files


