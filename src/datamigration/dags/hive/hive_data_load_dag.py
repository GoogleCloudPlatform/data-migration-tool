# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ast
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datamigration_utils.hive_bq_load_utils import (
    get_hive_tables,
    get_partition_clustering_info,
    get_text_format_schema,
    load_bq_tables,
)
from datamigration_utils.hive_dvt_check_utils import invoke_dvt_dag

from common_utils.operators.reporting_operator import ReportingOperator


def set_required_vars(ti, **kwargs):
    """
    TODO
    get bucket id from kwargs and then push as env variable
    Check parallelization_dag for reference
    """
    data_load_config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    Variable.set("data_load_config", data_load_config)
    validation_mode = data_load_config["validation_config"]["validation_mode"]
    Variable.set("validation_mode", validation_mode)
    batch_distribution = data_load_config["batchDistribution"]
    Variable.set("batch_distribution", batch_distribution)
    Variable.set("op_load_dtm", datetime.now())

    ti.xcom_push(key="user_config", value=data_load_config)


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hive_data_load_dag",
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:
    set_required_vars = PythonOperator(
        task_id="set_required_vars",
        python_callable=set_required_vars,
    )

    get_hive_tables = PythonOperator(
        task_id="get_hive_tables",
        python_callable=get_hive_tables,
        op_kwargs={"config": Variable.get("data_load_config", default_var="")},
    )

    get_text_format_schema = PythonOperator(
        task_id="get_text_format_schema",
        python_callable=get_text_format_schema,
        op_kwargs={"config": Variable.get("data_load_config", default_var="")},
    )

    get_partition_clustering_info = PythonOperator(
        task_id="get_partition_clustering_info",
        python_callable=get_partition_clustering_info,
        op_kwargs={"config": Variable.get("data_load_config", default_var="")},
    )

    load_bq_tables = PythonOperator.partial(
        task_id="load_bq_tables",
        python_callable=load_bq_tables,
        max_active_tis_per_dag=int(Variable.get("batch_distribution", default_var=10)),
        op_kwargs={
            "config": Variable.get("data_load_config", default_var=""),
            "op_load_dtm": Variable.get("op_load_dtm", default_var=""),
        },
    ).expand(op_args=get_partition_clustering_info.output)

    invoke_dvt_validation_dag = PythonOperator(
        task_id="invoke_dvt_dag",
        python_callable=invoke_dvt_dag,
        op_kwargs={"config": Variable.get("data_load_config", default_var="")},
    )
    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(key='user_config', task_ids='set_required_vars') }}",
        dag=dag,
    )
    (
        set_required_vars
        >> get_hive_tables
        >> get_text_format_schema
        >> get_partition_clustering_info
        >> load_bq_tables
        >> invoke_dvt_validation_dag
        >> dag_report
    )
