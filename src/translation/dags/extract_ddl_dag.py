import ast
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from translation_utils.ddl_extraction_utils.build_hive_ddl_extraction_group import (
    build_hive_ddl_extraction_group,
)
from translation_utils.ddl_extraction_utils.build_oracle_ddl_extraction_group import (
    build_oracle_ddl_extraction_group,
)
from translation_utils.ddl_extraction_utils.build_redshift_ddl_extraction_group import (
    build_redshift_ddl_extraction_group,
)
from translation_utils.ddl_extraction_utils.build_teradata_ddl_extraction_group import (
    build_teradata_ddl_extraction_group,
)

from common_utils.operators.reporting_operator import ReportingOperator

default_args = {"start_date": datetime(2018, 1, 3)}

DAG_ID = "extract_ddl_dag"


def _determine_next_taskgroup_from_source(ti, **kwargs):
    config = ast.literal_eval(kwargs["dag_run"].conf["config"])["config"]
    ti.xcom_push(key="config", value=config)

    data_source = config["source"].lower()

    logging.info(f"Group branching data_source: {data_source}")
    if data_source == "teradata":
        logging.info("Teradata")
        return "teradata_extraction_taskgroup.check_teradata_jdbc_jar_present"
    elif data_source == "hive":
        logging.info("Hive")
        return "hive_extraction_taskgroup.set_required_vars"
    elif data_source == "redshift":
        logging.info("Redshift")
        return "redshift_extraction_taskgroup.extract_redshift_ddl"
    elif data_source == "oracle":
        logging.info("Oracle")
        return "oracle_extraction_taskgroup.extract_ddl"
    else:
        logging.info(f"Error: Unsupported data source: {data_source}")
        return "end_task"


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule=None,
    render_template_as_native_obj=True,
) as dag:
    determine_next_taskgroup_from_source = BranchPythonOperator(
        task_id="determine_next_taskgroup_from_source",
        python_callable=_determine_next_taskgroup_from_source,
        dag=dag,
    )

    end_task = EmptyOperator(task_id="end_task", dag=dag)

    teradata_extraction_taskgroup = build_teradata_ddl_extraction_group(dag=dag)
    hive_extraction_taskgroup = build_hive_ddl_extraction_group(dag=dag)
    redshift_extraction_taskgroup = build_redshift_ddl_extraction_group(dag=dag)
    oracle_extraction_taskgroup = build_oracle_ddl_extraction_group(dag=dag)

    dag_report = ReportingOperator(
        task_id="dag_report",
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        configuration="{{ ti.xcom_pull(task_ids='determine_next_taskgroup_from_source', key='config') }}",
        dag=dag,
    )

    (
        determine_next_taskgroup_from_source
        >> [
            teradata_extraction_taskgroup,
            hive_extraction_taskgroup,
            redshift_extraction_taskgroup,
            oracle_extraction_taskgroup,
            end_task,
        ]
        >> dag_report
    )
