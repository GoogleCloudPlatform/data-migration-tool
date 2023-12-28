import ast
import logging

import datamigration_utils.constants as constants
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils import timezone
from google.cloud import bigquery

from common_utils import parallelization_utils


def read_config_file(translation_config):
    """
    Set required variables as dictionary variables from translation config
    """
    config_dict = {}
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
    config_dict["validation_mode"] = translation_config["validation_config"][
        "validation_mode"
    ]
    config_dict["batch_distribution"] = translation_config["batchDistribution"]
    config_dict["hive_ddl_metadata"] = "hive_ddl_metadata"
    config_dict["bq_dataset_audit"] = "dmt_logs"
    config_dict["bq_load_audit"] = "hive_bqload_audit"
    config_dict["dvt_results"] = "dmt_dvt_results"
    config_dict["schema_results_tbl"] = "dmt_schema_results"
    return config_dict


def get_dvt_table_list(dict):
    query = constants.dvt_query_table_list.format(
        hive_db_name=dict["hive_db_name"],
        bq_dataset_name=dict["bq_dataset"],
        bq_dataset_audit=dict["bq_dataset_audit"],
        bq_load_audit=dict["bq_load_audit"],
    )
    print(query)
    client = bigquery.Client()
    return client.query(query).to_dataframe()["tablename"].values.tolist()


def get_validation_dag_id(validation_mode):
    validation_gke_type = "gke"
    validation_crun_type = "cloudrun"
    validation_default_type = validation_gke_type
    validation_type_to_dag_id_mapping = {
        validation_crun_type: "validation_crun_dag",
        validation_gke_type: "validation_dag",
    }
    if validation_mode in validation_type_to_dag_id_mapping:
        return validation_type_to_dag_id_mapping[validation_mode]
    else:
        return validation_type_to_dag_id_mapping[validation_default_type]


def invoke_dvt_dag(config):
    dict = read_config_file(ast.literal_eval(config))
    run_id_prefix = timezone.utcnow()
    validation_mode = dict["validation_mode"]
    validation_dag_id = get_validation_dag_id(validation_mode)
    batch_size = int(dict["batch_distribution"])
    table_list = get_dvt_table_list(dict)
    table_list = str(table_list)
    table_list = table_list.strip("][").split(", ")
    print("Table List:", table_list)
    config = ast.literal_eval(config)
    for batch_run_id, batch in parallelization_utils.make_run_batches(
        table_list, batch_size, run_id_prefix
    ):
        if table_list == []:
            logging.info(f"empty table list, skipping validation for {batch}")
            continue
        conf = {"config": config, "files": batch, "table_list": batch}
        trigger_dag(
            dag_id=validation_dag_id,
            run_id=batch_run_id,
            replace_microseconds=False,
            conf=conf,
        )
        logging.info(
            f"invoked {validation_dag_id} (run_id: {batch_run_id}) for files {batch}"
        )
