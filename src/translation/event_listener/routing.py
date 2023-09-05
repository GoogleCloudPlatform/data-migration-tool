import json
import os

composer_env_url = os.environ.get("COMPOSER_ENV_URL")

# setup subscription to dag id mapping
dag_id_mapping_json = os.environ.get("DAG_ID_MAPPING")
default_dag_id = os.environ.get("DEFAULT_DAG_ID")

dag_id_mapping = json.loads(dag_id_mapping_json) if dag_id_mapping_json else {}


def get_dag_id(subscription):
    return (
        dag_id_mapping[subscription]
        if subscription in dag_id_mapping
        else default_dag_id
    )


def get_composer_env_url():
    return composer_env_url
