# Copyright 2022 Google LLC
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

import datetime
import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_composer import \
    CloudComposerGetEnvironmentOperator

with models.DAG(
    "workload_identity_creator_dag",
    schedule_interval="@once",
    default_args={"start_date": datetime.datetime(2022, 1, 1), "retries": 1},
) as dag:
    get_env = CloudComposerGetEnvironmentOperator(
        task_id="get_env",
        project_id=os.environ.get("GCP_PROJECT_ID"),
        region=os.environ.get("REGION"),
        environment_id=os.environ.get("COMPOSER_ENV"),
        dag=dag,
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="common_utils/bash_operator_scripts/setup_workload_identity.sh",
        params={
            "composer_k8s_cluster_region": os.environ.get("REGION"),
            "project_id": os.environ.get("GCP_PROJECT_ID"),
            "wi_sa_name": "sa-k8s",
            "iam_svc_account": os.environ.get("COMPOSER_SA"),
        },
        dag=dag,
    )

    get_env >> bash_task
