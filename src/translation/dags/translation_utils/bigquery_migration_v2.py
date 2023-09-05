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

import time

from googleapiclient import _auth as auth

from common_utils import discovery_util

custom_user_agent_http = auth.authorized_http(auth.default_credentials())
bq_migration_client = discovery_util.build_from_document_with_custom_http(
    "https://bigquerymigration.googleapis.com/$discovery/rest?version=v2",
    custom_user_agent_http,
)


def create_migration_workflow(project_id, tasks, location="us"):
    migration_workflow_spec = {
        "displayName": f"{project_id}-{time.time():.0f}",
        "tasks": tasks,
    }

    response = (
        bq_migration_client.projects()
        .locations()
        .workflows()
        .create(
            parent=f"projects/{project_id}/locations/{location}",
            body=migration_workflow_spec,
        )
        .execute(num_retries=10)
    )
    return response


def get_migration_workflow_state(workflow_name):
    response = (
        bq_migration_client.projects()
        .locations()
        .workflows()
        .get(name=workflow_name)
        .execute(num_retries=10)
    )
    return response


def create_single_task_migration_workflow(project_id, task):
    return create_migration_workflow(project_id, {"task": task})
