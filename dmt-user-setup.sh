#!/bin/bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT_ID=$(gcloud config get project)

roles=(  
  "roles/bigquery.dataViewer"
  "roles/bigquery.user"
  "roles/cloudbuild.builds.editor"
  "roles/composer.user"
  "roles/compute.admin"
  "roles/iam.serviceAccountViewer"
  "roles/logging.viewer"
  "roles/run.viewer"
  "roles/serviceusage.serviceUsageConsumer"
  "roles/storage.admin"
  "roles/vpcaccess.admin"
  "projects/${PROJECT_ID}/roles/DMTUserAddtionalPermissions"
)

for role in "${roles[@]}" ; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="user:${USER_ACCOUNT}" \
    --role="${role}"
done
