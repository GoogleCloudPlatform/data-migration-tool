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

if [[ -z "${BUILD_ACCOUNT}" ]]; then
  BUILD_ACCOUNT=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")@cloudbuild.gserviceaccount.com
fi

gcloud services enable \
  serviceusage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  cloudbuild.googleapis.com

roles=(
  "roles/bigquery.admin"
  "roles/run.admin"
  "roles/composer.admin"
  "roles/compute.instanceAdmin.v1"
  "roles/compute.networkAdmin"
  "roles/iam.serviceAccountCreator"
  "roles/logging.viewer"
  "roles/resourcemanager.projectIamAdmin"
  "roles/pubsub.admin"
  "roles/secretmanager.admin"
  "roles/iam.serviceAccountUser"
  "roles/serviceusage.serviceUsageAdmin"
  "roles/storage.admin"
  "roles/artifactregistry.admin"
)

for role in "${roles[@]}" ; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${BUILD_ACCOUNT}" \
    --role="${role}"
done
