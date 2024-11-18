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

# Set up required permissions for the Cloud Build service account to be able to deploy DMT infrastructure using Terraform.

PROJECT_ID=$(gcloud config get project)

if [[ -z "${BUILD_ACCOUNT}" ]]; then
  BUILD_ACCOUNT=$(gcloud builds get-default-service-account --format="value(serviceAccountEmail)" | awk -F"/" '{print $NF}')
fi

gcloud services enable \
  serviceusage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  cloudbuild.googleapis.com

roles=(
  "roles/artifactregistry.admin"
  "roles/bigquery.admin"
  "roles/cloudbuild.builds.builder"
  "roles/composer.admin"
  "roles/compute.instanceAdmin.v1"
  "roles/compute.networkAdmin"
  "roles/compute.securityAdmin"
  "roles/container.viewer"
  "roles/iam.serviceAccountCreator"
  "roles/iam.serviceAccountUser"
  "roles/iam.serviceAccountDeleter"
  "roles/logging.viewer"
  "roles/logging.logWriter"
  "roles/pubsub.admin"
  "roles/resourcemanager.projectIamAdmin"
  "roles/run.admin"
  "roles/secretmanager.admin"
  "roles/serviceusage.serviceUsageAdmin"
  "roles/storage.admin"
)

for role in "${roles[@]}" ; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${BUILD_ACCOUNT}" \
    --role="${role}"
done
