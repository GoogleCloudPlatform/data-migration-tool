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

permissions=(
  "bigquery.datasets.get"
  "composer.environments.get"
  "composer.environments.list"
  "compute.firewalls.create"
  "compute.firewalls.get"
  "compute.firewalls.list"
  "compute.instances.create"
  "compute.instances.list"
  "compute.networks.get"
  "compute.networks.list"
  "compute.networks.updatePolicy"
  "container.clusters.get"
  "container.clusters.list"
  "iam.serviceAccounts.actAs"
  "iam.serviceAccounts.get"
  "iap.tunnelInstances.accessViaIAP"
  "resourcemanager.projects.getIamPolicy"
  "resourcemanager.projects.setIamPolicy"
  "run.services.update"
  "secretmanager.locations.list"
  "secretmanager.secrets.create"
  "secretmanager.secrets.get"
  "secretmanager.secrets.list"
  "secretmanager.versions.access"
  "secretmanager.versions.add"
  "secretmanager.versions.list"
  "serviceusage.services.enable"
  "vpcaccess.connectors.create"
  "vpcaccess.connectors.get"
  "vpcaccess.connectors.list"
  "vpcaccess.connectors.use"
)

gcloud iam roles create DMTUserAddtionalPermissions --project="${PROJECT_ID}" \
      --title=DMTUserAddtionalPermissions --description="Additional permissions required for DMT user" \
      --permissions=`echo $(echo ${permissions[@]}) | tr ' ' ','`
