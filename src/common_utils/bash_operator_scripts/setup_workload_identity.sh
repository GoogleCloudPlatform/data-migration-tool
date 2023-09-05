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

gcloud config set container/use_application_default_credentials TRUE &&

export KUBECONFIG=/tmp/composer_kube_config &&

gcloud container clusters get-credentials \
  {{ ti.xcom_pull(task_ids='get_env')['config']['gke_cluster'].split('/')[-1] }} \
  --region {{ params.composer_k8s_cluster_region }} \
  --project {{ params.project_id }} &&

echo -e "apiVersion: v1 \nkind: ServiceAccount \nmetadata: \n  name: sa-k8s \n  namespace: default" | kubectl apply -f -  &&

gcloud iam service-accounts add-iam-policy-binding \
  {{params.iam_svc_account}}@{{params.project_id}}.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member='serviceAccount:{{params.project_id}}.svc.id.goog[default/{{params.wi_sa_name}}]' \
  --verbosity=debug \
  --project {{params.project_id}} &&

kubectl annotate serviceaccount {{params.wi_sa_name}} \
  --namespace default iam.gke.io/gcp-service-account={{params.iam_svc_account}}@{{params.project_id}}.iam.gserviceaccount.com \
  --overwrite
