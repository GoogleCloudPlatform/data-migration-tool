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

set -o pipefail

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
fi

BASEDIR=$(dirname $0)
source $BASEDIR/config.sh

transfer_id=$1

transfer_run_dir=$TRANSFER_RUN_BASE_DIR/$transfer_id
if [ ! -d "$transfer_run_dir" ]; then
  echo "Agent not initialized : $transfer_run_dir does not exist"
  exit 1
fi

{
date
echo "Transfer ID: $transfer_id"

mem_free=$(free | grep Mem | awk '{print $4/$2 * 100.0}' | cut -d . -f 1)
while [ $mem_free -lt 30 ]
do
   date
   echo "Memory Free: $mem_free%"
   echo "Sleeping 60 seconds"
   sleep 60
   mem_free=$(free | grep Mem | awk '{print $4/$2 * 100.0}' | cut -d . -f 1)
done

echo "Memory Free: $mem_free%"
echo "Starting agent"
cd $transfer_run_dir
agent_cmd="nohup java -cp $TERAJDBC_JAR:$AGENT_JAR com.google.cloud.bigquery.dms.Agent --configuration-file=$transfer_run_dir/$transfer_id.json &> $transfer_run_dir/$transfer_id.out &"
echo $agent_cmd
eval $agent_cmd
echo "Agent Started"
date
echo "---"
} &>> $transfer_run_dir/start.log
