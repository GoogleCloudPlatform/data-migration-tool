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
  exit 0
fi

{
date
echo "Transfer ID: $transfer_id"

agent_pid=$(ps aux | grep java | grep $transfer_id | awk '{print $2}')
if [ -z "$agent_pid" ]
then
      echo "Agent not running"
else
      echo "Agent PID: $agent_pid"
      ps $agent_pid
      kill -9 $agent_pid
      echo "Killed agent"
fi
date
echo "---"
} &>> $transfer_run_dir/kill.log
