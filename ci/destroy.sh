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

TRANSLATION_DIR=terraform/translation/
DATAMIGRATION_HIVE_DIR=terraform/datamigration/hive
DATAMIGRATION_TERADATA_DIR=terraform/datamigration/teradata
DATAMIGRATION_REDSHIFT_DIR=terraform/datamigration/redshift
DATAMIGRATION_FLAT_FILES_DIR=terraform/datamigration/flat_files

#######################################
# Runs the terragrunt command to destroy
# all terraform for a given working dir.
# Globals:
#   None
# Arguments:
#   working_dir
# Returns:
#   None
#######################################
function terragrunt_destroy() {
  local working_dir=$1

  terragrunt run-all destroy \
    --terragrunt-non-interactive \
    --terragrunt-working-dir="${working_dir}"
}

#######################################
# Main entry-point for execution
# Globals:
#   TRANSLATION_DIR
#   DATAMIGRATION_HIVE_DIR
#   DATAMIGRATION_TERADATA_DIR
#   _DATA_SOURCE
# Arguments:
#   None
# Returns:
#   None
#######################################
function main() {
  source ci/dependencies.sh

  if [[ -n "${_TERRAGRUNT_WORKING_DIR}" ]]; then
    printf "Deploying user specified directory: %s" "${_TERRAGRUNT_WORKING_DIR}"
    terragrunt_destroy "${_TERRAGRUNT_WORKING_DIR}"
  else
    if [[ "${_DATA_SOURCE}" = "hive" ]]; then
      terragrunt_destroy "${DATAMIGRATION_HIVE_DIR}"
    elif [[ "${_DATA_SOURCE}" = "teradata" ]]; then
      terragrunt_destroy "${DATAMIGRATION_TERADATA_DIR}"
    elif [[ "${_DATA_SOURCE}" = "redshift" ]]; then
      terragrunt_destroy "${DATAMIGRATION_REDSHIFT_DIR}"
    elif [[ "${_DATA_SOURCE}" = "flat_files" ]]; then
      terragrunt_destroy "${DATAMIGRATION_FLAT_FILES_DIR}"
    fi
    terragrunt_destroy "${TRANSLATION_DIR}"
  fi
}

main
