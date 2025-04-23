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

TERRAFORM_VERSION=1.*
TERRAGRUNT_VERSION=0.77.22

#######################################
# Installs Terraform according to website:
# https://developer.hashicorp.com/terraform/downloads
# Globals:
#   TERRAFORM_VERSION
# Arguments:
#   None
# Returns:
#   None
#######################################
install_terraform() {
  curl https://apt.releases.hashicorp.com/gpg | gpg --dearmor | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" |
    tee /etc/apt/sources.list.d/hashicorp.list
  apt-get -qqy update
  apt-get -qqy install terraform=${TERRAFORM_VERSION}
  terraform -v
}

#######################################
# Installs Terragrunt according to website:
# https://terragrunt.gruntwork.io/docs/getting-started/install/#download-from-releases-page
# Globals:
#   TERRAGRUNT_VERSION
# Arguments:
#   None
# Returns:
#   None
#######################################
install_terragrunt() {
  apt-get -qqy install wget
  wget "https://github.com/gruntwork-io/terragrunt/releases/download/v${TERRAGRUNT_VERSION}/terragrunt_linux_amd64" -O terragrunt
  chmod u+x terragrunt
  mv terragrunt /usr/local/bin/terragrunt
  terragrunt -v
}

install_terraform
install_terragrunt
