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

#/*****************************************************************************
## Startup script to install dependencies in the compute instance                                            
#******************************************************************************/

cd opt || exit
rm -r migration_project_teradata_bq
mkdir migration_project_teradata_bq
cd migration_project_teradata_bq || exit

{
echo "Installing dependencies - Script Started *****"

echo "Migration Agent -"
wget -P . https://storage.googleapis.com/data_transfer_agent/latest/mirroring-agent.jar
echo ""

echo "java -"
apt update
apt-get install -y default-jdk
echo ""

script_bucket=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/scripts-bucket" -H "Metadata-Flavor: Google")
project_id=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/project-id" -H "Metadata-Flavor: Google")
controller_sub=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/attributes/controller-sub" -H "Metadata-Flavor: Google")

gsutil -m cp -r gs://$script_bucket/scripts/datamigration/teradata/* .

echo "Wrapper Scripts to Start/Kill Migration Agent - "
chmod 775 ./agent_controller/scripts/start_agent.sh
chmod 775 ./agent_controller/scripts/kill_agent.sh
echo ""

echo "Agent Controller setup - "
sudo apt --assume-yes install python3-pip
pip3 install -r ./agent_controller/requirements.txt
echo -e "project_id: $project_id\nsubscription_id: $controller_sub" > ./agent_controller/config/config.yaml
sudo cp ./agent_controller/agent-controller.service /lib/systemd/system
sudo systemctl daemon-reload
sudo systemctl enable agent-controller
sudo systemctl start agent-controller
sudo systemctl status agent-controller
echo ""

echo "Mounting disk- "
#Format the new partition with the ext4 file system
mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
#Creating new folder in /opt/migration_project_teradata_bq/
mkdir -p local_processing_space
#Mount the disk on new folder
mount -o discard,defaults /dev/sdb local_processing_space
#Assigning permission to new folder
chmod a+w local_processing_space
#Making an entry in fstab to mount an disk in case of restart
echo UUID="$(blkid -s UUID -o value /dev/sdb)" /opt/migration_project_teradata_bq/local_processing_space ext4 discard,defaults 0 2 | tee -a /etc/fstab
echo ""

echo "Assigning permissions - "
chmod 744 startup.log
chown -R root:root ../migration_project_teradata_bq

echo "Installing dependencies - Script Ended ******" 
} > startup.log 2>&1