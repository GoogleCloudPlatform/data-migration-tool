import re
import sys
import uuid
import json
import ast

from pathlib import Path
from typing import Any, List

from google.api_core.exceptions import AlreadyExists
from google.api_core.extended_operation import ExtendedOperation
from google.cloud import compute_v1, secretmanager_v1, storage
from googleapiclient.discovery import build
from jinja2 import Environment, FileSystemLoader

from test_utils import parse_config

import time

def load_test_config(config_file=None, config=None):
    env = Environment(loader=FileSystemLoader("config"))
    config_template = env.get_template(config_file)
    print(config_file)

    rendered_config = config_template.render(
        project_id=PROJECT_ID,
        unique_id=uuid.uuid4(),
        bucket_name=TRANSLATION_BUCKET_NAME,
        config_bucket_name=CONFIG_BUCKET_NAME,
        source_ip=config['SOURCE_IP'] if 'SOURCE_IP' in config else "",
        source_schema=config['SOURCE_SCHEMA'] if 'SOURCE_SCHEMA' in config else "",
        target_schema=config['TARGET_SCHEMA'] if 'TARGET_SCHEMA' in config else "",
        source_username=config['SOURCE_USERNAME'] if 'SOURCE_USERNAME' in config else "",
        secret_name=config['SECRET_NAME'] if 'SECRET_NAME' in config else "",
        source_dbname=config['SOURCE_DBNAME'] if 'SOURCE_DBNAME' in config else "",
        validation_mode=config['VALIDATION_MODE'] if 'VALIDATION_MODE' in config else "",
        validation_type=config['VALIDATION_TYPE'] if 'VALIDATION_TYPE' in config else "",
        validation_mapping_file=config['VALIDATION_MAPPING_FILE'] if 'VALIDATION_MAPPING_FILE' in config else "",
        data_mig_table_list_file=config['DATA_MIG_TABLE_LIST_FILE'] if 'DATA_MIG_TABLE_LIST_FILE' in config else "",
        data_bucket_name=DATA_FILE_BUCKET_NAME
    )

    print(config)

    config_file_name = (config_file.split('.')[0]+'_' + config['VALIDATION_MODE'] + '.' + config_file.split('.')[1]) if (config['VALIDATION_MODE'] is not None) else config_file
    print(config_file_name)
    storage_client.bucket(CONFIG_BUCKET_NAME).blob(config_file_name).upload_from_string(
        rendered_config
    )
    print(rendered_config)


def load_test_data():
    print("Loading test data")
    bucket = storage_client.bucket(TRANSLATION_BUCKET_NAME)
    for sql_file in Path(".").glob("**/*.sql"):
        bucket.blob(str(sql_file)).upload_from_filename(sql_file)

    data_file_bucket = storage_client.bucket(DATA_FILE_BUCKET_NAME)
    for csv in Path(".").glob("**/*.csv"):
        data_file_bucket.blob(str(csv)).upload_from_filename(csv)

    config_bucket = storage_client.bucket(CONFIG_BUCKET_NAME)
    for xlsx in Path(".").glob("**/*.xlsx"):
        config_bucket.blob(str(xlsx)).upload_from_filename(xlsx)


def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
) -> Any:
    result = operation.result(timeout=timeout)

    if operation.error_code:
        print(
            f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
            file=sys.stderr,
            flush=True,
        )
        print(f"Operation ID: {operation.name}", file=sys.stderr, flush=True)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        print(f"Warnings during {verbose_name}:\n", file=sys.stderr, flush=True)
        for warning in operation.warnings:
            print(f" - {warning.code}: {warning.message}", file=sys.stderr, flush=True)

    return result


def create_instance(
    project_id: str,
    zone: str,
    instance_name: str,
    disks: List[compute_v1.AttachedDisk],
    machine_type: str = "n1-standard-4",
    network_link: str = "global/networks/default",
    subnetwork_link: str = None,
    internal_ip: str = None,
    external_access: bool = False,
    external_ipv4: str = None,
) -> compute_v1.Instance:
    instance_client = compute_v1.InstancesClient()

    network_interface = compute_v1.NetworkInterface()
    network_interface.name = network_link
    if subnetwork_link:
        network_interface.subnetwork = subnetwork_link

    if internal_ip:
        network_interface.network_i_p = internal_ip

    if external_access:
        access = compute_v1.AccessConfig()
        access.type_ = compute_v1.AccessConfig.Type.ONE_TO_ONE_NAT.name
        access.name = "External NAT"
        access.network_tier = access.NetworkTier.PREMIUM.name
        if external_ipv4:
            access.nat_i_p = external_ipv4
        network_interface.access_configs = [access]

    # Collect information into the Instance object.
    instance = compute_v1.Instance()
    instance.network_interfaces = [network_interface]
    instance.name = instance_name
    instance.disks = disks
    if re.match(r"^zones/[a-z\d\-]+/machineTypes/[a-z\d\-]+$", machine_type):
        instance.machine_type = machine_type
    else:
        instance.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

    # Prepare the request to insert an instance.
    request = compute_v1.InsertInstanceRequest()
    request.zone = zone
    request.project = project_id
    request.instance_resource = instance

    # Wait for the create operation to complete.
    print(f"Creating the {instance_name} instance in {zone}...")

    instance_client.insert(request=request)
    print(f"Instance {instance_name} created.")
    return instance_client.get(project=project_id, zone=zone, instance=instance_name)


def create_teradata_vm():
    instance_name = "teradata"
    instance_zone = "us-central1-c"

    image_client = compute_v1.ImagesClient()
    teradata_image = image_client.get(project="data-analytics-pocs", image="teradata")
    boot_disk = compute_v1.AttachedDisk()
    initialize_params = compute_v1.AttachedDiskInitializeParams()
    initialize_params.source_image = teradata_image.self_link
    initialize_params.disk_size_gb = teradata_image.disk_size_gb
    initialize_params.disk_type = f"zones/{instance_zone}/diskTypes/pd-standard"
    boot_disk.initialize_params = initialize_params
    boot_disk.auto_delete = True
    boot_disk.boot = True
    disks = [boot_disk]
    create_instance(storage_client.project, instance_zone, instance_name, disks)


def setup_env():
    add_secret_with_version(PROJECT_ID, "secret-edw_credentials", "dbc")
    create_teradata_vm()


def add_secret_with_version(project_id, secret_id, payload):
    client = secretmanager_v1.SecretManagerServiceClient()
    try:
        client.create_secret(
            parent=f"projects/{project_id}",
            secret_id=secret_id,
            secret={"replication": {"automatic": {}}},
        )
    except AlreadyExists:
        pass
    parent = client.secret_path(project_id, secret_id)
    client.add_secret_version(parent=parent, payload={"data": bytes(payload, "utf-8")})


def delete_transfer_configs():
    bq_data_transfer_client = build("bigquerydatatransfer", "v1")
    transferConfigs = (
        bq_data_transfer_client.projects()
        .transferConfigs()
        .list(parent=f"projects/{PROJECT_ID}", pageSize=1000)
        .execute()
        .get("transferConfigs")
    )
    if transferConfigs:
        for transferConfig in transferConfigs:
            response = (
                bq_data_transfer_client.projects()
                .locations()
                .transferConfigs()
                .delete(
                    name=transferConfig.get("name"),
                )
                .execute()
            )
            return response


# Run the following to setup project
"""
gcloud config set project YOUR_PROJECT_ID &&
bash iam-setup.sh &&
gcloud builds submit . --config cloudbuild_deploy.yaml --substitutions _DATA_SOURCE="teradata"
"""

cfg = parse_config('input.properties')

PROJECT_ID = cfg.get('inputs', 'PROJECT_ID')
CONFIG_BUCKET_NAME = cfg.get('inputs', 'CONFIG_BUCKET_NAME')
TRANSLATION_BUCKET_NAME = cfg.get('inputs', 'TRANSLATION_BUCKET_NAME')
DATA_FILE_BUCKET_NAME = cfg.get('inputs', 'DATA_FILE_BUCKET_NAME')

storage_client = storage.Client(project=PROJECT_ID)

# setup_env()
load_test_data()

print("Upload DDL config files")
ddl_config_list = cfg.get('inputs', 'DDL_CONFIG_LIST')
print(ddl_config_list)
ddl_config_list_obj = json.loads(ddl_config_list)
print(ddl_config_list_obj)
for ddl_config_file in ddl_config_list_obj:
    for ddl_config_item in ddl_config_list_obj[ddl_config_file]:
        load_test_config(ddl_config_file, ddl_config_item)
        time.sleep(120) # Delay for 2 minute (120 seconds).  

print("Delete data transfer configs")
delete_transfer_configs()

print("Upload data migration config files")
data_config_list = cfg.get('inputs', 'DATA_CONFIG_LIST')

data_config_list_obj = json.loads(data_config_list)

for data_config_file in data_config_list_obj:
    for data_config_item in data_config_list_obj[data_config_file]:
        load_test_config(data_config_file, data_config_item)  # First set up tables via DDLs
        time.sleep(120) # Delay for 2 minute (120 seconds). 

# time.sleep(1200) # Delay for 20 minute (1200 seconds) for data-migration completion
print("Upload SQL config files")
sql_config_list = cfg.get('inputs', 'SQL_CONFIG_LIST')

sql_config_list_obj = json.loads(sql_config_list)

for sql_config_file in sql_config_list_obj:
    for sql_config_item in sql_config_list_obj[sql_config_file]:
        load_test_config(sql_config_file, sql_config_item)  # First set up tables via DDLs
        time.sleep(120) # Delay for 2 minute (120 seconds).

print("Finished...")