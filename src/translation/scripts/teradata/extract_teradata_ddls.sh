#!/bin/bash
#extract_teradata_ddls.sh

# exit when any command fails
set -e

# Airflow working directory to generate terradata metadata
WORK_DIR="/home/airflow/genmetadata"

# DATA folder path
DATA_FOLDER_PATH="/home/airflow/gcs/data"

# Variables for Bash argument storage
CONNECTION_STRING_ARGS=$1
METADATA_STORAGE_PATH=$2
SOURCE_DB=$3
FOLDER_NAME=$4

# DWH Library version
DWH_LIB_VERSION="1.0.62"

# Check if dwh lib folder is not found, unzip directory
if [ ! -d "$WORK_DIR/dwh-migration-tools-v$DWH_LIB_VERSION" ];
then
    echo "$WORK_DIR/dwh-migration-tools-v$DWH_LIB_VERSION not found"

    cd $WORK_DIR

    echo "Download dwh-migration-tools-v$DWH_LIB_VERSION.zip"
    wget https://github.com/google/dwh-migration-tools/releases/download/v$DWH_LIB_VERSION/dwh-migration-tools-v$DWH_LIB_VERSION.zip

    unzip dwh-migration-tools-v$DWH_LIB_VERSION.zip

    rm dwh-migration-tools-v$DWH_LIB_VERSION.zip
    echo "$WORK_DIR is created and download completed"
else
    echo "$WORK_DIR/dwh-migration-tools-v$DWH_LIB_VERSION is found"
fi

# Go to DWH library bin path
cd $WORK_DIR/dwh-migration-tools-v$DWH_LIB_VERSION/bin

# Prepare dwh dumper arguments string based on bash argument
connectionStrArr=(${CONNECTION_STRING_ARGS//,/ })
connectionStr=''
for item in "${connectionStrArr[@]}"; do
    keyval=(${item//:/ })
    connectionStr+=' --'${keyval[0]}' '${keyval[1]}
done

# Execute dwh dumper
echo "Execute dwh dumper to generate metadata"
./dwh-migration-dumper $connectionStr
# Example command
# ./dwh-migration-dumper --connector <source_db> --host <IP> --user <Username> --password <Password> --database <SchemaName> --schema <SchemaName> --driver $DAGS_FOLDER/dependencies/teradata_extraction/terajdbc4.jar

# Check file is exist or not
outputFile=dwh-migration-${SOURCE_DB}-metadata.zip
if test -f "$outputFile";
then
    echo "$outputFile is generated."
    # Unzip generated folder
    unzip dwh-migration-${SOURCE_DB}-metadata.zip -d dwh-migration-${SOURCE_DB}-metadata

    # Copy metadata to composer bucket data folder
    mkdir -p $DATA_FOLDER_PATH/$FOLDER_NAME
    cp -R dwh-migration-${SOURCE_DB}-metadata/* $DATA_FOLDER_PATH/$FOLDER_NAME

    # Remove generated zip from working directory
    rm dwh-migration-${SOURCE_DB}-metadata.zip
    rm -r dwh-migration-${SOURCE_DB}-metadata
else
    echo "$outputFile is generated."
fi
echo "Metadata is generated successfully"
