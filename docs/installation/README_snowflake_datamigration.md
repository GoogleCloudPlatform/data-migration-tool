# [DRAFT] [WIP]

# [TODO]: Schema translation, and Data validation is yet to integerated

# Data Migration Tool User Guide - Data Transfer
* For Data Migration Tool introduction, please refer to [/README.md](/README.md)
* For Installation user guide, please refer to [/docs/installation/README.md](/docs/installation/README.md)

### Tool Features

* Initial release will support bulk load data transfer only. 
* Initial release will support Snowflake to BigQuery translation using BQ DTS, row and column validation of the data in source and target.

### Things to Take Note of


* If tables from <span style="text-decoration:underline;">multiple schemas</span> are to be migrated, create **separate config files** for each source schema.
* Current tool version supports only bulk load which is ideally done only once, note that the transfer shall take place by the process following upload of the config files.
* If there are <span style="text-decoration:underline;">new table additions to a batch/config file</span>, ideally create **a new config file** with a new table list even if rest details remain the same. If not followed, there might be multiple data transfer configs responsible for migrating the same tables.
* Note that reuploading the same configuration file or using a different config file for the same dataset and tables, will cause failures.
* **Note:** Snowflake connector proxy is a temporary stopgap solution until snowflake connector is released with run-time setup feature

### Prepare configuration file

The user uploads a configuration json file to **dmt-config-&lt;project-id-or-customer-name-given-in-deployment>**  bucket **data** folder which initiates data migration.

As the configuration is uploaded, a new file create/update trigger is sent to pub/sub which triggers the DAG **controller_dag**.

### Prerequisites 


* Target dataset creation to be done by the user/concerned team before uploading the configuration file.
* Snowflake Account URL 
* Snowflake JDBC URL
* Snowflake Warehouse name (which has access to required tables/schemas to migrate)
* Snowflake Storage Integeration which access to staging gcs buckets you wish to use for migration ([Steps](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration))
* Snowflake file format (will be used for unloading data from snowflake to gcs) ([Steps](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
* Snowflake Oauth Client ID, Client Secret and Refresh Token ([Steps](https://community.snowflake.com/s/article/HOW-TO-OAUTH-TOKEN-GENERATION-USING-SNOWFLAKE-CUSTOM-OAUTH))


### Data Migration Tool Deployment - Snowflake Data Migration

Data Migration deployment will take care of the workflow needed to perform


* Initial data load
* Data Validation using [DVT tool](https://github.com/GoogleCloudPlatform/professional-services-data-validator)
* Options to run DVT on cloud run and GKE airflow pod operator depending on scaling requirements.

Ensure that you have followed the steps to deploy Data Migration Tool architecture as mentioned in the [readme](/README.md) with the option of `_DATA_SOURCE=snowflake`

If you had previously only deployed translation architecture - you could run a data architecture deployment for snowflake migration with the below command provided the Terraform State files in remote GCS bucket from translation deployment are intact.


```
gcloud builds submit . --project ${SOURCE_PROJECT} \
--config cloudbuild_deploy.yaml \
--substitutions \
_DATA_SOURCE=snowflake
```


* Check for success messages in your Cloud Build Logs for deployment of
    * Google Cloud Storage
    * Cloud Pub/Sub
    * Cloud Composer
    * Cloud Run
    * Big Query


## Trigger Data Migration Tool 


* Data Migration 
* Data Validations
* SQL Validations

#### Sample configuration file

```
{
    "unique_id": "sf_test_migrate-1",
    "type": "data",
    "source": "snowflake",
    "table_list_file": "gs://<bucket>/<table_list_file_path>",
    "oauth_config": {
      "clientId": "secret:<CLIENT_ID_secret_name>",
      "clientSecret": "secret:<CLIENT_SECRET_secret_name>",
      "refreshToken": "secret:<REFRESH_TOKEN_secret_name>"
    },
    "setup_config": {
        "jdbcUrl": "jdbc:snowflake://<ACCOUNT_URL>",
        "accountUrl": "<ACCOUNT_URL>",
        "warehouse": "<WAREHOUSE>",
        "storageIntegration": "<STORAGE_INTEGRATION>"
    },
    "migration_config": {
        "sourceDatabaseName": "<SOURCE_DATABASE>",
        "sourceSchemaName": "<SOURCE_SCHEMA>",
        "targetDatabaseName": "<GCP_PROJECT_ID>",
        "targetSchemaName": "<BIGQUERY_DATASET>",
        "schema": false,
        "bqTableExists": false,
        "gcsBucketForDDLs": "<GCS_BUCKET>",
        "gcsBucketForTranslation": "<GCS_BUCKET>",
        "location": "us",
        "snowflakeStageLocation": "<GCS_BUCKET>/data-unload",
        "snowflakeFileFormatValue": "SF_GCS_CSV_FORMAT_DMT",
        "bqLoadFileFormat": "CSV"
    }
}
```

[Data Transfer sample config file location](samples/configs/snowflake)

  
#### Field Descriptions


<table>
  <tr>
   <td><strong>JSON attribute</strong>
   </td>
   <td><strong>Description</strong>
   </td>
  </tr>
  <tr>
   <td>
    unique_id
   </td>
   <td>Provide an unique name for identifying the data migration 
<p>
<strong>–</strong>
<p>
<strong>Note: </strong> If the user opted for data migration, along with schema migration through the tool, this unique id should be the same as the one used in the schema migration config file.
   </td>
  </tr>
  <tr>
   <td>
    type
   </td>
   <td>Type of migration : data
   </td>
  </tr>
  <tr>
   <td>
    source
   </td>
   <td>Source datawarehouse : snowflake
   </td>
  </tr>
  <tr>
   <td>
    table_list_file
   </td>
   <td>File uploaded in GCS bucket same as the one used for uploading config file. This file should provide table names to be migrated from a particular database.
<p>
<strong>Note: table_list_file</strong> key only needs to be provided in case the user chooses to opt only for data migration through the tool (without schema translation). 
<p>
   </td>
  </tr>
  <tr>
   <td>
    oauth_config
   </td>
   <td>Sub json config to be used for authenticating to snowflake.
   </td>
  </tr>
  <tr>
   <td>
    oauth_config:clientId
   </td>
   <td>Client Id generated as a part of pre-requisities<br>
      This can be plain text or Secret reference from Secret Manager
      <strong>secret: &lt;secret key name without `secret-` prefix></strong>
      For example - `secret:sf_client_id` , In this case secret name in Secret Manager should be: `secret-sf_client_id`
   </td>
  </tr>
  <tr>
   <td>
    oauth_config:clientSecret
   </td>
   <td>Client Secret generated as a part of pre-requisities<br>
      This can be plain text or Secret reference from Secret Manager
      <strong>secret: &lt;secret key name without `secret-` prefix></strong>
      For example - `secret:sf_client_secret` , In this case secret name in Secret Manager should be: `secret-sf_client_secret`
   </td>
  </tr>
  <tr>
   <td>
    oauth_config:refreshToken
   </td>
   <td>Refresh Token generated as a part of pre-requisities<br>
      This can be plain text or Secret reference from Secret Manager
      <strong>secret: &lt;secret key name without `secret-` prefix></strong>
      For example - `secret:refresh_token` , In this case secret name in Secret Manager should be: `secret-refresh_token`
   </td>
  </tr>
  
  <tr>
   <td>
    setup_config
   </td>
   <td>Sub json config to be used for initializing snwoflake migration job</td>
  </tr>
  <tr>
   <td>
    setup_config:jdbcUrl
   </td>
   <td>Snowflake JDBC url
   Format: jdbc:snowflake://&lt;ACCOUNT_URL&gt;
   </td>
  </tr>
  <tr>
   <td>
    setup_config:accountUrl
   </td>
   <td>Snowflake url which gets prepended to rest API requests for fetching data.
   </td>
  </tr>
  <tr>
   <td>
    setup_config:warehouse
   </td>
   <td>Snowflake warehouse name to use for executing all SQLs (Unloading and DDL extractions) in source 
   </td>
  </tr>
  <tr>
   <td>
    setup_config:storageIntegration
   </td>
   <td>Storage Integration name create as part of pre-requisities
   </td>
  </tr>
  
  <tr>
   <td>
    migration_config
   </td>
   <td>Sub json config to be used for each migration job
   </td>
  </tr>
  <tr>
   <td>
    migration_config:sourceDatabaseName
   </td>
      <td>Storage Integration name create as part of pre-requisities</td>
  </tr>
  <tr>
   <td>
    migration_config:sourceSchemaName
   </td>
   <td>Name of source schema  where the table to be migrated exists.</td>
  </tr>
  <tr>
   <td>
    migration_config:targetDatabaseName
   </td>
   <td>Name of target database where the target table would get created.</td>
  </tr>
  <tr>
   <td>
    migration_config:targetSchemaName
   </td>
   <td>Name of target schema where the target table would get created. In BigQuery it will be the dataset name</td>
  </tr>
  <tr>
   <td>
    migration_config:schema
   </td>
   <td>This is a boolean flag with the values true or false. True means that the connector will migrate all the tables in the schema, while false means that the connector will only migrate the table(s) specified in the table_list_file.
   </td>
  </tr>
  <tr>
   <td>
    migration_config:bqTableExists
   </td>
   <td>This is a boolean flag that can be either true or false. True means that the BigQuery (target) table already exists and the connector does not need to create it. False means that the connector should create a table using the translated DDls. Note that if the table exists in BigQuery and the value of this parameter is false, the connector will not override it and will throw an error.
   </td>
  </tr>
 <tr>
   <td>
    migration_config:gcsBucketForDDLs
   </td>
   <td>
   GCS bucket path (without gs:// prefix) where extracted DDLs will get saved.
   </td>
  </tr>
  <tr>
   <td>
    migration_config:location
   </td>
   <td>
   Location to be used for running translation job and bigquery load
   </td>
  </tr>
  <tr>
   <td>
    migration_config:snowflakeStageLocation
   </td>
   <td>This is the location in GCS where data unloaded from Snowflake will be saved. It should be created in advance because it will provide access to the service account created by Snowflake when we create "STORAGE INTEGRATION"
   </td>
  </tr>
  <tr>
   <td>
    migration_config:snowflakeFileFormatValue
   </td>
   <td>
This is the file format created in Snowflakes which will be used while unloading the data from Snowflakes to GCS in copy-into command. It could be CSV, Parquert and created in advance.
   </td>
  </tr>
  <tr>
   <td>
    migration_config:bqLoadFileFormat
   </td>
   <td>This is the file format which gets used by BigQuery load jobs while loading the GCS file data in BigQuery. Currently the CSV and Parquet format are supported.
   </td>
  </tr>
</table>


**unique_id:** name to uniquely identify the batches or DTS for this config file. 

_<span style="text-decoration:underline;">Note</span>_ if the user opted for data migration, along with schema migration through the tool, this unique id should be the same as the one used in the schema migration config file.

**table_list_file:** file uploaded in GCS bucket same as the one used for uploading config file. This file should provide table names to be migrated from a particular schema in newlines. 
Eg: 
- DEPARTMENT
- EMPLOYEE
- SALARY
- HR_RECORDS


<span style="text-decoration:underline;">Note</span> that this key only needs to be provided in case the user chooses to opt only for data migration through the tool (without schema translation). As such, tables structure is created by Snowflake Connector itself rather than the CompilerWorks schema migration feature from the tool. 

**_Tables in the CSV file should always be in the same case as how they exist in source Snowflake and ultimately match the contents of CSV/Excel file uploaded as validation parameters file in GCS._**

### Validation data in BigQuery

Composer DAG validation_dag will validate the data migrated to BQ. Results for DVT can be viewed in ``<PROJECT_ID>.dmt_logs.dmt_dvt_results``

Below query can be used to see the results:
```
SELECT * FROM `<project-id>.dmt_logs.dmt_dvt_results` where
-- validation_type='Column'
-- run_id = <transfer_run_id>
target_table_name = <target_table_name>
```

There are two ways to validate data - column or row. Validation_type given as column verifies count aggregation on the source and target tables. Validation_type given as row verifies hash of the rows based on the primary key. Hence for row validations, providing primary_key in Validation_config json is necessary.

Check the field descriptions section for detailed information on the keys in the configuration file.