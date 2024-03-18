# Snowflake connector proxy

Proxy web application for snowflake connector

**Note:** This is a temporary stopgap solution until snowflake connector is released with run-time setup feature

## Manual Setup on Local
* Copy the snowflake connector jar to [./jar](./jar) directory with name **snowflake-to-bq-data-transfer.jar**: 
`gsutil cp gs://dmt-snowflake/snowflake-to-bq-data-transfer-1.0.0.jar ./jar/snowflake-to-bq-data-transfer.jar`
* Install the dependencies: `pip install -r requirements.txt`
* Start the flask app: `python main.py` or `gunicorn --bind :8080 main:app`

## Deploy on Cloud Run
```
gcloud run deploy
```

## Routes

### ./migrate-data

Sample Payload:
```json
{
    "oauth_config": {
        "clientId": "<SNOWFLAKE_CLIENT_ID>",
        "clientSecret": "<SNOWFLAKE_CLIENT_SECRET>",
        "refreshToken": "<SNOWFLAKE_REFRESH_TOKEN>"
    },
    "setup_config": {
        "jdbcUrl": "jdbc:snowflake://<ACCOUNT_URL>",
        "accountUrl": "<ACCOUNT_URL>",
        "warehouse": "<WAREHOUSE>",
        "storageIntegration": "<STORAGE_INTEGRATION>"
    },
    "migration_config": {
        "sourceDatabaseName": "TEST_DATABASE",
        "sourceSchemaName": "public",
        "sourceTableName": "orders",
        "targetDatabaseName": "<GCP_PROJECT_ID>",
        "targetSchemaName": "<BIGQUERY_DATASET>",
        "schema": false,
        "bqTableExists": false,
        "gcsBucketForDDLs": "<GCS_BUCKET>",
        "gcsBucketForTranslation": "<GCS_BUCKET>",
        "location": "us",
        "snowflakeStageLocation": "snowflake_bq_migration/data-unload",
        "snowflakeFileFormatValue": "SF_GCS_CSV_FORMAT_DMT",
        "bqLoadFileFormat": "CSV"
    }
}
```