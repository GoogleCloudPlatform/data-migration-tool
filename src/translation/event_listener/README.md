## About

This app handles Cloud Storage notifications coming from a PubSub push subscription and triggers a DAG whenever a new message arrives.
Currently, it can be configured using environment variables to set the Composer environment API endpoint and Composer DAG ID based on subscription.
Depending on the `eventType` of the message, a transformation might be applied and sent as the configuration to the DAG run (see `transformation.py` to know which transformation is applied to each `eventType`).
See [Configuration](#configuration) for more details on how to setup the service.

## Configuration
Service is configured using the following environment variables:

- `COMPOSER_ENV_URL`: URL of the composer environment.
- `DAG_ID_MAPPING`: mapping between subscription and DAG to run (see the following section for more info).
- `DEFAULT_DAG_ID`: DAG to run by default when no mapping was found for a subscription.

### Subscription-DAG mapping logic
The service can be configured to route messages from different subscriptions to different dag_ids based on subscription_id. To do this, the mapping between subscription and dag_id should be defined in the `DAG_ID_MAPPING` environment variable as a json. For example:
```json
{
  "subscription_id_1": "dag_1",
  "subscription_id_2": "dag_2"
}
```
With this configuration, messages coming from `subscription_id_1` subscription will run `dag_1` and
messages coming from `subscription_id_2` subscription will run `dag_2`. Messages coming from any other
subscriptions will use the default `dag_id` if defined, otherwise they will be dropped.


## Code structure

The code is splitted in the following files:

  - `main.py`: contains the handler that extracts the message from the request and runs the DAG.
  - `routing.py`: contains routing rule loading/lookup logic based on subscription id
  - `composer.py`: contains helper functions to run the Composer DAG.
  - `errors.py`: contains response errors constants.
  - `transformation.py`: contains transformations and lookup logic based on `eventType` of the message.

## Run tests

After installing dependencies, run tests with the following command:
```bash
python -m pytest
```

## Build image

To build Docker image, run the following command:

```bash
gcloud builds submit --region=${GCP_REGION} --tag gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME} .
```


## Deploy image to Cloud Run

To deploy the built image to Cloud Run, run the following command:

```bash
gcloud run deploy ${CLOUD_RUN_SERVICE} --image gcr.io/${GCP_PROJECT_ID}/${IMAGE_NAME}
```