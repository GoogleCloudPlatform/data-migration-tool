import json
import os
from http import HTTPStatus

import pytest

from translation.event_listener import composer, errors, main, routing

MOCK_CONFIGS_DIR_NAME = "mock"


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


def test_handler_no_body(client):
    r = client.post("/")

    assert r.status_code == HTTPStatus.BAD_REQUEST


def test_handler_method_not_allowed(client):
    r = client.get("/")

    assert r.status_code == HTTPStatus.METHOD_NOT_ALLOWED


def test_handler_invalid_body(client):
    r = client.post("/", json="invalid message")

    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json == errors.INVALID_MESSAGE_FORMAT[0]


def test_handler_no_subscription_field(client):
    r = client.post("/", json={"message": "hello world"})

    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json == errors.SUBSCRIPTION_AND_MESSAGE_REQUIRED[0]


def test_handler_no_message_field(client):
    r = client.post("/", json={"subscription": "sample"})

    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json == errors.SUBSCRIPTION_AND_MESSAGE_REQUIRED[0]


def test_handler_no_rule_found(client):
    r = client.post(
        "/",
        json={"subscription": "sample", "message": {"attributes": "attributes values"}},
    )

    assert r.status_code == HTTPStatus.OK
    assert r.json == errors.NO_ROUTING_RULE_FOUND[0]


def test_handler_file_notification_rule_found(client, mocker):
    mock_dag_id = "sample_dag"
    mock_composer_env_url = "host.example/path"
    f = open(
        os.path.join(
            os.path.dirname(__file__), MOCK_CONFIGS_DIR_NAME, "file_notification.json"
        ),
        "r",
    )
    mock_envelope = {"subscription": "sample", "message": json.load(f)}
    expected_config = {
        "message": {
            "attributes": {
                "bucketId": "foo",
                "eventType": "OBJECT_FINALIZE",
                "objectId": "data/bar.json",
                "payloadFormat": "JSON_API_V1",
            },
            "otherFields": {},
        },
        "subscription": "sample",
    }

    mocker.patch(
        "translation.event_listener.routing.get_dag_id", return_value=mock_dag_id
    )
    mocker.patch(
        "translation.event_listener.routing.get_composer_env_url",
        return_value=mock_composer_env_url,
    )
    mocker.patch(
        "translation.event_listener.composer.run_dag", return_value=HTTPStatus.OK
    )

    r = client.post("/", json=mock_envelope)

    routing.get_dag_id.assert_called_once_with(mock_envelope["subscription"])
    routing.get_composer_env_url.assert_called_once_with()
    composer.run_dag.assert_called_once_with(
        mock_composer_env_url, mock_dag_id, expected_config
    )
    assert r.status_code == HTTPStatus.OK


def test_handler_transfer_notification_rule_found(client, mocker):
    mock_dag_id = "sample_dag"
    mock_composer_env_url = "host.example/path"
    f = open(
        os.path.join(
            os.path.dirname(__file__),
            MOCK_CONFIGS_DIR_NAME,
            "transfer_notification.json",
        ),
        "r",
    )
    mock_envelope = {"subscription": "sample", "message": json.load(f)}
    expected_config = {
        "message": {
            "attributes": {
                "eventType": "TRANSFER_RUN_FINISHED",
                "payloadFormat": "JSON_API_V1",
            },
            "data": "ewogICJkYXRhU291cmNlSWQiOiAib25fcHJlbWlzZXMiLAogICJkZXN0aW5hdGlvbkRhdGFzZXRJZCI6ICJ0ZDJicV90ZXN0MSIsCiAgImVtYWlsUHJlZmVyZW5jZXMiOiB7CiAgCiAgfSwKICAiZW5kVGltZSI6ICIyMDIyLTA5LTI3VDE2OjQxOjM1LjQ4NjE1NFoiLAogICJlcnJvclN0YXR1cyI6IHsKICAKICB9LAogICJuYW1lIjogInByb2plY3RzLzMzOTMyNzkyNDk3MS9sb2NhdGlvbnMvYXNpYS1zb3V0aDEvdHJhbnNmZXJDb25maWdzLzYzNTJiMjZmLTAwMDAtMjM1NC1iYzZkLTAwMWExMTQzODc0Mi9ydW5zLzYzMmY5MjAwLTAwMDAtMjlmMy04ODBiLWY0MDMwNDM5NDY5OCIsCiAgIm5vdGlmaWNhdGlvblB1YnN1YlRvcGljIjogInByb2plY3RzL2RhdGEtcG9jLTEyMzQvdG9waWNzL3RkMmJxX3Rlc3QxIiwKICAicGFyYW1zIjogewogICAgImFnZW50X3NlcnZpY2VfYWNjb3VudCI6ICIzMzkzMjc5MjQ5NzEtY29tcHV0ZUBkZXZlbG9wZXIuZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgICAiYnVja2V0IjogInRkMmJxX3Rlc3QxIiwKICAgICJkYXRhYmFzZV9uYW1lIjogImR2dCIsCiAgICAiZGF0YWJhc2VfdHlwZSI6ICJUZXJhZGF0YSIsCiAgICAic2NoZW1hX2ZpbGVfcGF0aCI6ICJnczovL3RkMmJxX3Rlc3QxLzMzOTMyNzkyNDk3MS9zY2hlbWEvc2NoZW1hcy5qc29uIiwKICAgICJ0YWJsZV9uYW1lX3BhdHRlcm5zIjogImRlcHRfaW5jciIKICB9LAogICJydW5UaW1lIjogIjIwMjItMDktMjdUMTY6Mzk6MDIuMTQ5WiIsCiAgInNjaGVkdWxlVGltZSI6ICIyMDIyLTA5LTI3VDE2OjM5OjA1LjE3ODE4M1oiLAogICJzdGFydFRpbWUiOiAiMjAyMi0wOS0yN1QxNjozOTowOC44MTcxNTlaIiwKICAic3RhdGUiOiAiU1VDQ0VFREVEIiwKICAidXBkYXRlVGltZSI6ICIyMDIyLTA5LTI3VDE2OjQxOjM1LjQ4NjE2OVoiLAogICJ1c2VySWQiOiAiNjY5MjgyMjY5NTgyNjExNjM0MSIKfQo=",
            "messageId": "5706411067730710",
            "message_id": "5706411067730710",
            "publishTime": "2022-09-27T16:41:35.921Z",
            "publish_time": "2022-09-27T16:41:35.921Z",
        },
        "subscription": "sample",
    }
    mocker.patch(
        "translation.event_listener.routing.get_dag_id", return_value=mock_dag_id
    )
    mocker.patch(
        "translation.event_listener.routing.get_composer_env_url",
        return_value=mock_composer_env_url,
    )
    mocker.patch(
        "translation.event_listener.composer.run_dag", return_value=HTTPStatus.OK
    )

    r = client.post("/", json=mock_envelope)

    routing.get_dag_id.assert_called_once_with(mock_envelope["subscription"])
    routing.get_composer_env_url.assert_called_once_with()
    composer.run_dag.assert_called_once_with(
        mock_composer_env_url, mock_dag_id, expected_config
    )
    assert r.status_code == HTTPStatus.OK
