from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
    authed_session = AuthorizedSession(credentials)
    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    return authed_session.request(method, url, **kwargs)


def run_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}
    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        print("error running dag: Check Airflow RBAC roles for your account.")
    elif response.status_code != 200:
        print(f"error running dag: {response.status_code} {response.text}")

    return response.status_code
