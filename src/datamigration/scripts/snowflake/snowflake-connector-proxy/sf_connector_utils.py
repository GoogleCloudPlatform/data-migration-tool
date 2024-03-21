from http import HTTPStatus

import requests
from requests.exceptions import HTTPError

_SAVE_OAUTH_VALUES_PATH = "connector/save-oauth-values"
_MIGRATE_DATA_PATH = "connector/migrate-data"


class SfConnectorUtils:
    """
    Util to communicate with Snowflake Connector APIs

    Args:
        host (str): Snowflake Connector Host
    """

    def __init__(self, host, port=None):
        self.host = host.strip("/")
        self.port = port
        if self.port:
            self.base_endpoint = f"{self.host}:{self.port}"
        else:
            self.base_endpoint = self.host

    def save_oauth_values(self, client_id, client_secret, refresh_token):
        """
        Instantiates snowflake oauth credentials

        Args:
            client_id (str): OAUTH_CLIENT_ID of Snowflake Security Integration

            client_secret (str): OAUTH_CLIENT_SECRET of Snowflake Security Integration

            refresh_token (str): OAUTH_REFRESH_TOKEN generated via Snowflake Security Integeration
        """
        params = {
            "clientId": client_id,
            "clientSecret": client_secret,
            "refreshToken": refresh_token,
        }
        response = requests.post(
            f"{self.base_endpoint}/{_SAVE_OAUTH_VALUES_PATH}", json=params
        )
        if response.status_code != HTTPStatus.OK:
            raise HTTPError(response.text)

    def migrate_data(self, params):
        """
        Initiates snowflake to bigquery migration

        Args:
            params (dict): Request payload for migrate-data API

        Returns:
            Array of json where each json is denoting status of each table migration
        """
        response = requests.post(
            f"{self.base_endpoint}/{_MIGRATE_DATA_PATH}", json=params
        )
        if response.status_code != HTTPStatus.OK:
            raise HTTPError(response.text)
        return response.json()
