from http import HTTPStatus

import requests
from requests.exceptions import HTTPError

_MIGRATE_DATA_PATH = "migrate-data"


class SfConnectorProxyUtils:
    """
    Util to communicate with Snowflake Connector APIs

    Args:
        host (str): Snowflake Connector Host
    """

    def __init__(self, host):
        self.host = host

    def migrate_data(self, params):
        """
        Initiates snowflake to bigquery migration

        Args:
            params (dict): Request payload for migrate-data API

        Returns:
            Array of json where each json is denoting status of each table migration
        """
        response = requests.post(f"{self.host}/{_MIGRATE_DATA_PATH}", json=params)
        if response.status_code != HTTPStatus.OK:
            raise HTTPError(response.text)
        return response.json()
