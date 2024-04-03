from http import HTTPStatus

import requests
from requests.exceptions import HTTPError, JSONDecodeError

from common_utils.oauth_utils import get_id_token

_MIGRATE_DATA_PATH = "migrate-data"
_EXTRACT_DDL_PATH = "extract-ddl"


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
            params (dict): Request payload for connector's ./migrate-data API

        Returns:
            Array of json where each json is denoting status of each table migration or Response text
        """
        headers = {"Authorization": f"Bearer {get_id_token(self.host)}"}
        response = requests.post(
            f"{self.host}/{_MIGRATE_DATA_PATH}", json=params, headers=headers
        )
        if response.status_code != HTTPStatus.OK:
            raise HTTPError(response.text)
        try:
            return response.json()
        except JSONDecodeError:
            return response.text

    def extract_ddl(self, params):
        """
        Initiates snowflake to bigquery ddl extraction and translation

        Args:
            params (dict): Request payload for connector's ./extract-ddl API

        Returns:
            Response text
        """
        headers = {"Authorization": f"Bearer {get_id_token(self.host)}"}
        response = requests.post(
            f"{self.host}/{_EXTRACT_DDL_PATH}", json=params, headers=headers
        )
        if response.status_code != HTTPStatus.OK:
            raise HTTPError(response.text)
        try:
            return response.json()
        except JSONDecodeError:
            return response.text
