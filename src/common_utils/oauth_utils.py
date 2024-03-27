import google.auth.transport.requests
import google.oauth2.id_token


def get_id_token(audience_url):
    """
    Fetch the ID Token from the current environment.

     Args:
        audience (str): The audience that this ID token is intended for.

    Returns:
        str: The ID token.

    Raises:
        ~google.auth.exceptions.DefaultCredentialsError:
            If metadata server doesn't exist and no valid service account
            credentials are found.
    """
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience_url)

    return id_token
