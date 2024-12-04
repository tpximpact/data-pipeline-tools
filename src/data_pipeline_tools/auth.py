"""Authentication-related functions."""

import json

from google.auth.credentials import Credentials
from google.cloud import secretmanager
from google.oauth2 import service_account


def harvest_headers(project_id: str, service: str) -> dict[str, str]:
    """Create headers for a request to the Harvest API.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the secret.
        service (str): The name of the service to use.

    Returns:
    -------
        dict[str, str]: The headers.

    """
    return {
        "Authorization": f"Bearer {access_secret_version(project_id, 'HARVEST_ACCESS_TOKEN')}",
        "Harvest-Account-ID": access_secret_version(project_id, "HARVEST_ACCOUNT_ID"),
        "service": service,
        "Content-Type": "application/json",
    }


def hibob_headers(project_id: str, service: str) -> dict[str, str]:
    """Create headers for a request to the Hibob API.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the secret.
        service (str): The name of the service to use.

    Returns:
    -------
        dict[str, str]: The headers.

    """
    return {
        "accept": "application/json",
        "Authorization": access_secret_version(project_id, "BOB_ACCESS_TOKEN"),
        "service": service,
    }


def pipedrive_access_token(project_id: str) -> str:
    """Access the Pipedrive access token from the Secret Manager service.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the secret.

    Returns:
    -------
        str: The Pipedrive access token.

    """
    return access_secret_version(project_id, "PIPEDRIVE_ACCESS_TOKEN")


def access_secret_version(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Access a secret version from the Secret Manager service.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the secret.
        secret_id (str): The ID of the secret to access.
        version_id (str): The version of the secret to access. Defaults to "latest".

    Returns:
    -------
        str: The secret payload.

    """
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}")
    return response.payload.data.decode("UTF-8")


def get_google_credentials(project_id: str) -> Credentials:
    """Load the credentials from the service account JSON file.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the secret.

    Returns:
    -------
        Credentials: The credentials.

    """
    return service_account.Credentials.from_service_account_info(
        json.loads(access_secret_version(project_id, "SERVICE_ACCOUNT_JSON")),
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
