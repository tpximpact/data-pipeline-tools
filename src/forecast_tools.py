"""Tools for interacting with the Forecast API client."""

import forecast

from .auth import access_secret_version


def forecast_client(project_id: str) -> forecast.Api:
    """Create Forecast API client for the given project.

    Args:
    ----
        project_id: The ID of the project to create the client for.

    Returns:
    -------
        A Forecast API client.

    """
    return forecast.Api(
        account_id=access_secret_version(project_id, "FORECAST_ACCOUNT_ID"),
        auth_token=access_secret_version(project_id, "FORECAST_ACCESS_TOKEN"),
    )
