# ruff: noqa: T201
"""Utility functions."""

import pandas as pd
import pandas_gbq
import requests
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from .auth import get_google_credentials


def read_from_bigquery(project_id: str, query: str) -> pd.DataFrame:
    """Read data from a BigQuery table and return it as a Pandas DataFrame.

    Args:
    ----
        project_id (str): The ID of the Google Cloud project that contains the BigQuery table.
        query (str): The SQL query to execute on the BigQuery table.

    Returns:
    -------
        pd.DataFrame: A Pandas DataFrame containing the data from the BigQuery table.

    """
    return pandas_gbq.read_gbq(query, project_id=project_id, credentials=get_google_credentials(project_id))


def write_to_bigquery(config: dict[str:str], df: pd.DataFrame, write_disposition: str) -> None:
    """Write a Pandas DataFrame to a BigQuery table.

    Args:
    ----
        config (dict): A dictionary containing the following keys:
            - dataset_id
            - table_name
            - location
        df (pd.DataFrame): The DataFrame to write to BigQuery.
        write_disposition (str): The write disposition to use when writing the DataFrame to BigQuery.

    """
    client = bigquery.Client(location=config["location"])

    # Get a reference to the BigQuery table to write to.
    dataset_ref = client.dataset(config["dataset_id"])
    table_ref = dataset_ref.table(config["table_name"])

    # Set up the job configuration with the specified write disposition.
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)

    try:
        # Write the DataFrame to BigQuery using the specified configuration.
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
    except BadRequest as e:
        print("Error writing DataFrame to BigQuery:", str(e))
        return

    print(f"Loaded {job.output_rows} rows into {config['dataset_id']}.{config['table_name']}")


def find_and_flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Find all columns that have a dictionary as a value in the first row of the DataFrame.

    Args:
    ----
        df (pd.DataFrame): The DataFrame to find the nested columns in.

    Returns:
    -------
        pd.DataFrame: The DataFrame with the nested columns flattened.

    """
    return flatten_columns(df, [column_name for column_name, value in df.iloc[0].items() if isinstance(value, dict)])


def flatten_columns(df: pd.DataFrame, nested_columns: list) -> pd.DataFrame:
    """Flatten the nested columns in a DataFrame.

    Args:
    ----
        df (pd.DataFrame): The DataFrame to flatten the nested columns in.
        nested_columns (list): The names of the nested columns to flatten.

    """
    for column in nested_columns:
        # Convert the column values to dictionaries if they are integers.
        df[column] = df[column].apply(lambda x: {"value": x} if isinstance(x, float | int | list | str) else x)
        # Convert the column values to dictionaries if they are None.
        df[column] = df[column].apply(lambda x: x if x is not None else {})

        print(f"Flattening column: {column}")
        flattened_df = pd.json_normalize(df[column], max_level=1).add_prefix(f"{column}_")

        # Add the flattened columns to the DataFrame and drop the original nested column.
        flat_df = pd.concat([df, flattened_df], axis=1)
        flat_df = flat_df.drop(column, axis=1)

    return flat_df


def get_harvest_pages(url: str, headers: dict) -> tuple[int, int]:
    """Get the total number of pages and total number of entries from a Harvest API endpoint.

    Args:
    ----
        url (str): The URL to get the total number of pages and total number of entries from.
        headers (dict): The headers to use for the request.

    Returns:
    -------
        tuple[int, int]: A tuple containing the total number of pages and total number of entries.

    """
    url = f"{url}1"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json()

        return data["total_pages"], data["total_entries"]
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Error retrieving total pages: {e}")
        return None, None
