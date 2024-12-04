"""Async data fetching functions."""

import asyncio

import aiohttp
import pandas as pd


async def get_data(url: str, headers: dict[str, str]) -> dict:
    """Make a HTTP GET request to a URL with the given headers.

    Args:
    ----
        url (str): The URL to get data from.
        headers (dict[str, str]): The headers to use for the request.

    Returns:
    -------
        dict: The response data as a JSON object.

    """
    async with aiohttp.ClientSession() as session, session.get(url, headers=headers) as response:
        return await response.json()


async def get_data_for_page_range(
    url: str,
    start_page: int,
    end_page: int,
    headers: dict[str, str],
    key: str,
) -> pd.DataFrame:
    """Get data from a range of pages.

    Args:
    ----
        url (str): The URL to get data from.
        start_page (int): The start page to get data from.
        end_page (int): The end page to get data from.
        headers (dict[str, str]): The headers to use for the request.
        key (str): The key to get the required values from the response data.

    """
    tasks = []
    # async with aiohttp.ClientSession() as session:
    for i in range(start_page, end_page + 1):
        this_url = f"{url}{i}"
        task = asyncio.ensure_future(get_data(this_url, headers))
        tasks.append(task)
    results = await asyncio.gather(*tasks)

    return pd.DataFrame([item for result in results for item in result[key]])


async def get_all_data(
    url: str,
    headers: dict[str, str],
    pages: int,
    key: str,
    batch_size: int = 10,
) -> pd.DataFrame:
    """Get data from all pages in batches.

    Args:
    ----
        url (str): The URL to get data from.
        headers (dict[str, str]): The headers to use for the request.
        pages (int): The number of pages to get data from.
        key (str): The key to get the required values from the response data.
        batch_size (int): The number of pages to get data from in each batch.

    """
    dfs = []
    for start_page in range(1, pages + 1, batch_size):
        end_page = min(start_page + batch_size - 1, pages)
        print(f"Getting pages {start_page} to {end_page}")  # noqa: T201
        dfs.append(await get_data_for_page_range(url, start_page, end_page, headers, key))
    return pd.concat(dfs).reset_index(drop=True)
