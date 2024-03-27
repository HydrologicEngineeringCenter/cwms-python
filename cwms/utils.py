from typing import Any, cast

import pandas as pd
from pandas import DataFrame
from requests.models import Response

from cwms.core import _CwmsBase
from cwms.types import JSON

from .exceptions import ClientError, NoDataFoundError, ServerError


def queryCDA(
    self: _CwmsBase, endpoint: str, payload: JSON, headerList: dict[str, str]
) -> JSON:
    """Send a query.

    Wrapper for requests.get that handles errors and returns response.

    Parameters
    ----------
    endpoint: string
        URL to query
    payload: dict
        query parameters passed to ``requests.get``
    headerList: dict
        headers

    Returns
    -------
    string: query response
        The response from the API query ``requests.get`` function call.
    """

    response = self.get_session().get(endpoint, params=payload, headers=headerList)

    raise_for_status(response)
    return cast(JSON, response.json())


def raise_for_status(response: Response) -> Response:
    if response.status_code == 404:
        raise NoDataFoundError(response)
    elif response.status_code >= 500:
        raise ServerError(response)
    elif response.status_code >= 400:
        raise ClientError(response)

    # if response.status_code > 200:

    #   raise Exception(
    #       f'Error Code: {response.status_code} \n Bad Request for URL: {response.url} \n response.text'
    #   )

    return response


def return_df(dict: JSON, dict_key: list[str]) -> DataFrame:
    """Convert output to correct format requested by user
    Parameters
    ----------
    response : Request object
        response from get request
    dict_key : str
        key needed to grab correct values from json decoded dictionary.

    Returns
    -------
    pandas df
    """

    # converts dictionary to df based on the key provided for the endpoint
    temp_dict = dict
    for key in dict_key:
        temp_dict = temp_dict[key]
    df = pd.DataFrame(temp_dict)

    # if timeseries values are present then grab the values and put into dataframe
    if dict_key[-1] == "values":
        df.columns = pd.Index([sub["name"] for sub in dict["value-columns"]])

        if "date-time" in df.columns:
            df["date-time"] = pd.to_datetime(df["date-time"], unit="ms")

    return df
