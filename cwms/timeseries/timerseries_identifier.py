from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_timeseries_identifier(ts_id: str, office_id: str) -> Data:
    """Retrieves the identifiing information for a timeseries.  Does not inluce time series values

    Parameters
    ----------
        ts_id: string
            Name(s) of the time series whose data is to be included in the response.
        office_id: string
            The owning office of the time series(s).
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe.
    """

    # creates the dataframe from the timeseries data
    endpoint = f"timeseries/identifier-descriptor/{ts_id}"
    params = {
        "office": office_id,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_timeseries_identifiers(
    office_id: str,
    timeseries_id_regex: Optional[str] = None,
    page_size: int = 500000,
) -> Data:
    """Retrieves the identifiing information for a timeseries.  Does not inluce time series values

    Parameters
    ----------
        ts_id: string
            Name(s) of the time series whose data is to be included in the response.
        office_id: string
            The owning office of the time series(s).
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe.
    """

    # creates the dataframe from the timeseries data
    endpoint = "timeseries/identifier-descriptor/"
    params = {
        "office": office_id,
        "timeseries-id-regex": timeseries_id_regex,
        "page-size": page_size,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="descriptors")


def delete_timeseries_identifier(
    ts_id: str, office_id: str, delete_method: str
) -> None:
    """
    deleted at timeseries with a

    Parameters
    ----------
    ts_id : str
        The ID of the timeseries to be deleted
    office_id : str
        The ID of the office that the time series belongs to
    delete_method : str
        Delete method for the timeseries.
        DELETE_ALL - deletes the timeseries and values
        DELETE_KEY - deletes the times series, but leaves the values in the tsv tables
        DELETE_DATA - deletes the values from the tsv table but leaves the time series key

    Returns
    -------
    None
    """
    delete_methods = ["DELETE_ALL", "DELETE_KEY", "DELETE_DATA"]
    if ts_id is None:
        raise ValueError("Deleting timeseries requires an id")
    if office_id is None:
        raise ValueError("Deleting timeseries requires an office")
    if delete_method not in delete_methods:
        raise ValueError(
            "Deleting timeseries requires a delete method of DELETE_ALL, DELETE_KEY, or DELETE_DATA"
        )

    endpoint = f"timeseries/identifier-descriptor/{ts_id}"
    params = {"office": office_id, "method": delete_method}

    return api.delete(endpoint, params)


def store_timeseries_identifier(
    data: str, fail_if_exists: Optional[bool] = True
) -> None:
    """
    This method is used to store a new rating template

    Parameters
    ----------
    data : str
        json for storing a time series identifier
        {
            "office-id": "string",
            "time-series-id": "string",
            "timezone-name": "string",
            "interval-offset-minutes": 0,
            "active": true
        }

    fail_if_exists : str, optional
        Throw a ClientError if the ts_id already exists.
        Default is `False`.

    Returns
    -------
    None
    """

    if data is None:
        raise ValueError("Cannot store a time series identifier with out json data")

    endpoint = "timeseries/identifier-descriptor/"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params)
