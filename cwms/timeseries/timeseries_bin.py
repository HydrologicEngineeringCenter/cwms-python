#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from typing import Optional

import requests

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_binary_timeseries(
    timeseries_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    version_date: Optional[datetime] = None,
    bin_type_mask: str = "*",
) -> Data:
    """
    Parameters
    ----------
    timeseries_id : str
        The ID of the timeseries.
    office_id : str
        The ID of the office.
    begin : datetime
        The start date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    end : datetime
        The end date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    version_date : datetime, optional
        The time series date version to retrieve. If not supplied,
        the maximum date version for each time step in the retrieval
        window will be retrieved.
    bin_type_mask : str, optional
        The binary media type pattern to match.
        Use glob-style wildcard characters instead of sql-style wildcard
        characters for pattern matching.
        Default value is `"*"`


    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of timeseries_id, office_id, begin, or end is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if timeseries_id is None:
        raise ValueError("Retrieve binary timeseries requires an id")
    if office_id is None:
        raise ValueError("Retrieve binary timeseries requires an office")
    if begin is None:
        raise ValueError("Retrieve binary timeseries requires a time window")
    if end is None:
        raise ValueError("Retrieve binary timeseries requires a time window")

    endpoint = "timeseries/binary"

    version_date_str = version_date.isoformat() if version_date else None
    params = {
        "office": office_id,
        "name": timeseries_id,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "version-date": version_date_str,
        "binary-type-mask": bin_type_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_large_blob(url: str) -> bytes:
    """
    Retrieves large blob data greater than 64kb from CWMS data api
    :param url: str
        Url used in query by CDA
    :return: bytes
        Large binary data
    """
    response = requests.get(url)
    return response.content


def store_binary_timeseries(data: JSON, replace_all: bool = False) -> None:
    """
    This method is used to store a binary time series through CWMS Data API.

    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    replace_all : str, optional
        Default is `False`.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If either dict is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Storing binary time series requires a JSON data dictionary")

    endpoint = "timeseries/binary"
    params = {"replace-all": replace_all}

    return api.post(endpoint, data, params)


def delete_binary_timeseries(
    timeseries_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    version_date: Optional[datetime] = None,
    bin_type_mask: str = "*",
) -> None:
    """
    Deletes binary timeseries data with the given ID,
    office ID and time range.

    Parameters
    ----------
    timeseries_id : str
        The ID of the binary time series data to be deleted.
    office_id : str
        The ID of the office that the binary time series belongs to.
    begin : datetime
        The start date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    end : datetime
        The end date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    version_date : Optional[datetime]
        The time series date version to retrieve. If not supplied,
        the maximum date version for each time step in the retrieval
        window will be deleted.
    bin_type_mask : str, optional
        The binary media type pattern to match.
        Use glob-style wildcard characters instead of sql-style wildcard
        characters for pattern matching.
        Default value is `"*"`


    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of timeseries_id, office_id, begin, or end is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if timeseries_id is None:
        raise ValueError("Deleting binary timeseries requires an id")
    if office_id is None:
        raise ValueError("Deleting binary timeseries requires an office")
    if begin is None:
        raise ValueError("Deleting binary timeseries requires a time window")
    if end is None:
        raise ValueError("Deleting binary timeseries requires a time window")

    endpoint = f"timeseries/binary/{timeseries_id}"
    version_date_str = version_date.isoformat() if version_date else None
    params = {
        "office": office_id,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "binary-type-mask": bin_type_mask,
        "version-date": version_date_str,
    }

    return api.delete(endpoint, params=params)
