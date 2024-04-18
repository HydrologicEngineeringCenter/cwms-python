#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from enum import Enum, auto
from typing import Optional

import cwms.api as api
from cwms.types import JSON, Data


class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


def get_binary_timeseries(
    timeseries_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    bin_type_mask: str = "*",
    min_attribute: Optional[float] = None,
    max_attribute: Optional[float] = None,
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
    bin_type_mask : str, optional
        The binary media type pattern to match.
        Use glob-style wildcard characters instead of sql-style wildcard
        characters for pattern matching.
        Default value is `"*"`
    min_attribute : float, optional
        The minimum attribute value to filter the timeseries data.
        Default is `None`.
    max_attribute : float, optional
        The maximum attribute value to filter the timeseries data.
        Default is `None`.

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
    params = {
        "office": office_id,
        "name": timeseries_id,
        "min-attribute": min_attribute,
        "max-attribute": max_attribute,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "binary-type-mask": bin_type_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


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
    bin_type_mask: str = "*",
    min_attribute: Optional[float] = None,
    max_attribute: Optional[float] = None,
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
    bin_type_mask : str, optional
        The binary media type pattern to match.
        Use glob-style wildcard characters instead of sql-style wildcard
        characters for pattern matching.
        Default value is `"*"`
    begin : datetime
        The start date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    end : datetime
        The end date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    min_attribute : float, optional
        The minimum attribute value to filter the timeseries data.
        Default is `None`.
    max_attribute : float, optional
        The maximum attribute value to filter the timeseries data.
        Default is `None`.

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
    params = {
        "office": office_id,
        "min-attribute": min_attribute,
        "max-attribute": max_attribute,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "binary-type-mask": bin_type_mask,
    }

    return api.delete(endpoint, params=params)
