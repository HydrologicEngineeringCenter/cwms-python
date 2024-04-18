#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from enum import Enum, auto
from typing import Optional

import cwms.api as api
from cwms.types import JSON, Data


class TextTsMode(Enum):
    REGULAR = auto()
    STANDARD = auto()
    ALL = auto()


class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


def get_text_timeseries(
    timeseries_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    mode: TextTsMode = TextTsMode.REGULAR,
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
    mode : TextTsMode, optional
        The mode for retrieving text timeseries data.
        Default is `TextTsMode.REGULAR`.
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
        raise ValueError("Retrieve text timeseries requires an id")
    if office_id is None:
        raise ValueError("Retrieve text timeseries requires an office")
    if begin is None:
        raise ValueError("Retrieve text timeseries requires a time window")
    if end is None:
        raise ValueError("Retrieve text timeseries requires a time window")

    endpoint = "timeseries/text"
    params = {
        "office": office_id,
        "name": timeseries_id,
        "min-attribute": min_attribute,
        "max-attribute": max_attribute,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "mode": mode.name,
    }

    response = api.get(endpoint, params)
    return Data(response)


def store_text_timeseries(data: JSON, replace_all: bool = False) -> Data:
    """
    This method is used to store a text time series through CWMS Data API.

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

    if dict is None:
        raise ValueError(
            "Cannot store a text time series without a JSON data dictionary"
        )

    endpoint = "timeseries/text"
    params = {"replace-all": replace_all}

    response = api.post(endpoint, data, params)
    return Data(response)


def delete_text_timeseries(
    timeseries_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    mode: TextTsMode = TextTsMode.REGULAR,
    text_mask: str = "*",
    min_attribute: Optional[float] = None,
    max_attribute: Optional[float] = None,
) -> None:
    """
    Deletes text timeseries data with the given ID and office ID and time range.

    Parameters
    ----------
    timeseries_id : str
        The ID of the text time series data to be deleted.
    office_id : str
        The ID of the office that the text time series belongs to.
    text_mask : str, optional
        The standard text pattern to match.
        Use glob-style wildcard characters instead of sql-style wildcard
        characters for pattern matching.
        For StandardTextTimeSeries this should be the Standard_Text_Id
        (such as 'E' for ESTIMATED)
        Default value is `"*"`
    begin : datetime
        The start date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    end : datetime
        The end date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    mode : TextTsMode, optional
        The mode for deleting text timeseries data.
        Default is `TextTsMode.REGULAR`.
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
        raise ValueError("Deleting text timeseries requires an id")
    if office_id is None:
        raise ValueError("Deleting text timeseries requires an office")
    if begin is None:
        raise ValueError("Deleting text timeseries requires a time window")
    if end is None:
        raise ValueError("Deleting text timeseries requires a time window")

    endpoint = f"timeseries/text/{timeseries_id}"
    params = {
        "office": office_id,
        "min-attribute": min_attribute,
        "max-attribute": max_attribute,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "mode": mode.name,
        "text-mask": text_mask,
    }

    return api.delete(endpoint, params)


def get_standard_text_catalog(
    text_id_mask: Optional[str] = None, office_id_mask: Optional[str] = None
) -> Data:
    """
    Retrieves standard text catalog for the given ID and office ID filters.

    Parameters
    ----------
    text_id_mask : str
        The ID filter of the standard text value to retrieve.
    office_id_mask : str
        The ID filter of the office that the standard text belongs to.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    endpoint = "timeseries/text/standard-text-id"
    params = {"text-id-mask": text_id_mask, "office-id-mask": office_id_mask}

    response = api.get(endpoint, params)
    return Data(response)


def get_standard_text(text_id: str, office_id: str) -> Data:
    """
    Retrieves standard text for the given ID and office ID.

    Parameters
    ----------
    text_id : str
        The ID of the standard text value to retrieve.
    office_id : str
        The ID of the office that the standard text belongs to.

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

    if text_id is None:
        raise ValueError("Retrieving standard text requires an id")
    if office_id is None:
        raise ValueError("Retrieving standard timeseries requires an office")

    endpoint = f"timeseries/text/standard-text-id/{text_id}"
    params = {"office": office_id}

    response = api.get(endpoint, params)
    return Data(response)


def delete_standard_text(
    text_id: str, delete_method: DeleteMethod, office_id: str
) -> None:
    """
    Deletes standard text for the given ID and office ID.

    Parameters
    ----------
    text_id : str
        The ID of the standard text value to be deleted.
    office_id : str
        The ID of the office that the standard text belongs to.
    delete_method : str
        Delete method for the standard text id.
        DELETE_ALL - deletes the key and the value from the clob table
        DELETE_KEY - deletes the text id key, but leaves the value in the clob table
        DELETE_DATA - deletes the value from the clob table but leaves the text id key

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

    if text_id is None:
        raise ValueError("Deleting standard text requires an id")
    if office_id is None:
        raise ValueError("Deleting standard timeseries requires an office")
    if delete_method is None:
        raise ValueError("Deleting standard timeseries requires a delete method")

    endpoint = f"timeseries/text/standard-text-id/{text_id}"
    params = {"office": office_id, "method": delete_method.name}

    return api.delete(endpoint, params)


def store_standard_text(data: JSON, fail_if_exists: bool = False) -> Data:
    """
    This method is used to store a standard text value through CWMS Data API.

    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        Throw a ClientError if the text id already exists.
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

    if dict is None:
        raise ValueError("Cannot store a standard text without a JSON data dictionary")

    endpoint = "timeseries/text/standard-text-id"
    params = {"fail-if-exists": fail_if_exists}

    response = api.post(endpoint, data, params)
    return Data(response)
