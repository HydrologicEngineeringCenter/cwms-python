#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_location_levels(
    level_id_mask: str = "*",
    office_id: Optional[str] = None,
    unit: Optional[str] = None,
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page: Optional[str] = None,
    page_size: Optional[int] = None,
) -> Data:
    """
    Parameters
    ----------
    level_id_mask : str, optional
        A string representing the mask for level IDs. Default is "*".

    office_id : str, optional
        A string representing the office ID.

    unit : str, optional
        A string representing the unit to retrieve values in.

    datum : str, optional
        A string representing the vertical datum.

    begin : datetime, optional
        A datetime object representing the beginning date.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

    end : datetime, optional
        A datetime object representing the end date.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

    page : str, optional
        A string representing the page to retrieve. If None then the first page will be retrieved.

    page_size : int, optional
        An integer representing the number of items per page.
    """
    endpoint = "levels"

    params = {
        "office": office_id,
        "level-id-mask": level_id_mask,
        "unit": unit,
        "datum": datum,
        "begin": begin.isoformat() if begin else "",
        "end": end.isoformat() if end else "",
        "page": page,
        "page-size": page_size,
    }
    response = api.get(endpoint, params)
    return Data(response)


def get_location_level(
    level_id: str,
    office_id: str,
    effective_date: datetime,
    unit: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    level_id : str
        The ID of the location level to retrieve.

    office_id : str
        The ID of the office associated with the location level.

    effective_date : datetime
        The effective date of the location level.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

    unit : str, optional
        The unit of measurement for the location level.

    Returns
    -------
    response : dict
        The JSON response containing the location level information.

    Raises
    ------
    ValueError
        If `level_id`, `office_id`, or `effective_date` is None.
    """

    if level_id is None:
        raise ValueError("Cannot retrieve a single location level without an id")
    if office_id is None:
        raise ValueError("Cannot retrieve a single location level without an office id")
    if effective_date is None:
        raise ValueError(
            "Cannot retrieve a single location level without an effective date"
        )
    endpoint = f"levels/{level_id}"

    params = {
        "office": office_id,
        "unit": unit,
        "effective-date": effective_date.isoformat(),
    }

    response = api.get(endpoint, params)
    return Data(response)


def store_location_level(data: JSON) -> None:
    """
    Parameters
    ----------
    data : dict
        The JSON data dictionary containing the location level information.

    """
    if data is None:
        raise ValueError("Cannot store a location level without a JSON data dictionary")

    endpoint = "levels"
    return api.post(endpoint, data, params=None)


def delete_location_level(
    location_level_id: str,
    office_id: str,
    effective_date: Optional[datetime] = None,
    cascade_delete: bool = False,
) -> None:
    """
    Parameters
    ----------
    location_level_id : str
        The ID of the location level to be deleted.
    office_id : str
        The ID of the office associated with the location level.
    effective_date : datetime, optional
        The effective date of the deletion. If not provided, the current date and time will be used.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
    cascade_delete : bool, optional
        If True, all related seasonal level data will also be deleted.
        If False or not provided and seasonal level data exists, an error will be thrown.
    """
    if location_level_id is None:
        raise ValueError("Cannot delete a location level without an id")
    if office_id is None:
        raise ValueError("Cannot delete a location level without an office id")
    endpoint = f"levels/{location_level_id}"

    params = {
        "office": office_id,
        "effective-date": (effective_date.isoformat() if effective_date else None),
        "cascade-delete": cascade_delete,
    }
    return api.delete(endpoint, params)


def get_level_as_timeseries(
    location_level_id: str,
    office_id: str,
    unit: str,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    interval: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    location_level_id : str
        The ID of the location level for which the time series data will be retrieved.
    office_id : str
        The ID of the office for which the time series data will be retrieved.
    unit : str
        The unit for the data of the time series response to be returned as.
    begin : datetime, optional
        The start datetime for the time series data. Defaults to None.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
    end : datetime, optional
        The end datetime for the time series data. Defaults to None.
        If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
    interval : str, optional
        The interval at which the time series data will be established. Defaults to None.

    Returns
    -------
    response : dict
        The JSON response containing the time series data in JSON format.
    """
    if location_level_id is None:
        raise ValueError(
            "Cannot retrieve a time series for a location level without an id"
        )
    if office_id is None:
        raise ValueError(
            "Cannot retrieve a time series for a location level without an office id"
        )
    endpoint = f"levels/{location_level_id}/timeseries"

    params = {
        "office": office_id,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "interval": interval,
        "unit": unit,
    }
    response = api.get(endpoint, params)
    return Data(response, selector="values")
