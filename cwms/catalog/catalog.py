from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

import cwms.api as api
from cwms.cwms_types import Data


def get_locations_catalog(
    office_id: str,
    page: Optional[str] = None,
    page_size: Optional[int] = 5000,
    unit_system: Optional[str] = None,
    like: Optional[str] = None,
    location_category_like: Optional[str] = None,
    location_group_like: Optional[str] = None,
    bounding_office_like: Optional[str] = None,
    location_kind_like: Optional[str] = None,
) -> Data:
    """Retrieves filters for a locations catalog

    Parameters
    ----------
        page: string
            The endpoint used to identify where the request is located.
        page_size: integer
            The entries per page returned. The default value is 5000.
        unit_system: string
            The unit system desired in response. Valid values for this
            field are:
                1. SI
                2. EN
        office_id: string
            The owning office of the timeseries group.
        like: string
            The regex for matching against the id
        location_category_like: string
            The regex for matching against the location category id
        location_group_like: string
            The regex for matching against the location group id
        bounding_office_like: string
            The regex for matching against the location bounding office
        location_kind_like: string
            Posix regular expression matching against the location kind. The location-kind is typically unset or one of the following: {"SITE", "EMBANKMENT", "OVERFLOW", "TURBINE", "STREAM", "PROJECT", "STREAMGAGE", "BASIN", "OUTLET", "LOCK", "GATE"}. Multiple kinds can be matched by using Regular Expression OR clauses. For example: "(SITE|STREAM)"

    Returns
    -------
        cwms data type
    """

    # CHECKS
    if office_id is None:
        raise ValueError("Retrieve locations catalog requires an office")

    dataset = "LOCATIONS"
    endpoint = f"catalog/{dataset}"
    params = {
        "page": page,
        "page-size": page_size,
        "units": unit_system,
        "office": office_id,
        "like": like,
        "location-category-like": location_category_like,
        "location-group-like": location_group_like,
        "bounding-office-like": bounding_office_like,
        "location-kind-like": location_kind_like,
    }

    response = api.get(endpoint=endpoint, params=params, api_version=2)
    return Data(response, selector="entries")


def get_timeseries_catalog(
    office_id: str,
    page: Optional[str] = None,
    page_size: Optional[int] = 5000,
    unit_system: Optional[str] = None,
    like: Optional[str] = None,
    timeseries_category_like: Optional[str] = None,
    timeseries_group_like: Optional[str] = "DMZ Include List",
    bounding_office_like: Optional[str] = None,
    include_extents: Optional[bool] = False,
) -> Data:
    """Retrieves filters for the timeseries catalog

    Parameters
    ----------
        page: string
            The endpoint used to identify where the request is located.
        page_size: integer
            The entries per page returned. The default value is 500.
        unit_system: string
            The unit system desired in response. Valid values for this
            field are:
                1. SI
                2. EN
        office_id: string
            The owning office of the timeseries group.
        like: string
            The regex for matching against the id
        timeseries_category_like: string
            The regex for matching against the category id
        timeseries_group_like: string
            The regex for matching against the timeseries group id. This will default to pull only public datasets
        bounding_office_like: string
            The regex for matching against the location bounding office
        include_extents: bool
            Whether to include the time series extents in the catalog

    Returns
    -------
        cwms data type
    """

    # CHECKS
    if office_id is None:
        raise ValueError("Retrieve timeseries catalog requires an office")

    dataset = "TIMESERIES"
    endpoint = f"catalog/{dataset}"
    params = {
        "page": page,
        "page-size": page_size,
        "unit-system": unit_system,
        "office": office_id,
        "like": like,
        "timeseries-category-like": timeseries_category_like,
        "timeseries-group-like": timeseries_group_like,
        "bounding-office-like": bounding_office_like,
        "include-extents": include_extents,
    }

    response = api.get(endpoint=endpoint, params=params, api_version=2)
    return Data(response, selector="entries")


def get_ts_extents(ts_id: str, office_id: str) -> Tuple[datetime, datetime, datetime]:
    """Retrieves earliest extent, latest extent, and last update via cwms.get_timeseries_catalog

    Parameters
    ----------
        ts_id: string
            Timseries id to query.
        office_id: string
            The owning office of the timeseries group.

    Returns
    -------
        tuple of datetime objects (earliest_time, latest_time, last_update)
    """
    cwms_cat = get_timeseries_catalog(
        office_id=office_id,
        like=ts_id,
        timeseries_group_like=None,
        page_size=500,
        include_extents=True,
    ).df

    times = cwms_cat[cwms_cat.name == ts_id].extents.values[0][0]

    earliest_time = pd.to_datetime(times["earliest-time"])
    latest_time = pd.to_datetime(times["latest-time"])
    last_update = pd.to_datetime(times["last-update"])

    return earliest_time, latest_time, last_update
