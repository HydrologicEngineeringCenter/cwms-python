from typing import Optional

import cwms.api as api
from cwms.types import Data


def get_locations_catalog(
    page: Optional[str] = None,
    page_size: Optional[int] = 5000,
    unit_system: Optional[str] = None,
    office_id: Optional[str] = None,
    like: Optional[str] = None,
    timeseries_category_like: Optional[str] = None,
    timeseries_group_like: Optional[str] = None,
    location_category_like: Optional[str] = None,
    location_group_like: Optional[str] = None,
    bounding_office_like: Optional[str] = None,
) -> Data:
    """Retrieves filters for a locations catalog

    Parameters
    ----------
        dataset: string
            The type of data in the list. Valid values for this field
            are:
                1. TIMESERIES
                2. LOCATIONS
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
        timeseries_category_like: string
            The regex for matching against the category id
        timeseries_group_like: string
            The regex for matching against the timeseries group id
        location_category_like: string
            The regex for matching against the location category id
        location_group_like: string
            The regex for matching against the location group id
        bounding_office_like: string
            The regex for matching against the location bounding office

    Returns
    -------
    response : dict
        The JSON response containing the time series catalog.
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
        "timeseries-category-like": timeseries_category_like,
        "timeseries-group-like": timeseries_group_like,
        "location-category-like": location_category_like,
        "location-group-like": location_group_like,
        "bounding-office-like": bounding_office_like,
    }

    response = api.get(endpoint=endpoint, params=params, api_version=2)
    return Data(response, selector="locations-catalog")


def get_timeseries_catalog(
    page: Optional[str] = None,
    page_size: Optional[int] = 5000,
    unit_system: Optional[str] = None,
    office_id: Optional[str] = None,
    like: Optional[str] = None,
    timeseries_category_like: Optional[str] = None,
    timeseries_group_like: Optional[str] = None,
    location_category_like: Optional[str] = None,
    location_group_like: Optional[str] = None,
    bounding_office_like: Optional[str] = None,
) -> Data:
    """Retrieves filters for the timeseries catalog

    Parameters
    ----------
        dataset: string
            The type of data in the list. Valid values for this field
            are:
                1. TIMESERIES
                2. LOCATIONS
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
            The regex for matching against the timeseries group id
        location_category_like: string
            The regex for matching against the location category id
        location_group_like: string
            The regex for matching against the location group id
        bounding_office_like: string
            The regex for matching against the location bounding office

    Returns
    -------
    response : dict
        The JSON response containing the time series catalog.
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
        "location-category-like": location_category_like,
        "location-group-like": location_group_like,
        "bounding-office-like": bounding_office_like,
    }

    response = api.get(endpoint=endpoint, params=params, api_version=2)
    return Data(response, selector="entries")
