from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_location_group(
    loc_group_id: str,
    category_id: str,
    office_id: str,
    group_office_id: Optional[str] = None,
    category_office_id: Optional[str] = None,
) -> Data:
    """Retreives time series stored in the requested time series group

    Parameters
        ----------
            group_id: string
                Location group whose data is to be included in the response.
            category_id: string
                The category id that contains the Location group.
            office_id: string
                The owning office of the Locations assigned to the group whose data is to be included in the response.
            group_office_id: string
                Specifies the owning office of the Location group.
            category_office_id: string
                Specifies the owning office of the Location group category.

        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = f"location/group/{loc_group_id}"
    params = {
        "office": office_id,
        "category-id": category_id,
        "category-office-id": category_office_id,
        "group-office-id": group_office_id,
    }

    response = api.get(endpoint, params, api_version=1)
    return Data(response, selector="assigned-locations")


def get_location_groups(
    office_id: Optional[str] = None,
    include_assigned: Optional[bool] = True,
    location_category_like: Optional[str] = None,
    location_office_id: Optional[str] = None,
    category_office_id: Optional[str] = None,
) -> Data:
    """
    Retreives a list of location groups.

    Parameters
    ----------
        office_id: string
            Specifies the owning office of the location group whose data is to be included in the response..
        include_assigned: Boolean
            Include the assigned location in the returned timeseries groups. (default: true)
        location_category_like: string
            Posix regular expression matching against the location category id
        location_office_id: String
            Specifies the owning office of the location assigned to the location group whose data is to be included in the response.
        category_office_id: string
            Specifies the owning office of the category the location group belongs to whose data is to be included in the response.
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = "location/group"
    params = {
        "office": office_id,
        "include-assigned": include_assigned,
        "location-category-like": location_category_like,
        "location-office-id": location_office_id,
        "category-office-id": category_office_id,
    }
    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response)


def store_location_groups(data: JSON) -> None:
    """
    Create new Location Group
    Parameters
    ----------
        data: JSON dictionary
            location group data to be stored.

    Returns
    -------
    None
    """

    if data is None:
        raise ValueError("Cannot store a standard text without timeseries group JSON")

    endpoint = "location/group"

    return api.post(endpoint=endpoint, data=data, api_version=1)


def update_location_group(
    data: JSON,
    group_id: str,
    office_id: str,
    replace_assigned_locs: Optional[bool] = False,
) -> None:
    """
    Updates the location groups with the provided group ID and office ID.

    Parameters
    ----------
        group_id : str
            The group if of the location to be updated
        office_id : str
            The ID of the office associated with the specified location group.
        replace_assigned_ts : bool, optional
            Specifies whether to unassign all existing locations before assigning new locations specified in the content body. Default is False.
        data: JSON dictionary
            Location Group data to be stored.

    Returns
    -------
    None
    """

    endpoint = f"location/group/{group_id}"
    params = {
        "replace-assigned-locs": replace_assigned_locs,
        "office": office_id,
    }

    api.patch(endpoint=endpoint, data=data, params=params, api_version=1)


def delete_location_group(group_id: str, category_id: str, office_id: str) -> None:
    """Deletes requested time series group

    Parameters
        ----------
            group_id: string
                The location group to be deleted
            category_id: string
                Specifies the location category of the location group to be deleted
            office_id: string
                Specifies the owning office of the location group to be deleted

        Returns
        -------
            None
    """

    endpoint = f"location/group/{group_id}"
    params = {
        "office": office_id,
        "category-id": category_id,
    }

    return api.delete(endpoint, params=params, api_version=1)
