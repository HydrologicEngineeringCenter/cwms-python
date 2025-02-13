#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def store_location_group(data: JSON, fail_if_exists: Optional[bool]) -> None:
    """
    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        A boolean value indicating whether to fail if the location group already exists.
        Default is True.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of data is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Cannot store a location group without a JSON data dictionary")

    endpoint = "location/group"
    api.post(endpoint, data, api_version=1)


def get_location_group(group_id: str, office_id: str, category_id: str) -> Data:
    """
    Parameters
    ----------
    group_id : str
        Specifies the location_group whose data is to be included in the response
    office_id : str
        Specifies the owning office of the location group whose data is to be included in the response.
    category_id : str
        Specifies the category containing the location group whose data is to be included in the response.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of group_id or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if group_id is None:
        raise ValueError("Retrieve location group requires a group id")
    if office_id is None:
        raise ValueError("Retrieve location group requires an office")
    if category_id is None:
        raise ValueError("Retrieve location group requires a category id")

    endpoint = f"location/group/{group_id}"

    params = {"office": office_id, "category-id": category_id}
    response = api.get(endpoint, params)

    return Data(response)


def get_location_groups(
    office_id: Optional[str],
    include_assigned: Optional[bool],
    location_category_like: Optional[str],
) -> Data:
    """
    Parameters
    ----------
    office_id : str
        Specifies the owning office of the location group(s) whose data is to be included in the response. If this field is not specified, matching location groups information from all offices shall be returned.
    include_assigned : str
        Include the assigned locations in the returned location groups. (default: false)
    location_category_like : str
        Posix regular expression matching against the location category id

    References
    ----------
        https://cwms.usace.army.mil/cwms-data-api/endpoint/location/group

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of office_id or category_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    endpoint = "location/group"
    params = {
        "office": office_id,
        "include_assigned": include_assigned,
        "location_category_like": location_category_like,
    }

    response = api.get(endpoint, params, api_version=1)

    return Data(response)


def delete_location_group(
    group_id: str, office_id: str, delete_method: DeleteMethod
) -> None:
    """
    Parameters
    ----------
    group_id : str
        The ID of the location group.
    office_id : str
        The ID of the office.
    delete_method: DeleteMethod
        The method to use to delete the location group.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of group_id, office_id, or delete_method is None.
    ClientError
        If a 400 range error code response is returned from the server.
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if group_id is None:
        raise ValueError("Delete location group requires a group id")
    if office_id is None:
        raise ValueError("Delete location group requires an office")
    if delete_method is None:
        raise ValueError("Delete location group requires a delete method")

    endpoint = f"location/group/{group_id}"
    params = {"office": office_id, "method": delete_method.name}
    api.delete(endpoint, params)


def update_location_group(
    data: JSON, office_id: str, group_id: str, replace_assigned_locs: Optional[bool]
) -> None:
    """
    Parameters
    ----------
    office_id : str
        Specifies the office of the user making the request. This is the office that the location, group, and category belong to. If the group and/or category belong to the CWMS office, this only identifies the location.
    replace_assigned_locs : str
        Specifies whether to unassign all existing locations before assigning new locations specified in the content body Default: false
    group_id : str
        The new name of the location group.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of old_location_group_name, new_location_group_name,  or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if group_id is None:
        raise ValueError("Rename location group requires a location group name")
    if office_id is None:
        raise ValueError("Rename location group requires an office")

    endpoint = f"location/group/{group_id}"
    params = {"office": office_id, "replace-assigned-locs": replace_assigned_locs}
    api.patch(data=data, endpoint=endpoint, params=params)
