#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_outlet(office_id: str, name: str) -> Data:
    """
    Parameters
    ----------
    name : str
        The ID of the outlet.
    office_id : str
        The ID of the office.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of name or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Retrieve outlet requires a name")
    if office_id is None:
        raise ValueError("Retrieve outlet requires an office")

    endpoint = f"projects/outlets/{name}"
    params = {"office": office_id}
    response = api.get(endpoint, params)
    return Data(response)


def get_outlets(office_id: str, project_id: str) -> Data:
    """
    Parameters
    ----------
    project_id : str
        The project ID of the outlets.
    office_id : str
        The ID of the project's office.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of project_id or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if project_id is None:
        raise ValueError("Retrieve outlets requires a project id")
    if office_id is None:
        raise ValueError("Retrieve outlets requires an office")

    endpoint = "projects/outlets"
    params = {"office": office_id, "project-id": project_id}
    response = api.get(endpoint, params)
    return Data(response)


def delete_outlet(office_id: str, name: str, delete_method: DeleteMethod) -> None:
    """
    Parameters
    ----------
    name : str
        The name of the outlets.
    office_id : str
        The ID of the project's office.
    delete_method: DeleteMethod
        The method to use to delete the outlet.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of name, delete_method, or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Delete outlet requires an outlet name")
    if office_id is None:
        raise ValueError("Delete outlet requires an office")
    if delete_method is None:
        raise ValueError("Delete outlet requires a delete method")

    endpoint = f"projects/outlets/{name}"
    params = {"office": office_id, "method": delete_method.name}
    api.delete(endpoint, params)


def rename_outlet(office_id: str, old_name: str, new_name: str) -> None:
    """
    Parameters
    ----------
    old_name : str
        The name of the outlet to rename.
    new_name : str
        The new name of the outlet.
    office_id : str
        The ID of the project's office.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of old_outlet_name, new_outlet_name,  or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if old_name is None:
        raise ValueError("Rename outlet requires the original outlet name")
    if new_name is None:
        raise ValueError("Rename outlet requires a new outlet name")
    if office_id is None:
        raise ValueError("Rename outlet requires an office")

    endpoint = f"projects/outlets/{old_name}"
    params = {"office": office_id, "name": new_name}
    api.patch(endpoint=endpoint, params=params)


def store_outlet(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """
    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        A boolean value indicating whether to fail if the outlet already exists.
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
        raise ValueError("Cannot store an outlet without a JSON data dictionary")

    endpoint = "projects/outlets"
    params = {"fail-if-exists": fail_if_exists}
    api.post(endpoint, data, params)
