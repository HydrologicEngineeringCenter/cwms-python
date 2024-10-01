#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_virtual_outlet(office_id: str, project_id: str, name: str) -> Data:
    """
    Parameters
    ----------
    name : str
        The ID of the virtual outlet.
    project_id:
        The project for the virtual outlet.
    office_id : str
        The ID of the office.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of name, project_id, or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Retrieve virtual outlet requires a name")
    if project_id is None:
        raise ValueError("Retrieve virtual outlet requires a project id")
    if office_id is None:
        raise ValueError("Retrieve virtual outlet requires an office")

    endpoint = f"projects/{office_id}/{project_id}/virtual-outlets/{name}"
    response = api.get(endpoint)
    return Data(response)


def get_virtual_outlets(office_id: str, project_id: str) -> Data:
    """
    Parameters
    ----------
    project_id:
        The project for the virtual outlets.
    office_id : str
        The ID of the office.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of project_id, or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if project_id is None:
        raise ValueError("Retrieve virtual outlets requires a project id")
    if office_id is None:
        raise ValueError("Retrieve virtual outlets requires an office")

    endpoint = f"projects/{office_id}/{project_id}/virtual-outlets"
    response = api.get(endpoint)
    return Data(response)


def delete_virtual_outlet(
    office_id: str, project_id: str, name: str, delete_method: DeleteMethod
) -> None:
    """
    Parameters
    ----------
    name : str
        The name of the virtual outlet.
    project_id:
        The project for the virtual outlet.
    office_id : str
        The ID of the virtual outlet's office.
    delete_method: DeleteMethod
        The method to use to delete the virtual outlet.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of name, project_id, delete_method, or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Delete virtual outlet requires an outlet name")
    if project_id is None:
        raise ValueError("Delete virtual outlet requires a project id")
    if office_id is None:
        raise ValueError("Delete virtual outlet requires an office")
    if delete_method is None:
        raise ValueError("Delete virtual outlet requires a delete method")

    endpoint = f"projects/{office_id}/{project_id}/virtual-outlets/{name}"
    params = {"method": delete_method.name}
    api.delete(endpoint, params)


def store_virtual_outlet(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """
    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        A boolean value indicating whether to fail if
        the virtual outlet already exists. Default is True.

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

    endpoint = "projects/virtual-outlets"
    params = {"fail-if-exists": fail_if_exists}
    api.post(endpoint, data, params)
