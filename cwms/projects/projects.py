#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.types import JSON, Data, DeleteMethod


def get_project(office_id: str, name: str) -> Data:
    """
    Parameters
    ----------
    name : str
        The ID of the project.
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
        raise ValueError("Retrieve project requires a name")
    if office_id is None:
        raise ValueError("Retrieve project requires an office")

    endpoint = f"projects/{name}"
    params = {"office": office_id}
    response = api.get(endpoint, params)
    return Data(response)


def get_projects(
    office_id: str,
    id_mask: Optional[str] = None,
    page: Optional[str] = None,
    page_size: Optional[int] = None,
) -> Data:
    """
    Parameters
    ----------
    office_id : str
        The ID of the project's office.
    id_mask : Optional[str]
        The project ID mask for projects to return.
    page : Optional[str]
        A string representing the page to retrieve.
        If None then the first page will be retrieved.
    page_size : Optional[int]
        An integer representing the number of items per page.

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

    endpoint = "projects"
    params = {
        "office": office_id,
        "id-mask": id_mask,
        "page": page,
        "page-size": page_size,
    }
    response = api.get(endpoint, params)
    return Data(response)


def delete_project(office_id: str, name: str, delete_method: DeleteMethod) -> None:
    """
    Parameters
    ----------
    name : str
        The name of the projects.
    office_id : str
        The ID of the project's office.
    delete_method: DeleteMethod
        The method to use to delete the project.

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
        raise ValueError("Delete project requires an project name")
    if office_id is None:
        raise ValueError("Delete project requires an office")
    if delete_method is None:
        raise ValueError("Delete project requires a delete method")

    endpoint = f"projects/{name}"
    params = {"office": office_id, "method": delete_method.name}
    api.delete(endpoint, params)


def rename_project(office_id: str, old_name: str, new_name: str) -> None:
    """
    Parameters
    ----------
    office_id : str
        The ID of the project's office.
    old_name : str
        The name of the project to rename.
    new_name : str
        The new name of the project.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of old_name, new_name,  or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if old_name is None:
        raise ValueError("Rename project requires the original project name")
    if new_name is None:
        raise ValueError("Rename project requires a new project name")
    if office_id is None:
        raise ValueError("Rename project requires an office")

    endpoint = f"projects/{old_name}"
    params = {"office": office_id, "name": new_name}
    api.patch(endpoint=endpoint, params=params)


def store_project(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """
    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        A boolean value indicating whether to fail if the project already exists.
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
        raise ValueError("Cannot store an project without a JSON data dictionary")

    endpoint = "projects"
    params = {"fail-if-exists": fail_if_exists}
    api.post(endpoint, data, params)
