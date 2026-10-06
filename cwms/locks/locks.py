#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_lock(name: str, office_id: str, unit: Optional[str] = "SI") -> Data:
    """
    Get a specified lock with the given office and name.

    Parameters
    ----------
    office_id : str
        The office ID of the lock to retrieve. (Query)
    name : str
        The name of the lock to retrieve. (Query)
    unit : str
        The unit system to use for the response. Defaults to "SI". (Query)

    Returns
    -------
    Data
        The JSON response from CWMS Data API wrapped in a Data object.

    Raises
    ------
    ValueError
        If any required path parameters are None.
    ClientError
        If a 400-level error occurs.
    NoDataFoundError
        If a 404-level error occurs.
    ServerError
        If a 500-level error occurs.
    """
    if not all([office_id, name]):
        raise ValueError("Office and Name must be provided.")

    endpoint = f"projects/locks/{name}"

    params = {"office": office_id, "unit": unit}

    response = api.get(endpoint, params, api_version=1)
    return Data(response)


def get_locks(office_id: str, project_id: str) -> Data:
    """
    Get all locks for the given office and project.

    Parameters
    ----------
    office_id : str
        The office ID of the locks to retrieve. (Query)
    project_id : str
        The project ID of the locks to retrieve. (Query)

    Returns
    -------
    Data
        The JSON response from CWMS Data API wrapped in a Data object.

    Raises
    ------
    ValueError
        If any required path parameters are None.
    ClientError
        If a 400-level error occurs.
    NoDataFoundError
        If a 404-level error occurs.
    ServerError
        If a 500-level error occurs.
    """
    if not all([office_id, project_id]):
        raise ValueError("Office and Project ID must be provided.")

    endpoint = "projects/locks"

    params = {"office": office_id, "project-id": project_id}

    response = api.get(endpoint, params, api_version=1)
    return Data(response)


def create_lock(data: JSON, fail_if_exists: bool = True) -> None:
    """
    Create CWMS Lock.

    Parameters
    ----------
    data : JSON
        Lock successfully stored to CWMS. (Body)
    fail_if_exists : bool, optional
        Create will fail if provided ID already exists. Default: True. (Query)

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any required parameters are None.
    ClientError
        If a 400-level error occurs.
    NoDataFoundError
        If a 404-level error occurs.
    ServerError
        If a 500-level error occurs.
    """
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = "projects/locks"
    params = {"fail-if-exists": fail_if_exists}

    api.post(endpoint, data, params, api_version=1)


def delete_lock(
    name: str, office_id: str, method: DeleteMethod = DeleteMethod.DELETE_KEY
) -> None:
    """
    Delete CWMS Lock

    Parameters
    ----------
    name : str
        Specifies the name of the lock to be deleted. (Path)
    office_id : str
        Specifies the owning office of the lock to be deleted. (Query)
    method : DeleteMethod, optional
        Specifies the delete method used. Defaults to "DELETE_KEY". (Query)

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any required parameters are None.
    ClientError
        If a 400-level error occurs.
    NoDataFoundError
        If a 404-level error occurs.
    ServerError
        If a 500-level error occurs.
    """
    if not all([name, office_id]):
        raise ValueError("Name and Office ID must be provided.")

    endpoint = f"projects/locks/{name}"
    params = {"office": office_id, "method": method.name}

    api.delete(endpoint, params, api_version=1)


def update_lock(name: str, office_id: str, new_name: str) -> None:
    """
    Rename CWMS Lock

    Parameters
    ----------
    name : str
        Specifies the name of the lock to be renamed. (Path)
    office_id : str
        Specifies the owning office of the lock to be renamed. (Query)
    new_name : str
        Specifies the new lock name. (Query)

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any required parameters are None.
    ClientError
        If a 400-level error occurs.
    NoDataFoundError
        If a 404-level error occurs.
    ServerError
        If a 500-level error occurs.
    """
    if not all([name, office_id, new_name]):
        raise ValueError("Name, Office ID, and New Name must be provided.")

    endpoint = f"projects/locks/{name}"
    params = {"office": office_id, "name": new_name}

    api.patch(endpoint, None, params, api_version=1)
