#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_water_user(office_id: str, project_id: str, water_user: str) -> Data:
    """
    Gets a specified water user.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)

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
    if not all([office_id, project_id, water_user]):
        raise ValueError("Office, project_id, and water_user must be provided.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}"

    response = api.get(endpoint, api_version=1)
    return Data(response)


def get_water_users(office_id: str, project_id: str) -> Data:
    """
    Gets a specified water user.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)

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
    if not all([office_id, project_id, water_user]):
        raise ValueError("Office and Project ID must be provided.")

    endpoint = f"projects/{office_id}/{project_id}/water-users"

    response = api.get(endpoint, api_version=1)
    return Data(response)


def create_water_user(
    data: JSON, office_id: str, project_id: str, fail_if_exists: bool = True
) -> None:
    """
    Stores a water user to CWMS.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    data : JSON
        Water user successfully stored to CWMS. (Body)
    fail_if_exists : bool, optional
        If true, the operation will fail if the water user already exists.
        Default: true (Query)

    Returns
    -------
    None

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
        raise ValueError("Office and project_id must be provided.")
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = f"projects/{office_id}/{project_id}/water-user"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=1)


def delete_water_user(office_id: str, project_id: str, water_user: str) -> None:
    """
    Deletes a specified water user.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)

    Returns
    -------
    None

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
    if not all([office_id, project_id, water_user]):
        raise ValueError("Office, Project ID, and Water User must be provided.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}"

    response = api.delete(endpoint, api_version=1)
    return Data(response)


def update_water_user(
    office_id: str, project_id: str, water_user: str, data: JSON, name: str
) -> Data:
    """
    Updates a water user in CWMS.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)
    data : JSON
        Water user entity data in JSON format. (Body)
    name : str
        Specifies the new name of the water user entity. (Query)

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
    if not all([office_id, project_id, water_user, name]):
        raise ValueError("Office, Project ID, Water User, and Name must be provided.")
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}"
    params = {"name": name}

    response = api.patch(endpoint, data, params, api_version=1)
    return Data(response)
