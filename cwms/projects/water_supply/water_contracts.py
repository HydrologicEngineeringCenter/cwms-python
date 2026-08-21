#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_water_contract(
    office_id: str, project_id: str, water_user: str, contract_name: str
) -> Data:
    """
    Return a specified water contract

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)
    contract_name : str
        The name of the contract to retrieve. (Path)

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
    if not all([office_id, project_id, water_user, contract_name]):
        raise ValueError(
            "Office, project_id, water_user, and contract_name must be provided."
        )

    endpoint = f"projects/{office_id}/{project_id}/water-users/{water_user}/contracts/{contract_name}"

    response = api.get(endpoint, api_version=1)
    return Data(response)


def get_water_contracts(office_id: str, project_id: str, water_user: str) -> Data:
    """
    Return all water contracts for the specified water user, project, and office

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

    endpoint = f"projects/{office_id}/{project_id}/water-users/{water_user}/contracts"

    response = api.get(endpoint, api_version=1)
    return Data(response)


def create_water_contract(
    office_id: str,
    project_id: str,
    water_user: str,
    data: JSON,
    fail_if_exists: bool = True,
    ignore_nulls: bool = False,
) -> None:
    """
    Create a new water contract

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)
    data : JSON
        Water contract successfully stored to CWMS. (Body)
    fail_if_exists : bool, optional
        If true, the contract will not be stored if it already exists.
        Default: True (Query)
    ignore_nulls : bool, optional
        If true, null fields will be ignored when storing the contract.
        Default: False (Query)

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
        raise ValueError("Office, project_id, and water_user must be provided.")
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}/contracts"
    params = {
        "fail-if-exists": fail_if_exists,
        "ignore-nulls": ignore_nulls,
    }

    api.post(endpoint, data, params, api_version=1)


def delete_water_contract(
    office_id: str,
    project_id: str,
    water_user: str,
    contract_name: str,
    method: DeleteMethod = DeleteMethod.DELETE_KEY,
) -> None:
    """
    Delete a specified water contract

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)
    contract_name : str
        The name of the contract to be deleted. (Path)
    method : DeleteMethod, optional
        Specifies the delete method used. Defaults to DELETE_KEY. (Query)

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
    if not all([office_id, project_id, water_user, contract_name]):
        raise ValueError(
            "Office, Project ID, Water User, and Contract Name must be provided."
        )

    endpoint = f"projects/{office_id}/{project_id}/water-users/{water_user}/contracts/{contract_name}"
    params = {"method": method.name}

    api.delete(endpoint, params, api_version=1)


def update_water_contract(
    office_id: str,
    project_id: str,
    water_user: str,
    contract_name: str,
    new_contract_name: str,
    data: JSON,
) -> None:
    """
    Updates a water contract in CWMS.

    Parameters
    ----------
    office_id : str
        The office Id the contract is associated with. (Path)
    project_id : str
        The project Id the contract is associated with. (Path)
    water_user : str
        The water user the contract is associated with. (Path)
    contract_name : str
        The name of the contract to be updated. (Path)
    new_contract_name : str
        The new name of the contract. (Query)
    data : JSON
        Water contract successfully updated in CWMS. (Body)

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
    if not all([office_id, project_id, water_user, contract_name, new_contract_name]):
        raise ValueError(
            "Office, Project ID, Contract Name, New Contract Name, and Water User must be provided."
        )
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}/contracts/{contract_name}"

    params = {"contract-name": new_contract_name}

    api.patch(endpoint, data, params, api_version=1)
