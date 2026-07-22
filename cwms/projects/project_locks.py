#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_project_lock(office_id: str, name: str, application_id: str) -> Data:
    """
    Parameters
    ----------
    name : str
        The ID of the project.
    office_id : str
        The ID of the office.
    application_id : str
        The ID of the application with the lock.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of name, application_id or office_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Retrieve project lock requires a name")
    if office_id is None:
        raise ValueError("Retrieve project requires an office")
    if application_id is None:
        raise ValueError("Retrieve project requires an application")

    endpoint = f"project-locks/{name}"
    params = {"office": office_id, "application-id": application_id}
    response = api.get(endpoint, params)
    return Data(response)


def get_project_locks(
    office_mask: str,
    project_mask: Optional[str] = None,
    application_mask: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    office_mask : str
        Specifies the office mask to be used to filter the locks.
    project_mask : Optional[str]
        Specifies the project mask to be used to filter the locks.
    application_mask : Optional[str]
        Specifies the application mask to be used to filter the locks.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If office_mask is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    endpoint = "project-locks"
    params = {
        "office-mask": office_mask,
        "project-mask": project_mask,
        "application-mask": application_mask,
    }
    response = api.get(endpoint, params)
    return Data(response)


def revoke_project_lock(
    office_id: str, name: str, revoke_timeout_seconds: Optional[int] = None
) -> None:
    """
    Parameters
    ----------
    name : str
        The name of the project.
    office_id : str
        The ID of the project's office.
    revoke_timeout_seconds: DeleteMethod
        time in seconds to wait for existing lock to be revoked.

    Returns
    -------
    None

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
        raise ValueError("Delete project requires an project name")
    if office_id is None:
        raise ValueError("Delete project requires an office")

    endpoint = f"project-locks/{name}"
    params = {"office": office_id, "revoke-timeout": revoke_timeout_seconds}
    api.delete(endpoint, params)


def request_project_lock(
    data: JSON,
    revoke_existing: Optional[bool] = False,
    revoke_timeout_seconds: Optional[int] = None,
) -> None:
    """
    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    revoke_existing : str, optional
        If an existing lock is found should a revoke be attempted
    revoke_timeout_seconds : str, optional
        time in seconds to wait for existing lock to be revoked.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If data is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Cannot require a project lock without a JSON data dictionary")

    endpoint = "project-locks"
    params = {
        "revoke-existing": revoke_existing,
        "revoke-timeout": revoke_timeout_seconds,
    }
    api.post(endpoint, data, params)


def deny_project_lock_request(lock_id: str) -> None:
    """
    Parameters
    ----------
    lock_id : str
        The ID of the requested project lock

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If lock_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if lock_id is None:
        raise ValueError("Cannot deny a project lock request without a lock id")

    endpoint = "project-locks/deny"
    params = {"lock-id": lock_id}
    api.post(endpoint, None, params)


def release_project_lock(office_id: str, lock_id: str) -> None:
    """
    Parameters
    ----------
    office_id : str
        The ID of the office owning the project lock
    lock_id : str
        The ID of the requested project lock

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If lock_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if office_id is None:
        raise ValueError("Release project lock requires an office")
    if lock_id is None:
        raise ValueError("Release project lock requires a lock id")

    endpoint = "project-locks/release"
    params = {"office": office_id, "lock-id": lock_id}
    api.post(endpoint, None, params)
