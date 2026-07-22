#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_project_lock_rights(
    office_mask: str,
    project_mask: Optional[str] = None,
    application_mask: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    office_mask : str
        Specifies the office mask to be used
        to filter the lock revoker rights.
    project_mask : Optional[str]
        Specifies the project mask to be used
        to filter the lock revoker rights.
    application_mask : Optional[str]
        Specifies the application mask to be used
        to filter the lock revoker rights.

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

    endpoint = "project-lock-rights"
    params = {
        "office-mask": office_mask,
        "project-mask": project_mask,
        "application-mask": application_mask,
    }
    response = api.get(endpoint, params)
    return Data(response)


def remove_all_project_lock_rights(
    office_id: str, application_id: str, user_id: str
) -> None:
    """
    Parameters
    ----------
    office_id : str
        Specifies the session office.
    application_id : str
        Specifies the application id.
    user_id : str
        Specifies the user.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of office_id, application_id, or user_id is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if office_id is None:
        raise ValueError("Remove project lock rights requires an office")
    if application_id is None:
        raise ValueError("Remove project lock rights requires an application")
    if user_id is None:
        raise ValueError("Remove project lock rights requires a user")

    endpoint = "project-lock-rights/remove-all"
    params = {"office": office_id, "application-id": application_id, "user-id": user_id}
    api.post(endpoint, None, params)


def update_project_lock_rights(
    office_id: str,
    application_id: str,
    user_id: str,
    allow: bool,
    project_mask: Optional[str] = None,
) -> None:
    """
    Parameters
    ----------
    office_id : str
        The ID of the office owning the project lock
    application_id : str
        Specifies the application id.
    user_id : str
        Specifies the user.
    allow : bool
        True to add the user to the allow list, False to add to the deny list
    project_mask : str
        Specifies the project mask to be used.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of office_id, application_id, user_id, or allow is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if office_id is None:
        raise ValueError("Update project lock rights requires an office")
    if application_id is None:
        raise ValueError("Update project lock rights requires an application")
    if user_id is None:
        raise ValueError("Update project lock rights requires a user")
    if allow is None:
        raise ValueError("Update project lock rights requires a allow flag")

    endpoint = "project-lock-rights/update"
    params = {
        "office": office_id,
        "application-id": application_id,
        "user-id": user_id,
        "allow": allow,
        "project-mask": project_mask,
    }
    api.post(endpoint, None, params)
