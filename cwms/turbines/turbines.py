from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data

# ==========================================================================
#                             GET CWMS TURBINES
# ==========================================================================


def get_project_turbines(office: str, project_id: str) -> Data:
    """Returns matching CWMS Turbine Data for a Reservoir Project. Get cwmsData projects turbines.
    Args:
        office (str): The office associated with the turbine data.
        project_id (str): The ID of the project.
    Returns:
        dict: A dictionary containing the turbine data.
    """
    endpoint = "projects/turbines"
    params = {"office": office, "project-id": project_id}

    response = api.get(endpoint=endpoint, params=params, api_version=1)

    return Data(response)


def get_project_turbine(office: str, name: str) -> Data:
    """Returns CWMS Turbine Data Get cwmsData projects turbines with name.
    Args:
        office (str): The office associated with the turbine data.
        name (str): The name of the turbine.
    Returns:
        dict: A dictionary containing the turbine data.
    """
    endpoint = f"projects/turbines/{name}"
    params = {"office": office}
    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response)


def get_project_turbine_changes(
    name: str,
    begin: datetime,
    end: datetime,
    office: str,
    page_size: Optional[int],
    unit_system: Optional[str],
    start_time_inclusive: Optional[bool],
    end_time_inclusive: Optional[bool],
) -> Data:
    """
    Returns CWMS Turbine Data for projects with specified office and turbine name changes within a given time range.
    Args:
        begin (str): The start date and time for the data retrieval in ISO 8601 format.
        end (str): The end date and time for the data retrieval in ISO 8601 format.
        end_time_inclusive (Optional[bool]): Whether the end time is inclusive.
        name (str): The name of the turbine.
        office (str): The office associated with the turbine data.
        page_size (Optional[int]): The number of records to return per page.
        start_time_inclusive (Optional[bool]): Whether the start time is inclusive.
        unit_system (Optional[str]): The unit system to use for the data [SI, EN].
    Returns:
        dict: A dictionary containing the turbine data.
    """
    if begin and not isinstance(begin, datetime):
        raise ValueError("begin needs to be in datetime")
    if end and not isinstance(end, datetime):
        raise ValueError("end needs to be in datetime")

    endpoint = f"projects/{office}/{name}/turbine-changes"
    params = {
        "name": name,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "office": office,
        "page-size": page_size,
        "unit-system": unit_system,
        "start-time-inclusive": start_time_inclusive,
        "end-time-inclusive": end_time_inclusive,
    }
    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response)


# ==========================================================================
#                             POST CWMS TURBINES
# ==========================================================================


def store_project_turbine(data: JSON, fail_if_exists: Optional[bool]) -> None:
    """
    Create a new turbine in CWMS.
    Parameters
    ----------
    fail_if_exists (bool): If True, the request will fail if the turbine already exists.

    Returns
    -------
    None


    Raises
    ------
    ValueError
        If provided data is None
    Unauthorized
        401 - Indicates that the client request has not been completed because it lacks valid authentication credentials for the requested resource.
    Forbidden
        403 - Indicates that the server understands the request but refuses to authorize it.
    Not Found
        404 - Indicates that the server cannot find the requested resource.
    Server Error
        500 - Indicates that the server encountered an unexpected condition that prevented it from fulfilling the request.
    """
    if data is None:
        raise ValueError(
            "Cannot store project turbine changes without a JSON data dictionary"
        )
    endpoint = "projects/turbines"
    params = {
        "fail-if-exists": fail_if_exists,
    }
    return api.post(endpoint=endpoint, data=data, params=params, api_version=1)


def store_project_turbine_changes(
    data: JSON, office: str, name: str, override_protection: Optional[bool]
) -> None:
    """
    Create CWMS Turbine Changes
    Parameters
    ----------
    office (str): Office id for the reservoir project location associated with the turbine changes.
    name (str): Specifies the name of project of the Turbine changes whose data is to stored.
    override_protection (bool): A flag ('True'/'False') specifying whether to delete protected data. Default is False

    Returns
    -------
    None - Turbine successfully stored to CWMS.


    Raises
    ------
    ValueError
        If provided data is None
    Unauthorized
        401 - Indicates that the client request has not been completed because it lacks valid authentication credentials for the requested resource.
    Forbidden
        403 - Indicates that the server understands the request but refuses to authorize it.
    Not Found
        404 - Indicates that the server cannot find the requested resource.
    Server Error
        500 - Indicates that the server encountered an unexpected condition that prevented it from fulfilling the request.
    """
    if data is None:
        raise ValueError(
            "Cannot store project turbine changes without a JSON data dictionary"
        )
    endpoint = f"projects/{office}/{name}/turbine-changes"
    params = {"override-protection": override_protection}
    return api.post(endpoint=endpoint, data=data, params=params, api_version=1)


# ==========================================================================
#                             DELETE CWMS TURBINES
# ==========================================================================


def delete_project_turbine(name: str, office: str, method: Optional[str]) -> None:
    """
    Delete CWMS Turbine
    Parameters
    ----------
    name (str): Specifies the name of the turbine to be deleted.
    office (str): Specifies the owning office of the turbine to be deleted.
    method (str): Specifies the delete method used. Defaults to "DELETE_KEY". Options are: DELETE_KEY, DELETE_DATA, DELETE_ALL
    Returns
    -------
    None - Turbine successfully deleted from CWMS.


    Raises
    ------
    ValueError
        If provided data is None
    Unauthorized
        401 - Indicates that the client request has not been completed because it lacks valid authentication credentials for the requested resource.
    Forbidden
        403 - Indicates that the server understands the request but refuses to authorize it.
    Not Found
        404 - Indicates that the server cannot find the requested resource.
    Server Error
        500 - Indicates that the server encountered an unexpected condition that prevented it from fulfilling the request.
    """
    endpoint = f"projects/turbines/{name}"
    params = {"office": office, "method": method}
    return api.delete(endpoint=endpoint, params=params, api_version=1)


def delete_project_turbine_changes(
    office: str,
    name: str,
    begin: datetime,
    end: datetime,
    override_protection: Optional[bool],
) -> None:
    """
    Delete CWMS Turbine Changes
    Parameters
    ----------
    name (str): Specifies the name of project for the turbine changes to be deleted.
    office (str): Specifies the owning office of the turbine to be deleted.
    begin (datetime): The start of the time window
    end (datetime): The end of the time window
    override_protection (bool): A flag ('True'/'False') specifying whether to delete protected data. Default is False

    Returns
    -------
    None - Turbine successfully deleted from CWMS.


    Raises
    ------
    ValueError
        If provided data is None
    Unauthorized
        401 - Indicates that the client request has not been completed because it lacks valid authentication credentials for the requested resource.
    Forbidden
        403 - Indicates that the server understands the request but refuses to authorize it.
    Not Found
        404 - Indicates that the server cannot find the requested resource.
    Server Error
        500 - Indicates that the server encountered an unexpected condition that prevented it from fulfilling the request.
    """
    endpoint = f"projects/{office}/{name}/turbine-changes"
    params = {
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "override-protection": override_protection,
    }
    return api.delete(endpoint=endpoint, params=params, api_version=1)
