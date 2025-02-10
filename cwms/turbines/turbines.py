from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_projects_turbines(office: str, projectId: str) -> Data:
    """Returns matching CWMS Turbine Data for a Reservoir Project. Get cwmsData projects turbines.
    Args:
        office (str): The office associated with the turbine data.
        projectId (str): The ID of the project.
    Returns:
        dict: A dictionary containing the turbine data.
    """
    endpoint = "projects/turbines"
    params = {"office": office, "project-id": projectId}

    response = api.get(endpoint=endpoint, params=params)

    return Data(response)


def get_projects_turbines_with_name(office: str, name: str) -> Data:
    """Returns CWMS Turbine Data Get cwmsData projects turbines with name.
    Args:
        office (str): The office associated with the turbine data.
        name (str): The name of the turbine.
    Returns:
        dict: A dictionary containing the turbine data.
    """
    endpoint = "projects/turbines"
    params = {"office": office, "name": name}
    response = api.get(endpoint=endpoint, params=params)
    return Data(response)


def get_projects_turbines_with_office_with_name_turbine_changes(
    name: str,
    begin: datetime,
    end: datetime,
    office: str,
    page_size: Optional[int],
    unit_system: Optional[dict],
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
        unit_system (Optional[dict]): The unit system to use for the data [SI, EN].
    Returns:
        dict: A dictionary containing the turbine data.
    """
    if begin and not isinstance(begin, datetime):
        raise ValueError("begin needs to be in datetime")
    if end and not isinstance(end, datetime):
        raise ValueError("end needs to be in datetime")

    endpoint = "projects/turbines"
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
    response = api.get(endpoint=endpoint, params=params)
    return Data(response)
