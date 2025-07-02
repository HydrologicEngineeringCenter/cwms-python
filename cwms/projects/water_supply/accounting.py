#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_pump_accounting(
    office_id: str,
    project_id: str,
    water_user: str,
    contract_name: str,
    start: str,
    end: str,
    timezone: str = "UTC",
    unit: str = "cms",
    start_time_inclusive: bool = True,
    end_time_inclusive: bool = True,
    ascending: bool = True,
    row_limit: int = 0,
) -> Data:
    """
    Retrieves pump accounting entries associated with a water supply contract.

    Parameters
    ----------
    office_id : str
        The office ID the pump accounting is associated with. (Path)
    project_id : str
        The project ID the pump accounting is associated with. (Path)
    water_user : str
        The water user the pump accounting is associated with. (Path)
    contract_name : str
        The name of the contract associated with the pump accounting. (Path)
    start : str
        The start time of the time window for pump accounting entries to retrieve.
        Format: ISO 8601 extended, with optional offset and timezone. (Query)
    end : str
        The end time of the time window for pump accounting entries to retrieve.
        Format: ISO 8601 extended, with optional offset and timezone. (Query)
    timezone : str, optional
        The default timezone to use if `start` or `end` lacks offset/timezone info.
        Defaults to "UTC". (Query)
    unit : str, optional
        Unit of flow rate for accounting entries. Defaults to "cms". (Query)
    start_time_inclusive : bool, optional
        Whether the start time is inclusive. Defaults to True. (Query)
    end_time_inclusive : bool, optional
        Whether the end time is inclusive. Defaults to True. (Query)
    ascending : bool, optional
        Whether entries should be returned in ascending order. Defaults to True. (Query)
    row_limit : int, optional
        Maximum number of rows to return. Defaults to 0, meaning no limit. (Query)

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
    if not all([office_id, project_id, water_user, contract_name, start, end]):
        raise ValueError("All required parameters must be provided.")

    endpoint = f"projects/{office_id}/{project_id}/water-user/{water_user}/contracts/{contract_name}/accounting"

    params: dict[str, str | int] = {
        "start": start,
        "end": end,
        "timezone": timezone,
        "unit": unit,
        "start-time-inclusive": str(start_time_inclusive).lower(),
        "end-time-inclusive": str(end_time_inclusive).lower(),
        "ascending": str(ascending).lower(),
        "row-limit": row_limit,
    }

    response = api.get(endpoint, params, api_version=1)
    return Data(response)


def store_pump_accounting(
    office: str,
    project_id: str,
    water_user: str,
    contract_name: str,
    data: JSON,
) -> None:
    """
    Creates a new pump accounting entry associated with a water supply contract.

    Parameters
    ----------
    office : str
        The office ID the accounting is associated with. (Path)
    project_id : str
        The project ID the accounting is associated with. (Path)
    water_user : str
        The water user the accounting is associated with. (Path)
    contract_name : str
        The name of the contract associated with the accounting. (Path)
    data : dict
        A dictionary representing the JSON data to be stored. This should match the
        WaterSupplyAccounting structure as defined by the API.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any required argument is missing.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """
    if not all([office, project_id, water_user, contract_name]):
        raise ValueError(
            "Office, project_id, water_user, and contract_name must be provided."
        )
    if not data:
        raise ValueError("Data must be provided and cannot be empty.")

    endpoint = f"projects/{office}/{project_id}/water-user/{water_user}/contracts/{contract_name}/accounting"
    params = {
        "office": office,
        "project-id": project_id,
        "water-user": water_user,
        "contract-name": contract_name,
    }
    api.post(endpoint, data, params)
