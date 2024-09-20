#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_forecast_instances(
    spec_id: Optional[str] = None,
    office: Optional[str] = None,
    designator: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    spec_id : str, optional
        The forecast spec id.
    office : str, optional
        The spec office id.
    designator : str, optional
        The spec designator.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of spec_id, office, or designator is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """
    if spec_id is None:
        raise ValueError("Retrieving forecast instances requires an id")
    if office is None:
        raise ValueError("Retrieving forecast instances requires an office")
    if designator is None:
        raise ValueError("Retrieving forecast instances requires a designator")
    endpoint = "forecast-instance"

    params = {
        "office": office,
        "name": spec_id,
        "designator": designator,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_forecast_instance(
    spec_id: str,
    office: str,
    designator: str,
    forecast_date: datetime,
    issue_date: datetime,
) -> Data:
    """
    Parameters
    ----------
    spec_id : str
        The ID of the forecast spec.
    office : str
        The ID of the office.
    designator : str
        The designator of the forecast spec

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of spec_id, office, or designator is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if spec_id is None:
        raise ValueError("Retrieve forecast instance requires an id")
    if office is None:
        raise ValueError("Retrieve a forecast instance requires an office")
    if designator is None:
        raise ValueError("Retrieve a forecast instance requires a designator")
    if forecast_date is None:
        raise ValueError("Retrieve a forecast instance requires a forecast date")
    if issue_date is None:
        raise ValueError("Retrieve a forecast instance requires a issue date")

    endpoint = f"forecast-instance/{spec_id}"

    params = {
        "office": office,
        "designator": designator,
        "forecast-date": forecast_date.isoformat(),
        "issue-date": issue_date.isoformat(),
    }

    response = api.get(endpoint, params)
    return Data(response)


def store_forecast_instance(data: JSON) -> None:
    """
    This method is used to store a forecast instance through CWMS Data API.

    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If dict is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """
    if data is None:
        raise ValueError("Storing a forecast instance requires a JSON data dictionary")
    endpoint = "forecast-instance"

    return api.post(endpoint, data, params=None)


def delete_forecast_instance(
    spec_id: str,
    office: str,
    designator: str,
    forecast_date: datetime,
    issue_date: datetime,
) -> None:
    """
    Parameters
    ----------
    spec_id : str
        The ID of the forecast spec.
    office : str
        The ID of the office.
    designator : str
        The designator of the forecast spec
    forecast_date : datetime
        The forecast date of the forecast instance
    issue_date : datetime
        The forecast issue date of the forecast instance

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of spec_id, office, designator,
        forecast_date, or issue_date is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """
    if spec_id is None:
        raise ValueError("Deleting a forecast instance requires an id")
    if office is None:
        raise ValueError("Deleting a forecast instance requires an office")
    if designator is None:
        raise ValueError("Deleting a forecast instance requires a designator")
    if forecast_date is None:
        raise ValueError("Deleting a forecast instance requires a forecast date")
    if issue_date is None:
        raise ValueError("Deleting a forecast instance requires a issue date")

    endpoint = f"forecast-instance/{spec_id}"

    params = {
        "office": office,
        "designator": designator,
        "forecast-date": forecast_date.isoformat(),
        "issue-date": issue_date.isoformat(),
    }
    return api.delete(endpoint, params)
