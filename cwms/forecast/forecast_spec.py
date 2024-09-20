#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def get_forecast_specs(
    id_mask: Optional[str] = None,
    office: Optional[str] = None,
    designator_mask: Optional[str] = None,
    source_entity: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    id_mask : str, optional
        The regex filter for the forecast spec id.
    office : str, optional
        The regex filter for the forecast spec id.
    designator_mask : str, optional
        The regex filter for the forecast spec id.
    source_entity : str, optional
        The regex filter for the forecast spec id.

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
    endpoint = "forecast-spec"

    params = {
        "office": office,
        "id_mask": id_mask,
        "designator-mask": designator_mask,
        "source-entity": source_entity,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_forecast_spec(spec_id: str, office: str, designator: str) -> Data:
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
        raise ValueError("Retrieve forecast spec requires an id")
    if office is None:
        raise ValueError("Retrieve a forecast spec requires an office")
    if designator is None:
        raise ValueError("Retrieve a forecast spec requires a designator")

    endpoint = f"forecast-spec/{spec_id}"

    params = {
        "office": office,
        "designator": designator,
    }

    response = api.get(endpoint, params)
    return Data(response)


def store_forecast_spec(data: JSON) -> None:
    """
    This method is used to store a forecast spec through CWMS Data API.

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
        raise ValueError("Storing a forecast spec requires a JSON data dictionary")
    endpoint = "forecast-spec"

    return api.post(endpoint, data)


def delete_forecast_spec(
    spec_id: str,
    office: str,
    designator: str,
    delete_method: DeleteMethod,
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
    delete_method: DeleteMethod
        The method to use to delete forecast spec data

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
        raise ValueError("Deleting a forecast spec requires an id")
    if office is None:
        raise ValueError("Deleting a forecast spec requires an office")
    if designator is None:
        raise ValueError("Deleting a forecast spec requires a designator")

    endpoint = f"forecast-spec/{spec_id}"
    params = {
        "office": office,
        "designator": designator,
        "method": delete_method.name,
    }
    return api.delete(endpoint, params)
