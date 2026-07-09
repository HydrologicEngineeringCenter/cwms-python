#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_property(
    office_id: str,
    category_id: str,
    name: str,
    default_value: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    office_id : str
        The ID of the office.
    category_id : str
        The category ID of the property.
    name : str
        The name of the property.
    default_value : Optional[str]
        The default value to return if the property does not exist.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of office_id, category_id, or name is None.
    ClientError
        If a 4xx range error code response is returned from the server.
    NoDataFoundError
        If a 404 error code response is returned from the server.
    ServerError
        If a 5xx range error code response is returned from the server.
    """

    if office_id is None:
        raise ValueError("Retrieve property requires an office")
    if category_id is None:
        raise ValueError("Retrieve property requires a category")
    if name is None:
        raise ValueError("Retrieve property requires a name")

    endpoint = f"properties/{name}"
    params = {
        "office": office_id,
        "category-id": category_id,
        "default-value": default_value,
    }
    response = api.get(endpoint, params)
    return Data(response)


def get_properties(
    office_mask: Optional[str] = None,
    category_id_mask: Optional[str] = None,
    name_mask: Optional[str] = None,
) -> Data:
    """
    Parameters
    ----------
    office_mask : Optional[str]
        The office mask for properties to return.
    category_id_mask : Optional[str]
        The category ID mask for properties to return.
    name_mask : Optional[str]
        The property name mask for properties to return.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ClientError
        If a 4xx range error code response is returned from the server.
    NoDataFoundError
        If a 404 error code response is returned from the server.
    ServerError
        If a 5xx range error code response is returned from the server.
    """

    endpoint = "properties"
    params = {
        "office-mask": office_mask,
        "category-id-mask": category_id_mask,
        "name-mask": name_mask,
    }
    response = api.get(endpoint, params)
    return Data(response)


def store_property(data: JSON) -> None:
    """
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
        If data is None.
    ClientError
        If a 4xx range error code response is returned from the server.
    NoDataFoundError
        If a 404 error code response is returned from the server.
    ServerError
        If a 5xx range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Cannot store a property without a JSON data dictionary")

    endpoint = "properties"
    api.post(endpoint, data)


def update_property(name: str, data: JSON) -> None:
    """
    Parameters
    ----------
    name : str
        The name of the property to update.
    data : dict
        A dictionary representing the JSON data to be updated.
        If the `data` value is None, a `ValueError` will be raised.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If name or data is None.
    ClientError
        If a 4xx range error code response is returned from the server.
    NoDataFoundError
        If a 404 error code response is returned from the server.
    ServerError
        If a 5xx range error code response is returned from the server.
    """

    if name is None:
        raise ValueError("Update property requires a name")
    if data is None:
        raise ValueError("Cannot update a property without a JSON data dictionary")

    endpoint = f"properties/{name}"
    api.patch(endpoint, data)


def delete_property(office_id: str, category_id: str, name: str) -> None:
    """
    Parameters
    ----------
    office_id : str
        The ID of the office.
    category_id : str
        The category ID of the property.
    name : str
        The name of the property.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of office_id, category_id, or name is None.
    ClientError
        If a 4xx range error code response is returned from the server.
    NoDataFoundError
        If a 404 error code response is returned from the server.
    ServerError
        If a 5xx range error code response is returned from the server.
    """

    if office_id is None:
        raise ValueError("Delete property requires an office")
    if category_id is None:
        raise ValueError("Delete property requires a category")
    if name is None:
        raise ValueError("Delete property requires a name")

    endpoint = f"properties/{name}"
    params = {"office": office_id, "category-id": category_id}
    api.delete(endpoint, params)
