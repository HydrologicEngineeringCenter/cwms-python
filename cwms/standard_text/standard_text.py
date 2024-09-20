#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data, DeleteMethod


def standard_text_to_json(text_id: str, standard_text: str, office_id: str) -> JSON:
    """
    Created the re  required JSON dictionary to store a standard text.

    Parameters
    ----------
    text_id : str
        The id to store the standard text under
    office_id : str
        the office that the standard text id belongs to
    standard_text: str
        The standard text to store in under the text_id

    Returns
    -------
    JSON dictionary that can be stored using store_standard_text
    """

    if text_id is None:
        raise ValueError("Cannot store a standard text without a text id")
    if standard_text is None:
        raise ValueError("Cannot store a standard text without a standard_text message")
    if office_id is None:
        raise ValueError("Cannot store a standard text without an office_id")

    st_dict = {
        "id": {"office-id": f"{office_id}", "id": f"{text_id}"},
        "standard-text": f"{standard_text}",
    }
    return st_dict


def get_standard_text_catalog(
    text_id_mask: Optional[str] = None, office_id_mask: Optional[str] = None
) -> Data:
    """
    Retrieves standard text catalog for the given ID and office ID filters.

    Parameters
    ----------
    text_id_mask : str
        The ID filter of the standard text value to retrieve.
    office_id_mask : str
        The ID filter of the office that the standard text belongs to.

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

    endpoint = "standard-text-id"
    params = {"text-id-mask": text_id_mask, "office-id-mask": office_id_mask}

    response = api.get(endpoint, params)
    return Data(response)


def get_standard_text(text_id: str, office_id: str) -> Data:
    """
    Retrieves standard text for the given ID and office ID.

    Parameters
    ----------
    text_id : str
        The ID of the standard text value to retrieve.
    office_id : str
        The ID of the office that the standard text belongs to.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    Raises
    ------
    ValueError
        If any of timeseries_id, office_id, begin, or end is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if text_id is None:
        raise ValueError("Retrieving standard text requires an id")
    if office_id is None:
        raise ValueError("Retrieving standard timeseries requires an office")

    endpoint = f"standard-text-id/{text_id}"
    params = {"office": office_id}

    response = api.get(endpoint, params)
    return Data(response)


def delete_standard_text(
    text_id: str, delete_method: DeleteMethod, office_id: str
) -> None:
    """
    Deletes standard text for the given ID and office ID.

    Parameters
    ----------
    text_id : str
        The ID of the standard text value to be deleted.
    office_id : str
        The ID of the office that the standard text belongs to.
    delete_method : str
        Delete method for the standard text id.
        DELETE_ALL - deletes the key and the value from the clob table
        DELETE_KEY - deletes the text id key, but leaves the value in the clob table
        DELETE_DATA - deletes the value from the clob table but leaves the text id key

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any of timeseries_id, office_id, begin, or end is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if text_id is None:
        raise ValueError("Deleting standard text requires an id")
    if office_id is None:
        raise ValueError("Deleting standard timeseries requires an office")
    if delete_method is None:
        raise ValueError("Deleting standard timeseries requires a delete method")

    endpoint = f"standard-text-id/{text_id}"
    params = {"office": office_id, "method": delete_method.name}

    return api.delete(endpoint, params)


def store_standard_text(data: JSON, fail_if_exists: bool = False) -> None:
    """
    This method is used to store a standard text value through CWMS Data API.

    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        Throw a ClientError if the text id already exists.
        Default is `False`.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If either dict is None.
    ClientError
        If a 400 range error code response is returned from the server.
    NoDataFoundError
        If a 404 range error code response is returned from the server.
    ServerError
        If a 500 range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Cannot store a standard text without a JSON data dictionary")

    endpoint = "standard-text-id"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params)
