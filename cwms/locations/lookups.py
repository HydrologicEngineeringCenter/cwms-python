#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import cwms.api as api
from cwms.cwms_types import JSON, Data

ENDPOINT = "lookup-types"


def get_all_lookups(category: str, prefix: str, office_id: str) -> Data:
    """
    Retrieves all lookups for a given category, prefix, and office.

    Parameters
    ----------
    category : str
        Filters lookup types to the specified category
    prefix : str
        Filters lookup types to the specified prefix
    office_id : str
        Filters lookup types to the specified office ID

    Returns
    -------
    Data
        The JSON response from CWMS Data API wrapped in a Data object.

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
    if not all([category, prefix, office_id]):
        raise ValueError("Category, Prefix, and Office ID must be specified")

    params = {"category": category, "prefix": prefix, "office": office_id}
    response = api.get(ENDPOINT, params, api_version=1)
    return Data(response)


def create_lookup(data: JSON, category: str, prefix: str) -> None:
    """
    Creates a new lookup entry.

    Parameters
    ----------
    data: JSON
        A dictionary representing the JSON data to be stored. This should match the
        LookupType structure as defined by the API.
    category : str
        Specifies the category of the lookup.
    prefix : str
        Specifies the prefix of the lookup.

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
    if not all([category, prefix]):
        raise ValueError("Category and Prefix must be specified")
    if not data:
        raise ValueError("Data must be specified")
    params = {"category": category, "prefix": prefix}
    api.post(ENDPOINT, data, params, api_version=1)


def update_lookup(data: JSON, category: str, prefix: str) -> None:
    """
    Updates a specified lookup entry.

    Parameters
    ----------
    data : JSON
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    category : str
        Specifies the category of the lookup.
    prefix : str
        Specifies the prefix of the lookup.

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
    if not all([category, prefix]):
        raise ValueError("Category and Prefix must be specified")
    if not data:
        raise ValueError("Data must be specified")

    # Note that the path parameter is unused in CDA
    endpoint = f"{ENDPOINT}/{category}"
    params = {"category": category, "prefix": prefix}
    api.patch(endpoint, data, params, api_version=1)


def delete_lookup(category: str, prefix: str, office_id: str) -> None:
    """
    Deletes a specified lookup entry.

    Parameters
    ----------
    category : str
        Specifies the category id of the lookup type to be deleted.
    prefix : str
        Specifies the prefix of the lookup type to be deleted.
    office_id : str
        Specifies the owning office of the lookup type to be deleted.

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
    if not all([category, prefix, office_id]):
        raise ValueError("Category, Prefix, and Office ID must be specified")

    endpoint = f"{ENDPOINT}/{category}"
    params = {"category": category, "prefix": prefix, "office": office_id}
    api.delete(endpoint, params, api_version=1)
