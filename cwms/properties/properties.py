#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_properties(
    office_mask: Optional[str] = None,
    category_id_mask: Optional[str] = None,
    name_mask: Optional[str] = None,
) -> Data:
    """
    Returns matching CWMS Property Data.

    Parameters
    ----------
        office_mask: string, optional
            Filters properties to the specified office mask
        category_id_mask: string, optional
            Filters properties to the specified category mask
        name_mask: string, optional
            Filters properties to the specified name mask

    Returns
    -------
        cwms data type
    """

    endpoint = "properties"
    params = {
        "office-mask": office_mask,
        "category-id-mask": category_id_mask,
        "name-mask": name_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_property(
    name: str, office: str, category_id: str, default_value: Optional[str] = None
) -> Data:
    """
    Returns CWMS Property Data.

    Parameters
    ----------
        name: string
            Specifies the name of the property to be retrieved.
        office: string
            Specifies the owning office of the property to be retrieved.
        category_id: string
            Specifies the category id of the property to be retrieved.
        default_value: string, optional
            Specifies the default value if the property does not exist.

    Returns
    -------
        cwms data type
    """

    endpoint = f"properties/{name}"
    params = {
        "office": office,
        "category-id": category_id,
        "default-value": default_value,
    }

    response = api.get(endpoint, params)
    return Data(response)


def create_property(data: JSON) -> None:
    """
    Create CWMS Property.

    Parameters
    ----------
        data: JSON dictionary
            Property data to be stored.
            Example:
            {
                "office-id": "string",
                "name": "string",
                "category": "string",
                "value": 0,
                "comment": "string
            }

    Returns
    -------
        None
    """

    endpoint = "properties"

    if data is None:
        raise ValueError("Cannot store a property without JSON data")

    return api.post(endpoint, data)


def update_property(name: str, data: JSON) -> None:
    """
    Update CWMS Property.

    Parameters
    ----------
        name: string
            Specifies the name of the property to be updated.
        data: JSON dictionary
            Property data to be updated.
            Example:
            {
                "office-id": "string",
                "name": "string",
                "category": "string",
                "value": 0,
                "comment": "string
            }

    Returns
    -------
        None
    """

    endpoint = f"properties/{name}"

    if name is None:
        raise ValueError("Must specify a property name to update")

    if data is None:
        raise ValueError("Cannot update a property without JSON data")

    return api.patch(endpoint, data)


def delete_property(name: str, office: str, category_id: str) -> None:
    """
    Delete CWMS Property.

    Parameters
    ----------
        name: string
            Specifies the name of the property to be deleted.
        office: string
            Specifies the owning office of the property to be deleted.
        category_id: string
            Specifies the category id of the property to be deleted.

    Returns
    -------
        None
    """

    endpoint = f"properties/{name}"

    params = {
        "office": office,
        "category-id": category_id,
    }

    return api.delete(endpoint, params)
