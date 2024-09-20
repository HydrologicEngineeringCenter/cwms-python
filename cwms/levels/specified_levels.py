#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_specified_levels(
    specified_level_mask: Optional[str] = "*", office_id: Optional[str] = "*"
) -> Data:
    """
    Retrieve JSON data for multiple specified levels from CWMS Data API.

    Parameters
    ----------
    specified_level_mask : str
        The mask to filter the specified levels. Default value is "*" to include all offices.
    office_id : str
        The office for the specified levels. Default value is "*" in order to include all offices.

    Returns
    -------
    response : dict
        the JSON response from CWMS Data API.

    """

    endpoint = "specified-levels"

    params = {
        "office": office_id,
        "template-id-mask": specified_level_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


def store_specified_level(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """
    This method is used to store a specified level through CWMS Data API.

    Parameters
    ----------
    data : dict
        A dictionary representing the JSON data to be stored.
        If the `data` value is None, a `ValueError` will be raised.
    fail_if_exists : str, optional
        A boolean value indicating whether to fail if the specified level entry already exists.
        Default is True.

    Returns
    -------
    None
    """
    if data is None:
        raise ValueError(
            "Cannot store a specified level without a JSON data dictionary"
        )
    endpoint = "specified-levels"

    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params)


def delete_specified_level(specified_level_id: str, office_id: str) -> None:
    """
    Deletes a specified level with the given ID and office ID.

    Parameters
    ----------
    specified_level_id : str
        The ID of the specified level to be deleted. (str)
    office_id : str
        The ID of the office that the specified level belongs to. (str)

    Returns
    -------
    None
    """

    if specified_level_id is None:
        raise ValueError("Cannot delete a specified level without an id")
    if office_id is None:
        raise ValueError("Cannot delete a specified level without an office id")
    endpoint = f"specified-levels/{specified_level_id}"

    params = {"office": office_id}
    return api.delete(endpoint, params)


def update_specified_level(
    old_specified_level_id: str, new_specified_level_id: str, office_id: str
) -> None:
    """
    Parameters
    ----------
    old_specified_level_id : str
        The old specified level ID that needs to be updated.
    new_specified_level_id : str
        The new specified level ID that will replace the old ID.
    office_id : str
        The ID of the office associated with the specified level.

    Returns
    -------
    None
    """

    if old_specified_level_id is None:
        raise ValueError("Cannot update a specified level without an old id")
    if new_specified_level_id is None:
        raise ValueError("Cannot update a specified level without a new id")
    if office_id is None:
        raise ValueError("Cannot update a specified level without an office id")
    endpoint = f"specified-levels/{old_specified_level_id}"

    params = {
        "office": office_id,
        "specified-level-id": new_specified_level_id,
    }
    return api.patch(endpoint=endpoint, params=params)
