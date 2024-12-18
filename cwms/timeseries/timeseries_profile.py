#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from typing import Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_timeseries_profile(office_id: str, location_id: str, parameter_id: str) -> Data:
    """
    Retrieves a timeseries profile.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile
        location_id: string
            The location associated with the timeseries profile parser
        parameter_id: string
            Name of the key parameter associated with the timeseries profile

    Returns
    -------
        cwms data type
    """

    endpoint = f"timeseries/profile/{location_id}/{parameter_id}"
    params = {
        "office": office_id,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_timeseries_profiles(
    office_mask: Optional[str],
    location_mask: Optional[str],
    parameter_id_mask: Optional[str],
    page: Optional[str] = None,
    page_size: Optional[int] = 1000,
) -> Data:
    """
    Retrieves all timeseries profiles that fit the provided masks. Does not include time series values.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_mask: string
            A mask to limit the results based on office. Uses regex to compare with office IDs in the database.
            Default value is '*'
        location_mask: string
            A mask to limit the results based on location. Uses regex to compare with location IDs in the database.
            Default value is '*'
        parameter_id_mask: string
            A mask to limit the results based on the parameter associated with the timeseries profile. Uses regex to
            compare the parameter IDs in the database. Default value is '*'

    Returns
    -------
        cwms data type
    """

    endpoint = "timeseries/profile"
    params = {
        "office-mask": office_mask,
        "location-mask": location_mask,
        "parameter-id-mask": parameter_id_mask,
        "page": page,
        "page-size": page_size,
    }

    response = api.get(endpoint, params)
    return Data(response)


def delete_timeseries_profile(
    office_id: str, parameter_id: str, location_id: str
) -> None:
    """
    Deletes a specified timeseries profile

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile
        parameter_id: string
            Name of the key parameter associated with the timeseries profile
        location_id: string
            The location associated with the timeseries profile

    Returns
    -------
        None
    """

    endpoint = f"timeseries/profile/{location_id}/{parameter_id}"
    params = {
        "office": office_id,
    }

    return api.delete(endpoint, params)


def store_timeseries_profile(data: str, fail_if_exists: Optional[bool] = True) -> None:
    """
    Stores a new timeseries profile

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
    data: string
        json for storing a timeseries profile
        {
            "description": "string",
            "parameter-list": [
                "string",
                ...
            ],
            "location-id": {
                "office-id": "string",
                "name": "string"
            },
            "reference-ts-id": {
                "office-id": "string",
                "name": "string"
            },
            "key-parameter": "string"
        }

    fail_if_exists: boolean, optional
        Throw a ClientError if the profile already exists
        Default is `True`

    Returns
    -------
        None
    """

    endpoint = "timeseries/profile"
    params = {
        "fail-if-exists": fail_if_exists,
    }

    return api.post(endpoint, data, params)
