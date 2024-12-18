#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from typing import Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_timeseries_profile_parser(
    office_id: str, location_id: str, parameter_id: str
) -> Data:
    """
    Returns a timeseries profile parser used to interpret timeseries data input.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile parser
        location_id: string
            The location name associated with the timeseries profile parser
        parameter_id: string
            The name of the key parameter associated with the timeseries profile parser

    Returns
    -------
        cwms data type
    """

    endpoint = f"timeseries/profile-parser/{location_id}/{parameter_id}"
    params = {
        "office": office_id,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_timeseries_profile_parsers(
    office_mask: Optional[str],
    location_mask: Optional[str],
    parameter_id_mask: Optional[str],
) -> Data:
    """
    Returns a list of timeseries profile parsers.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_mask: string, optional
            A mask to limit the results based on office. Uses regex to compare with office IDs in the database.
            Default value is '*'
        location_mask: string, optional
            A mask to limit the results based on location. Uses regex to compare with location IDs in the database.
            Default value is '*'
        parameter_id_mask: string, optional
            A mask to limit the results based on the parameter associated with the timeseries profile. Uses regex to
            compare the parameter IDs in the database. Default value is '*'

    Returns
    -------
        cwms data type
    """

    endpoint = "timeseries/profile-parser"
    params = {
        "office-mask": office_mask,
        "location-mask": location_mask,
        "parameter-id-mask": parameter_id_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


def delete_timeseries_profile_parser(
    office_id: str, location_id: str, parameter_id: str
) -> None:
    """
    Deletes a specified timeseries profile parser

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile parser
        location_id: string
            The location associated with the timeseries profile parser
        parameter_id: string
            The name of the key parameter associated with the timeseries profile parser

    Returns
    -------
        None
    """

    endpoint = f"timeseries/profile-parser/{location_id}/{parameter_id}"
    params = {"office": office_id}

    return api.delete(endpoint, params)


def store_timeseries_profile_parser(
    data: str, fail_if_exists: Optional[bool] = True
) -> None:
    """
    Stores a new timeseries profile parser.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        data: string
            JSON for storing a timeseries profile parser

            Indexed:
            {
                "type": "indexed-timeseries-profile-parser",
                "location-id": {
                    "office-id": "string",
                    "name": "string"
                },
                "key-parameter": "string",
                "record-delimiter": "char",
                "time-format": "MM/DD/YYYY,HH24:MI:SS",
                "time-zone": "string",
                "parameter-info-list": [
                    {
                    "type": "indexed-parameter-info",
                    "parameter": "string",
                    "unit": "string",
                    "index": int
                    },
                    {
                    "type": "indexed-parameter-info",
                    "parameter": "string",
                    "unit": "string",
                    "index": int
                    }
                ],
                "time-in-two-fields": bool,
                "field-delimiter": "char",
                "time-field": int
            }

            Columnar:
            {
                "type": "columnar-timeseries-profile-parser",
                "location-id": {
                    "office-id": "string",
                    "name": "string"
                },
                "key-parameter": "string",
                "record-delimiter": "char",
                "time-format": "MM/DD/YYYY,HH24:MI:SS",
                "time-zone": "string",
                "parameter-info-list": [
                    {
                    "type": "columnar-parameter-info",
                    "parameter": "string",
                    "unit": "string",
                    "start-column": int,
                    "end-column": int
                    },
                    {
                    "type": "columnar-parameter-info",
                    "parameter": "string",
                    "unit": "string",
                    "start-column": int,
                    "end-column": int
                    }
                ],
                "time-in-two-fields": bool,
                "time-start-column": int,
                "time-end-column": int
            }

        fail_if_exists: boolean, optional
            Throw a ClientError if the parser already exists
            Default is `True`

    Returns
    -------
        None
    """

    endpoint = "timeseries/profile-parser"
    params = {
        "fail-if-exists": fail_if_exists,
    }

    return api.post(endpoint, data, params)
