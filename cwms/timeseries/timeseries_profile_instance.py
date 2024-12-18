#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import Data


def get_timeseries_profile_instance(
    office_id: str,
    location_id: str,
    parameter_id: str,
    version: str,
    unit: str,
    version_date: Optional[datetime],
    start: Optional[datetime],
    end: Optional[datetime],
    page: Optional[str] = None,
    page_size: Optional[int] = 500,
    start_inclusive: Optional[bool] = True,
    end_inclusive: Optional[bool] = True,
    previous: Optional[bool] = False,
    next: Optional[bool] = False,
    max_version: Optional[bool] = False,
) -> Data:
    """
    Returns a timeseries profile instance with associated timeseries values.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile instance
        location_id: string
            The location associated with the timeseries profile instance
        parameter_id: string
            The name of the key parameter associated with the timeseries profile instance
        version: str
            The version of the timeseries profile instance
        unit: str
            The requested units to use for the key parameter values of the timeseries profile instance
        version_date: datetime, optional
            The version date associated with the timeseries profile instance
        start_inclusive: boolean, optional
            Whether the returned timeseries profile instance should include data from the specified
            start timestamp. Default is `True`.
        end_inclusive: boolean, optional
            Whether the returned timeseries profile instance should include data from the specified
            end timestamp. Default is `True`.
        previous: boolean, optional
            The previous timeseries profile instance. Default is `False`.
        next: boolean, optional
            The next timeseries profile instance. Default is `False`.
        max_version: boolean, optional
            Whether the provided version is the maximum version of the timeseries profile instance.
            Default is `False`.
        start: datetime, optional
            The start timestamp of the timeseries profile instance. Default is the year 1800.
        end: datetime, optional
            The end timestamp of the timeseries profile instance. Default is the year 3000.
        page: string, optional
            The page cursor of the timeseries profile instance.
        page_size: string, optional
            The number of timeseries profile instance data records retrieved as part of the instance. Default is `1000`.

    Returns
    -------
        cwms data type
    """

    endpoint = f"timeseries/profile-instance/{location_id}/{parameter_id}/{version}"
    params = {
        "office": office_id,
        "version-date": version_date.isoformat() if version_date else None,
        "unit": unit,
        "start-time-inclusive": start_inclusive,
        "end-time-inclusive": end_inclusive,
        "previous": previous,
        "next": next,
        "max-version": max_version,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "page": page,
        "page-size": page_size,
    }

    response = api.get(endpoint, params)
    return Data(response)


def get_timeseries_profile_instances(
    office_mask: Optional[str],
    location_mask: Optional[str],
    parameter_id_mask: Optional[str],
    version_mask: Optional[str],
) -> Data:
    """
    Retrieves a list of timeseries profile instances that match the specified masks. Does not return timeseries values.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_mask: string
            A mask to limit the results based on office ID. Uses regex to compare with office IDs in the database.
            Default value is `*`
        location_mask: string
            A mask to limit the results based on location ID. Uses regex to compare with location IDs in the database.
            Default value is `*`
        parameter_id_mask: string
            A mask to limit the results based on the parameter associated with the timeseries profile instance.
             Uses regex to compare the parameter IDs in the database. Default value is `*`
        version_mask: string
            A mask to limit the results based on the version associated with the timeseries profile instance.
            Default value is `*`

    Returns
    -------
        cwms data type
    """

    endpoint = "timeseries/profile-instance"
    params = {
        "office-mask": office_mask,
        "location-mask": location_mask,
        "parameter-id-mask": parameter_id_mask,
        "version-mask": version_mask,
    }

    response = api.get(endpoint, params)
    return Data(response)


def delete_timeseries_profile_instance(
    office_id: str,
    location_id: str,
    parameter_id: str,
    version: str,
    version_date: datetime,
    first_date: datetime,
    override_protection: Optional[bool] = True,
) -> None:
    """
    Deletes a timeseries profile instance.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        office_id: string
            The owning office of the timeseries profile instance
        location_id: string
            The name identifier for the timeseries profile instance to delete
        parameter_id: string
            The name of the key parameter associated with the timeseries profile instance
        version: string
            The version of the timeseries profile instance
        version_date: datetime
            The timestamp of the timeseries profile instance version
        first_date: datetime
            The first date of the timeseries profile instance
        override_protection: boolean, optional
            Whether to enable override protection for the timeseries profile instance. Default is `True`.

    Returns
    -------
        None
    """

    endpoint = f"timeseries/profile-instance/{location_id}/{parameter_id}/{version}"
    params = {
        "office": office_id,
        "version-date": version_date.isoformat() if version_date else None,
        "date": first_date.isoformat() if first_date else None,
        "override-protection": override_protection,
    }

    return api.delete(endpoint, params)


def store_timeseries_profile_instance(
    profile_data: str,
    version: str,
    version_date: datetime,
    store_rule: Optional[str] = None,
    override_protection: Optional[bool] = False,
) -> None:
    """
    Stores a new timeseries profile instance. Requires timeseries profile and parser to already be stored.

    Compatibility Warning:
    Currently, the TimeSeries Profile API may not be fully supported
    until a new version of cwms-data-access is released with the updated
    endpoint implementation.

    Parameters
    ----------
        profile_data: string
            The profile data of the timeseries profile instance
        store_rule: boolean, optional
            The method of storing the timeseries profile instance. Default is `REPLACE_ALL`.
        version: string
            The version of the timeseries profile instance.
        version_date: datetime
            The version date of the timeseries profile instance.
        override_protection: boolean, optional
            Whether to enable override protection for the timeseries profile instance. Default is `False`.

    Returns
    -------
        None
    """

    endpoint = "timeseries/profile-instance"
    params = {
        "profile-data": profile_data,
        "method": store_rule,
        "version": version,
        "version-date": version_date.isoformat() if version_date else None,
        "override-protection": override_protection,
    }

    return api.post(endpoint, None, params)
