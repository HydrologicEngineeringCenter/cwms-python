from datetime import datetime
from typing import Optional

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_measurements(
    office_id: Optional[str] = None,
    location_id_mask: Optional[str] = None,
    min_number_id: Optional[str] = None,
    max_number_id: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    timezone: Optional[str] = None,
    min_height: Optional[float] = None,
    max_height: Optional[float] = None,
    min_flow: Optional[float] = None,
    max_flow: Optional[float] = None,
    agency: Optional[str] = None,
    quality: Optional[str] = None,
    unit: Optional[str] = "EN",
) -> Data:
    """Returns matching measurement data

    Parameters
    ----------
        office_id: string, optional, default is None
            Office id mask for filtering measurements.
        location_id_mask: string, optional, default is None
            Location id mask for filtering measurements. Use null to retrieve measurements for all locations.
        min_number_id: sting, optional, default is None
            Minimum measurement number-id for filtering measurements.
        max_number_id: string, optional, default is None
            Maximum measurement number-id for filtering measurements.
        begin: datetime, optional, default is None
            Start of the time window for data to be included in the response. If this field is
            not specified, then begin time will be unbounded. Any timezone information should be
            passed within the datetime object. If no timezone information is given, default will be UTC.
        end: datetime, optional, default is None
            End of the time window for data to be included in the response. If this field is
            not specified, then begin time will be unbounded. Any timezone information should
            be passed within the datetime object. If no timezone information is given, default will be UTC.
        timezone: string, optional, default is None
            This field specifies a default timezone to be used if the format of the begin and end
            parameters do not include offset or time zone information. Defaults to UTC
        min_height: float, optional, default is None
            Minimum height for filtering measurements.
        max_height: float, optional, default is None
            Maximum flow for filtering measurements.
        min_flow: float, optional, default is None
            Minimum flow for filtering measurements.
        max_flow: float, optional, default is None
            Maximum flow for filtering measurements.
        agency: string, optional, default is None
            Agency for filtering measurements
        quality: string, optional, default is None
            Quality for filtering measurements
        unit_systems: string, optional, default is EN
            Specifies the unit system of the response. Valid values for the unit field are:
                1. EN. English unit system.
                2. SI. SI unit system.
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe. Dates returned are all in UTC.
    """

    # creates the dataframe from the timeseries data
    endpoint = "measurements"
    if begin and not isinstance(begin, datetime):
        raise ValueError("begin needs to be in datetime")
    if end and not isinstance(end, datetime):
        raise ValueError("end needs to be in datetime")

    params = {
        "office-mask": office_id,
        "id-mask": location_id_mask,
        "min-number": min_number_id,
        "max-number": max_number_id,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "timezone": timezone,
        "min-height": min_height,
        "max-height": max_height,
        "min-flow": min_flow,
        "max-flow": max_flow,
        "agency": agency,
        "quality": quality,
        "unit-system": unit,
    }

    response = api.get(endpoint, params, api_version=1)
    return Data(response)  # , selector=selector)


def store_measurements(
    data: JSON,
    fail_if_exists: Optional[bool] = True,
) -> None:
    """Will Create new measurement(s)

    Parameters
    ----------
        data: JSON dictionary
            measurement data to be stored.
        fail_if_exists: bool, optional, default is True
            Create will fail if provided Measurement(s) already exist.

    Returns
    -------
    response
    """

    endpoint = "measurements"
    params = {
        "fail-if-exists": fail_if_exists,
    }

    if not isinstance(data, list):
        raise ValueError(
            "Cannot store a measurement without a JSON list, object is not a list of dictionaries"
        )
    for item in data:
        if not isinstance(item, dict):
            raise ValueError(
                "Cannot store a measurement without a JSON list: a non-dictionary object was found"
            )

    return api.post(endpoint, data, params, api_version=1)


def delete_measurements(
    location_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    timezone: Optional[str] = None,
    min_number_id: Optional[str] = None,
    max_number_id: Optional[str] = None,
) -> None:
    """Delete an existing measurement

    Parameters
    ----------
        office_id: string
            Specifies the office of the measurements to delete
        location_id: string
            Specifies the location-id of the measurement(s) to be deleted.
        begin: datetime
            Start of the time window to delete. Any timezone information should be
            passed within the datetime object. If no timezone information is given, default will be UTC.
        end: datetime
            End of the time window to delete. Any timezone information should
            be passed within the datetime object. If no timezone information is given, default will be UTC.
        timezone: string, optional, default is None
            This field specifies a default timezone to be used if the format of the begin and end
            parameters do not include offset or time zone information. Defaults to UTC
        min_number_id: sting, optional, default is None
            Minimum measurement number-id of the measurement to be deleted.
        max_number_id: string, optional, default is None
            Maximum measurement number-id of the measurement to be deleted.

    Returns
    -------
        None
    """

    if location_id is None:
        raise ValueError("Deleting measurements requires a location id")
    if office_id is None:
        raise ValueError("Deleting measurements requires an office")

    endpoint = f"measurements/{location_id}"

    params = {
        "office": office_id,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "timezone": timezone,
        "min-number": min_number_id,
        "max-number": max_number_id,
    }

    return api.delete(endpoint, params, api_version=1)


def get_measurements_extents(
    office_mask: Optional[str] = None,
) -> Data:
    """Get time extents of streamflow measurements

    Parameters
    ----------
        office_mask: string
            Office Id used to filter the results.

    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe. Dates returned are all in UTC.

    """
    endpoint = "measurements/time-extents"

    params = {
        "office-mask": office_mask,
    }

    response = api.get(endpoint, params, api_version=1)
    return Data(response)  # , selector=selector)
