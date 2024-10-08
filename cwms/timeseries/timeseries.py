from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_timeseries_group(group_id: str, category_id: str, office_id: str) -> Data:
    """Retreives time series stored in the requested time series group

    Parameters
        ----------
            group_id: string
                Timeseries group whose data is to be included in the response.
            category_id: string
                The category id that contains the timeseries group.
            office_id: string
                The owning office of the timeseries group.

        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = f"timeseries/group/{group_id}"
    params = {"office": office_id, "category-id": category_id}

    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response, selector="assigned-time-series")


def get_timeseries(
    ts_id: str,
    office_id: str,
    unit: str = "EN",
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page_size: int = 500000,
    version_date: Optional[datetime] = None,
    trim: Optional[bool] = True,
) -> Data:
    """Retrieves time series values from a specified time series and time window.  Value date-times
    obtained are always in UTC.

    Parameters
    ----------
        ts_id: string
            Name(s) of the time series whose data is to be included in the response.
        office_id: string
            The owning office of the time series(s).
        unit: string, optional, default is EN
            The unit or unit system of the response. Defaults to EN. Valid values
            for the unit field are:
                1. EN. English unit system.
                2. SI. SI unit system.
                3. Other.
        datum: string, optional, default is None
            The elevation datum of the response. This field affects only elevation location
            levels. Valid values for this field are:
                1. NAVD88.
                2. NGVD29.
        begin: datetime, optional, default is None
            Start of the time window for data to be included in the response. If this field is
            not specified, any required time window begins 24 hours prior to the specified
            or default end time. Any timezone information should be passed within the datetime
            object. If no timezone information is given, default will be UTC.
        end: datetime, optional, default is None
            End of the time window for data to be included in the response. If this field is
            not specified, any required time window ends at the current time. Any timezone
            information should be passed within the datetime object. If no timezone information
            is given, default will be UTC.
        page_size: int, optional, default is 5000000: Sepcifies the number of records to obtain in
            a single call.
        version_date: datetime, optional, default is None
            Version date of time series values being requested. If this field is not specified and
            the timeseries is versioned, the query will return the max aggregate for the time period.
        trim: boolean, optional, default is True
            Specifies whether to trim missing values from the beginning and end of the retrieved values.
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe. dates are all in UTC
    """

    # creates the dataframe from the timeseries data
    endpoint = "timeseries"
    params = {
        "office": office_id,
        "name": ts_id,
        "unit": unit,
        "datum": datum,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "page-size": page_size,
        "version-date": version_date.isoformat() if version_date else None,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="values")


def timeseries_df_to_json(
    data: pd.DataFrame,
    ts_id: str,
    units: str,
    office_id: str,
    version_date: Optional[datetime] = None,
) -> JSON:
    """This function converts a dataframe to a json dictionary in the correct format to be posted using the store_timeseries fucntion.

    Parameters
    ----------
        data: pd.Dataframe
            Time Series data to be stored.  Data must be provided in the following format
                dataframe should have three columns date-time, value, quality-code. date-time values
                can be a string in ISO8601 format or a datetime field. if quality-code column is not
                present is will be set to 0.
                                        date-time value  quality-code
                0   2023-12-20T14:45:00.000-05:00  93.1           0
                1   2023-12-20T15:00:00.000-05:00  99.8           0
                2   2023-12-20T15:15:00.000-05:00  98.5           0
                3   2023-12-20T15:30:00.000-05:00  98.5           0
        ts_id: str
            timeseried id:specified name of the timeseries to be posted to
        office_id: str
            the owning office of the time series
        units: str
            units of values to be stored (ie. ft, in, m, cfs....)
        version_date: datetime, optional, default is None
            Version date of time series values to be posted.

    Returns:
        JSON
    """
    # check dataframe columns
    if "quality-code" not in data:
        data["quality-code"] = 0
    if "date-time" not in data:
        raise TypeError(
            "date-time is a required column in data when posting as a dateframe"
        )
    if "value" not in data:
        raise TypeError(
            "value is a required column when posting data when posting as a dataframe"
        )

    # make sure that dataTime column is in iso8601 formate.
    data["date-time"] = pd.to_datetime(data["date-time"]).apply(pd.Timestamp.isoformat)
    data = data.reindex(columns=["date-time", "value", "quality-code"])
    if data.isnull().values.any():
        raise ValueError("Null/NaN data must be removed from the dataframe")

    ts_dict = {
        "name": ts_id,
        "office-id": office_id,
        "units": units,
        "values": data.values.tolist(),
        "version-date": version_date,
    }

    return ts_dict


def store_timeseries(
    data: JSON,
    create_as_ltrs: Optional[bool] = False,
    store_rule: Optional[str] = None,
    override_protection: Optional[bool] = False,
) -> None:
    """Will Create new TimeSeries if not already present.  Will store any data provided

    Parameters
    ----------
        data: JSON dictionary
            Time Series data to be stored.
        create_as_ltrs: bool, optional, defualt is False
            Flag indicating if timeseries should be created as Local Regular Time Series.
        store_rule: str, optional, default is None:
            The business rule to use when merging the incoming with existing data. Available values :
                REPLACE_ALL,
                DO_NOT_REPLACE,
                REPLACE_MISSING_VALUES_ONLY,
                REPLACE_WITH_NON_MISSING,
                DELETE_INSERT.
        override_protection: str, optional, default is False
            A flag to ignore the protected data quality when storing data.

    Returns
    -------
    response
    """

    endpoint = "timeseries"
    params = {
        "create-as-lrts": create_as_ltrs,
        "store-rule": store_rule,
        "override-protection": override_protection,
    }

    if not isinstance(data, dict):
        raise ValueError("Cannot store a timeseries without a JSON data dictionary")

    return api.post(endpoint, data, params)


def delete_timeseries(
    ts_id: str,
    office_id: str,
    begin: datetime,
    end: datetime,
    version_date: Optional[datetime] = None,
) -> None:
    """
    Deletes binary timeseries data with the given ID,
    office ID and time range.

    Parameters
    ----------
    timeseries_id : str
        The ID of the binary time series data to be deleted.
    office_id : str
        The ID of the office that the binary time series belongs to.
    begin : datetime
        The start date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    end : datetime
        The end date and time of the time range.
        If the datetime has a timezone it will be used,
        otherwise it is assumed to be in UTC.
    version_date : Optional[datetime]
        The time series date version to retrieve. If not supplied,
        the maximum date version for each time step in the retrieval
        window will be deleted.


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

    if ts_id is None:
        raise ValueError("Deleting binary timeseries requires an id")
    if office_id is None:
        raise ValueError("Deleting binary timeseries requires an office")
    if begin is None:
        raise ValueError("Deleting binary timeseries requires a time window")
    if end is None:
        raise ValueError("Deleting binary timeseries requires a time window")

    endpoint = f"timeseries/{ts_id}"
    version_date_str = version_date.isoformat() if version_date else None
    params = {
        "office": office_id,
        "begin": begin.isoformat(),
        "end": end.isoformat(),
        "version-date": version_date_str,
    }

    return api.delete(endpoint, params=params)
