from datetime import datetime
from typing import Optional

import pandas as pd

import cwms.api as api
from cwms.types import JSON
from cwms.utils import return_df


def get_timeseries_group_df(
    group_id: str, category_id: str, office_id: str
) -> pd.DataFrame:
    """Retreives time series stored in the requested time series groupas a dataframe

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
        df : pandas.dataframe
            The pandas dataframe containing the time series group information.
    """

    response = get_timeseries_group_json(group_id, category_id, office_id)
    return return_df(response, dict_key=["assigned-time-series"])


def get_timeseries_group_json(group_id: str, category_id: str, office_id: str) -> JSON:
    """Retreives time series stored in the requested time series group as a dictionary

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
        response : dict
            The JSON response containing the time series group information.
    """

    endpoint = f"timeseries/group/{group_id}"
    params = {"office": office_id, "category-id": category_id}

    return api.get(endpoint, params)


def get_timeseries_df(
    tsId: str,
    office_id: str,
    unit: str = "EN",
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page_size: int = 500000,
) -> pd.DataFrame:
    """Retrieves time series data from a specified time series and time window.
    Value date-times obtained are always in UTC.

    Parameters
    ----------
        tsId: string
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

    Returns
    -------
    df : pandas.DataFrame
        The pandas dataframe containing the time series dates, values, and quality codes. Values are always in UTC.
    """

    response = get_timeseries_json(tsId, office_id, unit, datum, begin, end, page_size)
    return return_df(response, dict_key=["values"])


def get_timeseries_json(
    tsId: str,
    office_id: str,
    unit: str = "EN",
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page_size: int = 500000,
    version_date: Optional[datetime] = None,
) -> JSON:
    """Retrieves time series data from a specified time series and time window.  Value date-times
    obtained are always in UTC.

    Parameters
    ----------
        tsId: string
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
    Returns
    -------
    response : dict
        The JSON response containing the time series information.  Values are always in UTC.
    """

    # creates the dataframe from the timeseries data
    endpoint = "timeseries"
    params = {
        "office": office_id,
        "name": tsId,
        "unit": unit,
        "datum": datum,
        "begin": begin.isoformat() if begin else "",
        "end": end.isoformat() if end else "",
        "page-size": page_size,
        "version-date": version_date.isoformat() if version_date else "",
    }

    return api.get(endpoint, params)


def create_timeseries(
    data: JSON,
    create_as_ltrs: bool = False,
    store_rule: Optional[str] = None,
    override_protection: bool = False,
) -> JSON:
    """Will Create new TimeSeries if not already present.  Will store any data provided

    Parameters
    ----------
        data: pd.Dataframe, or Dictionary
            Time Series data to be stored.  If dataframe data must be provided in the following format
                df.tsId = timeseried id:specified name of the time series to be posted to
                df.office = the owning office of the time series
                df.units = units of values to be stored (ie. ft, in, m, cfs....)
                dataframe should have three columns date-time, value, quality-code. date-time values
                can be a string in ISO8601 formate or a datetime field. if quality-code column is not
                present is will be set to 0.
                                        date-time value  quality-code
                0   2023-12-20T14:45:00.000-05:00  93.1           0
                1   2023-12-20T15:00:00.000-05:00  99.8           0
                2   2023-12-20T15:15:00.000-05:00  98.5           0
                3   2023-12-20T15:30:00.000-05:00  98.5           0
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

    if isinstance(data, pd.DataFrame):
        # grab time series information
        tsId = data.tsId
        office = data.office
        units = data.units
        version_date = data.versionDate

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
        data["date-time"] = pd.to_datetime(data["date-time"]).apply(
            pd.Timestamp.isoformat
        )
        data = data.reindex(columns=["date-time", "value", "quality-code"])
        if data.isnull().values.any():
            raise ValueError("Null/NaN data must be removed from the dataframe")

        ts_dict = {
            "name": tsId,
            "office-id": office,
            "units": units,
            "values": data.values.tolist(),
            "version-date": version_date,
        }

    elif isinstance(data, dict):
        ts_dict = data

    else:
        raise TypeError("data is not of type dataframe or dictionary")

    return api.post(endpoint, ts_dict, params)
