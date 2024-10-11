import threading
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from pandas import DataFrame

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


def get_multi_timeseries_df(
    ts_ids: list[str],
    office_id: str,
    unit: Optional[str] = "EN",
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    melted: Optional[bool] = False,
) -> DataFrame:
    """gets multiple timeseries and stores into a single dataframe

    Parameters
    ----------
        ts_ids: linst
            a list of timeseries to get.  If the timeseries is a verioned timeseries then serpeate the ts_id from the
            version_date using a :.  Example "OMA.Stage.Inst.6Hours.0.Fcst-MRBWM-GRFT:2024-04-22 07:00:00-05:00".  Make
            sure that the version date include the timezone offset if not in UTC.
        office_id: string
            The owning office of the time series(s).
        unit: string, optional, default is EN
            The unit or unit system of the response. Defaults to EN. Valid values
            for the unit field are:
                1. EN. English unit system.
                2. SI. SI unit system.
                3. Other.
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
        melted: Boolean, optional, default is false
            if set to True a melted dataframe will be provided. By default a multi-index column dataframe will be
            returned.


        Returns
        -------
            dataframe
    """

    def get_ts_ids(
        result_dict: list[Dict[str, Any]],
        ts_id: str,
        office_id: str,
        begin: datetime,
        end: datetime,
        unit: str,
        version_date: datetime,
    ) -> None:
        data = get_timeseries(
            ts_id=ts_id,
            office_id=office_id,
            unit=unit,
            begin=begin,
            end=end,
            version_date=version_date,
        )
        result_dict.append(
            {
                "ts_id": ts_id,
                "unit": data.json["units"],
                "version_date": version_date,
                "values": data.df,
            }
        )

    result_dict = []  # type: list[Dict[str,Any]]
    threads = []
    for ts_id in ts_ids:
        if ":" in ts_id:
            ts_id, version_date = ts_id.split(":", 1)
            version_date_dt = pd.to_datetime(version_date)
        else:
            version_date_dt = None
        t = threading.Thread(
            target=get_ts_ids,
            args=(result_dict, ts_id, office_id, begin, end, unit, version_date_dt),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    data = pd.DataFrame()
    for row in result_dict:
        temp_df = row["values"]
        temp_df = temp_df.assign(ts_id=row["ts_id"], units=row["unit"])
        if "version_date" in row.keys():
            temp_df = temp_df.assign(version_date=row["version_date"])
        temp_df.dropna(how="all", axis=1, inplace=True)
        data = pd.concat([data, temp_df], ignore_index=True)

    if not melted:
        cols = ["ts_id", "units"]
        if "version_date" in data.columns:
            cols.append("version_date")
            data["version_date"] = data["version_date"].dt.strftime(
                "%Y-%m-%d %H:%M:%S%z"
            )
            data["version_date"] = (
                data["version_date"].str[:-2] + ":" + data["version_date"].str[-2:]
            )
            data.fillna({"version_date": ""}, inplace=True)
        data = data.pivot(index="date-time", columns=cols, values="value")

    return data


def get_timeseries(
    ts_id: str,
    office_id: str,
    unit: Optional[str] = "EN",
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page_size: Optional[int] = 500000,
    version_date: Optional[datetime] = None,
    trim: Optional[bool] = True,
) -> Data:
    """Retrieves time series values from a specified time series and time window.  Value date-times
    obtained are always in UTC.

    Parameters
    ----------
        ts_id: string
            Name of the time series whose data is to be included in the response.
        office_id: string
            The owning office of the time series.
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
    if begin and not isinstance(begin, datetime):
        raise ValueError("begin needs to be in datetime")
    if end and not isinstance(end, datetime):
        raise ValueError("end needs to be in datetime")
    if version_date and not isinstance(version_date, datetime):
        raise ValueError("version_date needs to be in datetime")
    params = {
        "office": office_id,
        "name": ts_id,
        "unit": unit,
        "datum": datum,
        "begin": begin.isoformat() if begin else None,
        "end": end.isoformat() if end else None,
        "page-size": page_size,
        "page": None,
        "version-date": version_date.isoformat() if version_date else None,
        "trim": trim,
    }
    selector = "values"

    response = api.get_with_paging(selector=selector, endpoint=endpoint, params=params)
    return Data(response, selector=selector)


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
