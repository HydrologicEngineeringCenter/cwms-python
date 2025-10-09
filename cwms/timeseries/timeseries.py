import concurrent.futures
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.catalog.catalog import get_ts_extents
from cwms.cwms_types import JSON, Data


def get_multi_timeseries_df(
    ts_ids: list[str],
    office_id: str,
    unit: Optional[str] = "EN",
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    melted: Optional[bool] = False,
    max_workers: Optional[int] = 30,
) -> DataFrame:
    """gets multiple timeseries and stores into a single dataframe

    Parameters
    ----------
        ts_ids: list
            a list of timeseries to get.  If the timeseries is a versioned timeseries then separate the ts_id from the
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
        max_workers: Int, Optional, default is None
            It is a number of Threads aka size of pool in concurrent.futures.ThreadPoolExecutor. From 3.8 onwards
            default value is min(32, os.cpu_count() + 4). Out of these 5 threads are preserved for I/O bound task.


        Returns
        -------
            dataframe
    """

    def get_ts_ids(ts_id: str) -> Any:
        try:
            if ":" in ts_id:
                ts_id, version_date = ts_id.split(":", 1)
                version_date_dt = pd.to_datetime(version_date)
            else:
                version_date_dt = None
            data = get_timeseries(
                ts_id=ts_id,
                office_id=office_id,
                unit=unit,
                begin=begin,
                end=end,
                version_date=version_date_dt,
                multithread=False,
            )
            result_dict = {
                "ts_id": ts_id,
                "unit": data.json["units"],
                "version_date": version_date_dt,
                "values": data.df,
            }
            return result_dict
        except Exception as e:
            logging.error(f"Error processing {ts_id}: {e}")
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(get_ts_ids, ts_ids)

    result_dict = list(results)
    data = pd.DataFrame()
    for row in result_dict:
        if row:
            temp_df = row["values"]
            temp_df = temp_df.assign(ts_id=row["ts_id"], units=row["unit"])
            if "version_date" in row.keys():
                temp_df = temp_df.assign(version_date=row["version_date"])
            temp_df.dropna(how="all", axis=1, inplace=True)
            data = pd.concat([data, temp_df], ignore_index=True)

    if not melted and "date-time" in data.columns:
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


def chunk_timeseries_time_range(
    begin: datetime, end: datetime, chunk_size: timedelta
) -> List[Tuple[datetime, datetime]]:
    """
    Splits a time range into smaller chunks.

    Parameters
    ----------
    begin : datetime
        The start of the time range.
    end : datetime
        The end of the time range.
    chunk_size : timedelta
        The size of each chunk.

    Returns
    -------
    List[Tuple[datetime, datetime]]
        A list of tuples, where each tuple represents the start and end of a chunk.
    """
    chunks = []
    current = begin
    while current < end:
        next_chunk = min(current + chunk_size, end)
        chunks.append((current, next_chunk))
        current = next_chunk
    return chunks


def get_timeseries_chunk(
    selector: str, endpoint: str, param: Dict[str, Any], begin: datetime, end: datetime
) -> Data:
    param["begin"] = begin.isoformat() if begin else None
    param["end"] = end.isoformat() if end else None
    response = api.get_with_paging(selector=selector, endpoint=endpoint, params=param)
    return Data(response, selector=selector)


def fetch_timeseries_chunks(
    chunks: List[Tuple[datetime, datetime]],
    params: Dict[str, Any],
    selector: str,
    endpoint: str,
    max_workers: int,
) -> List[Data]:
    # Initialize an empty list to store results
    results = []

    # Create a ThreadPoolExecutor to manage multithreading
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each chunk to the api
        future_to_chunk = {
            executor.submit(
                get_timeseries_chunk,
                selector,
                endpoint,
                params.copy(),
                chunk_start,
                chunk_end,
            ): (chunk_start, chunk_end)
            for chunk_start, chunk_end in chunks
        }

        # Process completed threads as they finish
        for future in concurrent.futures.as_completed(future_to_chunk):
            try:
                # Retrieve the result of the completed future
                result = future.result()
                results.append(result)
            except Exception as e:
                chunk_start, chunk_end = future_to_chunk[future]
                # Log or handle any errors that occur during execution
                logging.error(
                    f"Failed to fetch data from {chunk_start} to {chunk_end}: {e}"
                )
    return results


def combine_timeseries_results(results: List[Data]) -> Data:
    """
    Combines the results from multiple chunks into a single cwms Data object.

    Parameters
    ----------
    results : list
        List of cwms Data objects returned from the executor.

    Returns
    -------
    cwms Data
        Combined cwms Data object with merged DataFrame and updated JSON metadata.
    """
    # Extract DataFrames from each cwms data object
    dataframes = [result.df for result in results]

    # Combine all DataFrames into one
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Sort the combined DataFrame by 'date-time'
    combined_df.sort_values(by="date-time", inplace=True)

    # Drop duplicate rows based on 'date-time' (if necessary)
    combined_df.drop_duplicates(subset="date-time", inplace=True)

    # Extract metadata from the first result (assuming all chunks share the same metadata)
    combined_json = results[0].json

    # Update metadata to reflect the combined time range
    combined_json["begin"] = combined_df["date-time"].min().isoformat()
    combined_json["end"] = combined_df["date-time"].max().isoformat()
    combined_json["total"] = len(combined_df)

    # Update the "values" key in the JSON to include the combined data
    combined_json["values"] = combined_df.to_dict(orient="records")

    # Return a new cwms Data object with the combined DataFrame and updated metadata
    return Data(combined_json, selector="values")


def validate_dates(
    begin: Optional[datetime] = None, end: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    # Ensure `begin` and `end` are valid datetime objects
    begin = begin or datetime.now(tz=timezone.utc) - timedelta(
        days=1
    )  # Default to 24 hours ago
    end = end or datetime.now(tz=timezone.utc)
    # assign UTC tz
    begin = begin.replace(tzinfo=timezone.utc)
    end = end.replace(tzinfo=timezone.utc)
    return begin, end


def get_timeseries(
    ts_id: str,
    office_id: str,
    unit: Optional[str] = "EN",
    datum: Optional[str] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    page_size: Optional[int] = 300000,
    version_date: Optional[datetime] = None,
    trim: Optional[bool] = True,
    multithread: Optional[bool] = True,
    max_workers: int = 20,
    max_days_per_chunk: int = 14,
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
        page_size: int, optional, default is 300000: Specifies the number of records to obtain in
            a single call.
        version_date: datetime, optional, default is None
            Version date of time series values being requested. If this field is not specified and
            the timeseries is versioned, the query will return the max aggregate for the time period.
        trim: boolean, optional, default is True
            Specifies whether to trim missing values from the beginning and end of the retrieved values.
        multithread: boolean, optional, default is True
            Specifies whether to trim missing values from the beginning and end of the retrieved values.
        max_workers: integer, default is 20
            The maximum number of worker threads that will be spawned for multithreading, If calling more than 3 years of 15 minute data, consider using 30 max_workers
        max_days_per_chunk: integer, default is 14
            The maximum number of days that would be included in a thread. If calling more than 1 year of 15 minute data, consider using 30 days
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe. dates are all in UTC
    """

    selector = "values"
    endpoint = "timeseries"
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

    begin, end = validate_dates(begin=begin, end=end)

    # grab extents if begin is before CWMS DB were implemented to prevent empty queries outside of extents
    if begin < datetime(2014, 1, 1, tzinfo=timezone.utc) and multithread:
        try:
            begin_extent, _, _ = get_ts_extents(ts_id=ts_id, office_id=office_id)
            # replace begin with begin extent if outside extents
            if begin < begin_extent:
                begin = begin_extent
                logging.debug(
                    f"Requested begin was before any data in this timeseries. Reseting to {begin}"
                )
        except Exception as e:
            # If getting extents fails, fall back to single-threaded mode
            logging.debug(
                f"Could not retrieve time series extents ({e}). Falling back to single-threaded mode."
            )

            response = api.get_with_paging(
                selector=selector, endpoint=endpoint, params=params
            )
            return Data(response, selector=selector)

    # divide the time range into chunks
    chunks = chunk_timeseries_time_range(begin, end, timedelta(days=max_days_per_chunk))

    # find max worker thread
    max_workers = max(min(len(chunks), max_workers), 1)

    # if not multithread
    if max_workers == 1 or not multithread:
        response = api.get_with_paging(
            selector=selector, endpoint="timeseries", params=params
        )
        return Data(response, selector=selector)
    else:
        logging.debug(
            f"Fetching {len(chunks)} chunks of timeseries data with {max_workers} threads"
        )
        # fetch the data
        result_list = fetch_timeseries_chunks(
            chunks,
            params,
            selector,
            endpoint,
            max_workers,
        )

        # combine the results
        results = combine_timeseries_results(result_list)

        return results


def timeseries_df_to_json(
    data: pd.DataFrame,
    ts_id: str,
    units: str,
    office_id: str,
    version_date: Optional[datetime] = None,
) -> JSON:
    """This function converts a dataframe to a json dictionary in the correct format to be posted using the store_timeseries function.

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
            timeseries id:specified name of the timeseries to be posted to
        office_id: str
            the owning office of the time series
        units: str
            units of values to be stored (ie. ft, in, m, cfs....)
        version_date: datetime, optional, default is None
            Version date of time series values to be posted.

    Returns:
        JSON.  Dates in JSON will be in UTC to be stored in
    """

    # make a copy so original dataframe does not get updated.
    df = data.copy()
    # check dataframe columns
    if "quality-code" not in df:
        df["quality-code"] = 0
    if "date-time" not in df:
        raise TypeError(
            "date-time is a required column in data when posting as a dataframe"
        )
    if "value" not in df:
        raise TypeError(
            "value is a required column when posting data when posting as a dataframe"
        )

    # make sure that dataTime column is in iso8601 formate.
    df["date-time"] = pd.to_datetime(df["date-time"], utc=True).apply(
        pd.Timestamp.isoformat
    )
    df = df.reindex(columns=["date-time", "value", "quality-code"])
    if df.isnull().values.any():
        raise ValueError("Null/NaN data must be removed from the dataframe")
    if version_date:
        version_date_iso = version_date.isoformat()
    else:
        version_date_iso = None
    ts_dict = {
        "name": ts_id,
        "office-id": office_id,
        "units": units,
        "values": df.values.tolist(),
        "version-date": version_date_iso,
    }

    return ts_dict


def store_multi_timeseries_df(
    data: pd.DataFrame,
    office_id: str,
    max_workers: Optional[int] = 30,
) -> None:
    def store_ts_ids(
        data: pd.DataFrame,
        ts_id: str,
        office_id: str,
        version_date: Optional[datetime] = None,
        multithread: bool = False,
    ) -> None:
        try:
            units = data["units"].iloc[0]
            data_json = timeseries_df_to_json(
                data=data,
                ts_id=ts_id,
                units=units,
                office_id=office_id,
                version_date=version_date,
            )
            store_timeseries(data=data_json, multithread=multithread)
        except Exception as e:
            print(f"Error processing {ts_id}: {e}")
        return None

    ts_data_all = data.copy()
    if "version_date" not in ts_data_all.columns:
        ts_data_all = ts_data_all.assign(version_date=pd.to_datetime(pd.Series([])))
    unique_tsids = (
        ts_data_all["ts_id"].astype(str) + ":" + ts_data_all["version_date"].astype(str)
    ).unique()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for unique_tsid in unique_tsids:
            ts_id, version_date = unique_tsid.split(":", 1)
            if version_date != "NaT":
                version_date_dt = pd.to_datetime(version_date)
                ts_data = ts_data_all[
                    (ts_data_all["ts_id"] == ts_id)
                    & (ts_data_all["version_date"] == version_date_dt)
                ]
            else:
                version_date_dt = None
                ts_data = ts_data_all[
                    (ts_data_all["ts_id"] == ts_id) & ts_data_all["version_date"].isna()
                ]
            if not data.empty:
                executor.submit(
                    store_ts_ids, ts_data, ts_id, office_id, version_date_dt
                )


def chunk_timeseries_data(
    data: Dict[str, Any], chunk_size: int
) -> List[Dict[str, Any]]:
    """
    Splits the time series values into smaller chunks.

    Parameters
    ----------
    values : list
        List of time series values to be stored.
    chunk_size : int
        Maximum number of values per chunk.

    Returns
    -------
    list
        List of chunks, where each chunk is a subset of the values.
    """
    values = data["values"]
    chunk_list = []
    for i in range(0, len(values), chunk_size):
        chunk = values[i : i + chunk_size]
        chunked_data = {
            "name": data["name"],
            "office-id": data["office-id"],
            "units": data["units"],
            "values": chunk,
            "version-date": data.get("version-date"),
        }

        chunk_list.append(chunked_data)
    return chunk_list


def store_timeseries(
    data: JSON,
    create_as_ltrs: Optional[bool] = False,
    store_rule: Optional[str] = None,
    override_protection: Optional[bool] = False,
    multithread: Optional[bool] = True,
    max_workers: int = 20,
    chunk_size: int = 2 * 7 * 24 * 4,  # two weeks of 15 min data
) -> None:
    """Will Create new TimeSeries if not already present.  Will store any data provided

    Parameters
    ----------
        data: JSON dictionary
            Time Series data to be stored.
        create_as_ltrs: bool, optional, default is False
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
        multithread: bool, default is true
        max_workers: int, default is 20, maximum numbers of worker threads,
            if saving more than 3 years of 15 minute data, consider using 30
        chunk_size: int, default is 2 * 7 * 24 * 4 (two weeks of 15 minute data),
            maximum values that will be saved by a thread, if saving more than 3 years of 15 minute data,
            consider using 30 days of 15 minute data

    Returns
    -------
    None
    """

    endpoint = "timeseries"
    params = {
        "create-as-lrts": create_as_ltrs,
        "store-rule": store_rule,
        "override-protection": override_protection,
    }

    if not isinstance(data, dict):
        raise ValueError("Cannot store a timeseries without a JSON data dictionary")

    # Chunk the data
    chunks = chunk_timeseries_data(data, chunk_size)

    # if multi-threaded not needed
    if len(chunks) == 1 or not multithread:
        return api.post(endpoint, data, params)

    actual_workers = min(max_workers, len(chunks))
    logging.debug(
        f"Storing {len(chunks)} chunks of timeseries data with {actual_workers} threads"
    )

    # Store chunks concurrently
    responses: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Initialize an empty list to store futures
        futures = []
        # Submit each chunk as a separate task to the executor
        for chunk in chunks:
            future = executor.submit(
                api.post,  # The function to execute
                endpoint,  # The chunk of data to store
                data,
                params,
            )
            futures.append(future)  # Add the future to the list

        for future in concurrent.futures.as_completed(futures):
            try:
                responses.append({"success:": future.result()})
            except Exception as e:
                start_time = chunk["values"][0][0]
                end_time = chunk["values"][-1][0]
                logging.error(
                    f"Error storing chunk from {start_time} to {end_time}: {e}"
                )
                responses.append({"error": str(e)})

    return


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
