import concurrent.futures
import math
import time
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
            print(f"Error processing {ts_id}: {e}")
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
    max_threads: int = 20,
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
        max_threads: integer, default is 20
            The maximum number of threads that will be spawned for multithreading
        max_days_per_chunk: integer, default is 30
            The maximum number of days that would be included in a thread
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe. dates are all in UTC
    """

    endpoint = "timeseries"
    selector = "values"

    if begin and not isinstance(begin, datetime):
        raise ValueError("begin needs to be in datetime")
    if end and not isinstance(end, datetime):
        raise ValueError("end needs to be in datetime")
    if version_date and not isinstance(version_date, datetime):
        raise ValueError("version_date needs to be in datetime")

    # default end to now if not provided
    if end is None:
        end = datetime.now(timezone.utc)
    else:
        end = end.replace(tzinfo=timezone.utc)
    if begin is None:
        # keep original behavior: default window begins 24 hours prior to end
        begin = end - timedelta(days=1)
    else:
        begin = begin.replace(tzinfo=timezone.utc)

    def _call_api_for_range(begin: datetime, end: datetime) -> Dict[str, Any]:
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
        response = api.get_with_paging(
            selector=selector, endpoint=endpoint, params=params
        )
        return dict(response)

    # if multithread disabled or short range, do a single call
    if not multithread:
        response = _call_api_for_range(begin, end)
        return Data(response, selector=selector)

    # Try to get extents for multithreading, fall back to single-threaded if it fails
    try:
        begin_extent, end_extent, last_update = get_ts_extents(
            ts_id=ts_id, office_id=office_id
        )
        if begin.replace(tzinfo=timezone.utc) < begin_extent:
            begin = begin_extent
    except Exception as e:
        # If getting extents fails, fall back to single-threaded mode
        print(
            f"WARNING: Could not retrieve time series extents ({e}). Falling back to single-threaded mode."
        )
        response = _call_api_for_range(begin, end)
        return Data(response, selector=selector)

    total_days = (end - begin).total_seconds() / (24 * 3600)

    # split into N chunks where each chunk <= max_days_per_chunk, but cap chunks to max_threads
    required_chunks = math.ceil(total_days / max_days_per_chunk)

    chunks = min(required_chunks, max_threads)
    print(
        f"INFO: Getting data with {max_threads} threads. Downloading {required_chunks} required chunks."
    )

    if total_days <= max_days_per_chunk:
        response = _call_api_for_range(begin, end)
        return Data(response, selector=selector)

    # create roughly equal ranges
    chunk_seconds = (end - begin).total_seconds() / chunks
    ranges: List[Tuple[datetime, datetime]] = []
    for i in range(chunks):
        b_chunk = begin + timedelta(seconds=math.floor(i * chunk_seconds))
        e_chunk = begin + timedelta(seconds=math.floor((i + 1) * chunk_seconds))
        if i == chunks - 1:
            e_chunk = end
        ranges.append((b_chunk, e_chunk))

    # perform parallel requests
    responses: List[Optional[Dict[str, Any]]] = [None] * len(ranges)
    with concurrent.futures.ThreadPoolExecutor(max_workers=chunks) as executor:
        future_to_idx = {
            executor.submit(_call_api_for_range, r[0], r[1]): idx
            for idx, r in enumerate(ranges)
        }
        for fut in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                responses[idx] = fut.result()
            except Exception as exc:
                # fail fast: re-raise so caller sees error (or change to continue/partial)
                raise

    # responses are now in the same order as ranges
    sorted_responses = [resp for resp in responses if resp is not None]

    # Merge JSON "values" lists (assumes top-level "values" list present)
    merged_json: Dict[str, Any] = {}
    # merge metadata from first response (you can adjust which metadata to prefer)
    if sorted_responses:
        merged_json.update(
            {k: v for k, v in sorted_responses[0].items() if k != "values"}
        )
        merged_values: List[Any] = []
        for resp in sorted_responses:
            vals = resp.get("values") or []
            merged_values.extend(vals)
        # optionally deduplicate by date-time
        try:
            # preserve order and dedupe by date-time string
            seen = set()
            deduped = []
            for v in merged_values:
                dt = v.get("date-time")
                if dt not in seen:
                    seen.add(dt)
                    deduped.append(v)
            merged_json["values"] = deduped
        except Exception:
            merged_json["values"] = merged_values
    else:
        merged_json["values"] = []

    final_data = Data(merged_json, selector=selector)

    return final_data


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
    data: pd.DataFrame, office_id: str, max_workers: Optional[int] = 30
) -> None:
    def store_ts_ids(
        data: pd.DataFrame,
        ts_id: str,
        office_id: str,
        version_date: Optional[datetime] = None,
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
            store_timeseries(data=data_json, multithread=False)
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


def _post_chunk_with_retries(
    endpoint: str,
    chunk_json: JSON,
    params: Dict[str, Any],
    max_retries: int = 3,
    backoff: float = 0.5,
) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            api.post(endpoint, chunk_json, params)
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))


def _get_chunk_date_range(chunk_json: JSON) -> Tuple[str, str]:
    """Extract start and end dates from a chunk's values"""
    try:
        values = chunk_json.get("values", [])
        if not values:
            return ("No values", "No values")

        # Get first and last date-time
        start_date = values[0][0]
        end_date = values[-1][0]

        return (start_date, end_date)
    except Exception:
        return ("Error parsing dates", "Error parsing dates")


def store_timeseries(
    data: JSON,
    create_as_ltrs: Optional[bool] = False,
    store_rule: Optional[str] = None,
    override_protection: Optional[bool] = False,
    multithread: Optional[bool] = True,
    max_threads: int = 20,
    max_values_per_chunk: int = 700,
    max_retries: int = 3,
) -> Optional[List[Dict[str, Any] | None]]:
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
        max_threads: int, maximum numbers of threads
        max_values_per_chunk: int, maximum values that will be saved by a thread
        max_retries: int, maximum number of store retries that will be attempted

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

    values = data["values"]
    total = len(values)

    # small payload: single post
    if (not multithread) or total <= max_values_per_chunk:
        api.post(endpoint, data, params)
        return None

    # determine chunking
    required_chunks = math.ceil(total / max_values_per_chunk)

    # process in batches of max_threads
    print(f"INFO: Need {required_chunks} chunks of ~{max_values_per_chunk} values each")
    # print(f"INFO: Will process in batches of {max_threads} threads")

    # preserve metadata keys except "values"
    meta = {k: v for k, v in data.items() if k != "values"}

    # Build chunk payloads first
    all_chunk_payloads = []
    for i in range(required_chunks):
        start = i * max_values_per_chunk
        end = min(start + max_values_per_chunk, total)
        chunk_vals = values[start:end]
        chunk_json = dict(meta)
        chunk_json["values"] = chunk_vals
        all_chunk_payloads.append((i, chunk_json))

    # print(f"INFO: Created {len(all_chunk_payloads)} chunks, sizes: {[len(payload[1]['values']) for payload in all_chunk_payloads[:5]]}...")

    # Process chunks in batches of max_threads
    all_responses: List[Optional[Dict[str, Any]]] = [None] * len(all_chunk_payloads)

    for batch_start in range(0, len(all_chunk_payloads), max_threads):
        batch_end = min(batch_start + max_threads, len(all_chunk_payloads))
        batch_chunks = all_chunk_payloads[batch_start:batch_end]

        # print(f"INFO: Processing batch {batch_start//max_threads + 1}: chunks {batch_start} to {batch_end-1}")

        # Process this batch with ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(batch_chunks)
        ) as executor:
            future_to_idx = {
                executor.submit(
                    _post_chunk_with_retries, endpoint, payload, params, max_retries
                ): batch_start
                + local_idx
                for local_idx, (_, payload) in enumerate(batch_chunks)
            }

            for fut in concurrent.futures.as_completed(future_to_idx):
                global_idx = future_to_idx[fut]
                try:
                    result = fut.result()
                    all_responses[global_idx] = {"ok": True, "status": 200}
                except Exception as e:
                    # Get the chunk that failed and extract date range
                    chunk_json = all_chunk_payloads[global_idx][1]
                    start_date, end_date = _get_chunk_date_range(chunk_json)
                    chunk_size = len(chunk_json["values"])
                    print(
                        f"ERROR: Chunk {global_idx} failed (size: {chunk_size}, dates: {start_date} to {end_date}): {e}"
                    )
                    raise

    # Check for errors
    errors = []
    for idx, result in enumerate(all_responses):
        if not result or not result.get("ok"):
            chunk_json = all_chunk_payloads[idx][1]
            start_date, end_date = _get_chunk_date_range(chunk_json)
            chunk_size = len(chunk_json["values"])
            status = result.get("status") if result else "No response"

            error_info = {
                "chunk_index": idx,
                "chunk_size": chunk_size,
                "start_date": start_date,
                "end_date": end_date,
                "status": status,
                "result": result,
            }
            errors.append(error_info)
            print(
                f"ERROR: Chunk {idx} failed - Size: {chunk_size}, Dates: {start_date} to {end_date}, Status: {status}"
            )

    if errors:
        error_summary = []
        for error in errors:
            error_summary.append(
                f"Chunk {error['chunk_index']} (size: {error['chunk_size']}, {error['start_date']} to {error['end_date']}): Status {error['status']}"
            )

        error_message = f"Failed chunks:\n" + "\n".join(error_summary)
        print(
            f"SUMMARY: {len(errors)} chunks failed out of {len(all_chunk_payloads)} total"
        )
        raise RuntimeError(error_message)

    print(
        f"INFO: Store success - All {len(all_chunk_payloads)} chunks completed successfully"
    )
    return all_responses


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
