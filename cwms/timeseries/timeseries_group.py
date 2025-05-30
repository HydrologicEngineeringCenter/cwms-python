import threading
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_timeseries_group(
    group_id: str,
    category_id: str,
    category_office_id: str,
    office_id: Optional[str] = None,
    group_office_id: Optional[str] = None,
) -> Data:
    """Retreives time series stored in the requested time series group

    Parameters
        ----------
            group_id: string
                Timeseries group whose data is to be included in the response.
            category_id: string
                The category id that contains the timeseries group.
            office_id: string
                The owning office of the timeseries assigned to the group whose data is to be included in the response.
            group_office_id: string
                Specifies the owning office of the timeseries group.
            category_office_id: string
                Specifies the owning office of the timeseries group category.

        Returns
        -------
            cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = f"timeseries/group/{group_id}"
    params = {
        "office": office_id,
        "category-id": category_id,
        "category-office-id": category_office_id,
        "group-office-id": group_office_id,
    }

    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response, selector="assigned-time-series")


def get_timeseries_groups(
    office_id: Optional[str] = None,
    include_assigned: Optional[bool] = True,
    timeseries_category_like: Optional[str] = None,
    timeseries_group_like: Optional[str] = None,
    category_office_id: Optional[str] = None,
) -> Data:
    """
    Retreives a list of time series groups.

    Parameters
    ----------
        category_id: string
            The category id that contains the timeseries group.
        include_assigned: Boolean
            Include the assigned timeseries in the returned timeseries groups. (default: true)
        timeseries_category_like: string
            Posix regular expression matching against the timeseries category id
        timeseries_group_like: String
            Posix regular expression matching against the timeseries group id
        category_office_id: string
            Specifies the owning office of the timeseries group category
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe
    """

    endpoint = "timeseries/group"
    params = {
        "office": office_id,
        "include-assigned": include_assigned,
        "timeseries-category-like": timeseries_category_like,
        "timeseries_group_like": timeseries_group_like,
        "category-office-id": category_office_id,
    }
    response = api.get(endpoint=endpoint, params=params, api_version=1)
    return Data(response)


def timeseries_group_df_to_json(
    data: pd.DataFrame,
    group_id: str,
    group_office_id: str,
    category_office_id: str,
    category_id: str,
) -> JSON:
    """
    Converts a dataframe to a json dictionary in the correct format.

    Parameters
    ----------
        data: pd.DataFrame
            Dataframe containing timeseries information.
        group_id: str
            The group ID for the timeseries.
        office_id: str
            The ID of the office associated with the specified timeseries.
        category_id: str
            The ID of the category associated with the group

    Returns
    -------
    JSON
        JSON dictionary of the timeseries data.
    """
    df = data.copy()
    required_columns = ["office-id", "timeseries-id"]
    optional_columns = ["alias-id", "attribute", "ts-code"]
    for column in required_columns:
        if column not in df.columns:
            raise TypeError(
                f"{column} is a required column in data when posting as a dataframe"
            )

    if df[required_columns].isnull().any().any():
        raise ValueError(
            f"Null/NaN values found in required columns: {required_columns}. "
        )

    # Fill optional columns with default values if missing
    if "alias-id" not in df.columns:
        df["alias-id"] = None
    if "attribute" not in df.columns:
        df["attribute"] = 0

    # Replace NaN with None for optional columns
    for column in optional_columns:
        if column in df.columns:
            data[column] = df[column].where(pd.notnull(df[column]), None)

    # Build the list of time-series entries
    assigned_time_series = df.apply(
        lambda entry: {
            "office-id": entry["office-id"],
            "timeseries-id": entry["timeseries-id"],
            "alias-id": entry["alias-id"],
            "attribute": entry["attribute"],
            **(
                {"ts-code": entry["ts-code"]}
                if "ts-code" in entry and pd.notna(entry["ts-code"])
                else {}
            ),
        },
        axis=1,
    ).tolist()

    # Construct the final JSON dictionary
    json_dict = {
        "office-id": group_office_id,
        "id": group_id,
        "time-series-category": {"office-id": category_office_id, "id": category_id},
        "assigned-time-series": assigned_time_series,
    }

    return json_dict


def store_timeseries_groups(data: JSON, fail_if_exists: Optional[bool] = True) -> None:
    """
    Create new TimeSeriesGroup
    Parameters
    ----------
        data: JSON dictionary
            Time Series data to be stored.
        fail_if_exists: Boolean Defualt = True
            Create will fail if provided ID already exists.

    Returns
    -------
    None
    """

    if data is None:
        raise ValueError("Cannot store a standard text without timeseries group JSON")

    endpoint = "timeseries/group"
    params = {"fail-if-exists": fail_if_exists}

    return api.post(endpoint, data, params, api_version=1)


def update_timeseries_groups(
    data: JSON,
    group_id: str,
    office_id: str,
    replace_assigned_ts: Optional[bool] = False,
) -> None:
    """
    Updates the timeseries groups with the provided group ID and office ID.

    Parameters
    ----------
        group_id : str
            The group id of the timeseries group to be updates
        office_id : str
            The ID of the office associated with the timeseries group.
        replace_assigned_ts : bool, optional
            Specifies whether to unassign all existing timeseries before assigning new timeseries specified in the content body. Default is False.
        data: JSON dictionary
            Timeseries data to be stored.

    Returns
    -------
    None
    """
    if not group_id:
        raise ValueError("Cannot update a timeseries groups without an id")
    if not office_id:
        raise ValueError("Cannot update a timeseries groups without an office id")

    endpoint = f"timeseries/group/{group_id}"
    params = {
        "replace-assigned-ts": replace_assigned_ts,
        "office": office_id,
    }

    api.patch(endpoint=endpoint, data=data, params=params, api_version=1)


def delete_timeseries_group(group_id: str, category_id: str, office_id: str) -> None:
    """Deletes requested time series group

    Parameters
        ----------
            group_id: string
                The time series group to be deleted
            category_id: string
                Specifies the time series category of the time series group to be deleted
            office_id: string
                Specifies the owning office of the time series group to be deleted

        Returns
        -------
            None
    """

    endpoint = f"timeseries/group/{group_id}"
    params = {
        "office": office_id,
        "category-id": category_id,
    }

    return api.delete(endpoint, params=params, api_version=1)
