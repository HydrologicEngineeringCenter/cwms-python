from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.types import JSON, Data


def get_location_group(loc_group_id: str, category_id: str, office_id: str) -> Data:
    endpoint = f"location/group/{loc_group_id}"
    params = {"office": office_id, "category-id": category_id}

    response = api.get(endpoint, params, api_version=1)
    return Data(response, selector="assigned-locations")


def get_locations(
    office_id: Optional[str] = None,
    loc_ids: Optional[str] = None,
    units: Optional[str] = None,
    datum: Optional[str] = None,
) -> Data:
    endpoint = "locations"
    params = {
        "office": office_id,
        "names": loc_ids,
        "units": units,
        "datum": datum,
    }

    response = api.get(endpoint, params)
    return Data(response, selector="locations.locations")


def ExpandLocations(df: DataFrame) -> DataFrame:
    df_alias = pd.DataFrame()
    temp = df.aliases.apply(pd.Series)
    for i in temp.columns:
        temp2 = temp[i].apply(pd.Series).dropna(how="all")
        temp2 = temp2.dropna(how="all", axis="columns")
        temp2 = temp2.reset_index()
        df_alias = pd.concat([df_alias, temp2], ignore_index=True)
    df_alias = df_alias.drop_duplicates(subset=["locID", "name"], keep="last")
    df_alias = df_alias.pivot(index="locID", columns="name", values="value")
    df_alias = pd.concat([df, df_alias], axis=1)
    return df_alias


def delete_location(
    loc_ids: str,
    office_id: Optional[str] = None,
) -> None:
    """
    Deletes location data with the given ID and office ID.

    Parameters
    ----------
        office_id : str
            The ID of the office that the data belongs to.
        loc_ids : str
            The ID of the location that the data belongs to.

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

    if loc_ids is None:
        raise ValueError("Deleting location requires an id")
    if office_id is None:
        raise ValueError("Deleting location requires an office")

    endpoint = f"locations/{loc_ids}"
    params = {
        "office": office_id,
    }

    return api.delete(endpoint, params=params)


def store_location(data: JSON, replace_all: bool = False) -> None:
    """
    This method is used to store and update location's data through CWMS Data API.

    Parameters
    ----------
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.
        replace_all : str, optional
            Default is `False`.

    Returns
    -------
        None

    Raises
    ------
        ValueError
            If either dict is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
    """

    if data is None:
        raise ValueError("Storing location requires a JSON data dictionary")

    endpoint = "locations"
    params = {"replace-all": replace_all}

    return api.post(endpoint, data, params)


def update_locations(old_loc_id: str, new_loc_id: str, office_id: str) -> None:
    """
    Parameters
    ----------
        old_loc_id : str
            The old specified location ID that needs to be updated.
        new_loc_id : str
            The new specified location ID that will replace the old ID.
        office_id : str
            The ID of the office associated with the specified level.

    Returns
    -------
        None
    """

    if old_loc_id is None:
        raise ValueError("Cannot update a specified level without an old id")
    if new_loc_id is None:
        raise ValueError("Cannot update a specified level without a new id")
    if office_id is None:
        raise ValueError("Cannot update a specified level without an office id")

    endpoint = f"locations/{old_loc_id}"
    params = {
        "office": office_id,
        "location-id": new_loc_id,
    }

    return api.patch(endpoint=endpoint, params=params)
