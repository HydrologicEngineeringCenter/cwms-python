from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.cwms_types import JSON, Data


def get_location_group(loc_group_id: str, category_id: str, office_id: str) -> Data:
    endpoint = f"location/group/{loc_group_id}"
    params = {"office": office_id, "category-id": category_id}

    response = api.get(endpoint, params, api_version=1)
    return Data(response, selector="assigned-locations")


def get_location(location_id: str, office_id: str, unit: str = "EN") -> Data:
    """
    Get location data for a single location

    Parameters
    ----------
        location_id: str
            The ID of the location that the data belongs to.
        office_id : str
            The ID of the office that the data belongs to.
        unit: string, optional, default is EN
            The unit or unit system of the response. Defaults to EN. Valid values
            for the unit field are:
                1. EN. English unit system.
                2. SI. SI unit system.
                3. Other.

    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe

    """

    endpoint = f"locations/{location_id}"
    params = {"office": office_id, "unit": unit}
    response = api.get(endpoint, params=params)
    return Data(response)


def get_locations(
    office_id: Optional[str] = None,
    location_ids: Optional[str] = None,
    units: Optional[str] = "EN",
    datum: Optional[str] = None,
) -> Data:
    """
    Get location data for a single location

    Parameters
    ----------
        location_id: str
            Specifies the name(s) of the location(s) whose data is to be included in the response. This parameter is a Posix regular expression matching against the id
        office_id : str
            The ID of the office that the locations belongs to.
        unit: string, optional, default is EN
            The unit or unit system of the response. Defaults to EN. Valid values
            for the unit field are:
                1. EN. English unit system.
                2. SI. SI unit system.
                3. Other.
        Datum: string, optional, default is None
            Specifies the elevation datum of the response. This field affects only vertical datum. Valid values for this field are:
                1.) NAVD88 The elevation values will in the specified or default units above the NAVD-88 datum.
                2.) NGVD29 The elevation values will be in the specified or default units above the NGVD-29 datum.
    Returns
    -------
        cwms data type.  data.json will return the JSON output and data.df will return a dataframe

    """
    endpoint = "locations"
    params = {
        "office": office_id,
        "names": location_ids,
        "units": units,
        "datum": datum,
    }

    response = api.get(endpoint, params)
    return Data(response)


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
    location_id: str,
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
    """

    if location_id is None:
        raise ValueError("Deleting location requires an id")
    if office_id is None:
        raise ValueError("Deleting location requires an office")

    endpoint = f"locations/{location_id}"
    params = {
        "office": office_id,
    }

    return api.delete(endpoint, params=params)


def store_location(data: JSON) -> None:
    """
    This method is used to store and update location's data through CWMS Data API.

    Parameters
    ----------
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.

    Returns
    -------
        None

    """

    if data is None:
        raise ValueError("Storing location requires a JSON data dictionary")

    endpoint = "locations"

    return api.post(endpoint, data)


def update_location(location_id: str, data: JSON) -> None:
    """
    Parameters
    ----------
        location_id : str
            The location ID that needs to be updated.
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.

    Returns
    -------
        None
    """

    if location_id is None:
        raise ValueError("Cannot update a specified level without an old id")
    if data is None:
        raise ValueError("Storing location requires a JSON data dictionary")

    endpoint = f"locations/{location_id}"

    return api.patch(endpoint=endpoint, data=data)
