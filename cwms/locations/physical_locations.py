from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
import cwms.catalog.catalog as ct
from cwms.types import Data


def get_locations_catalog(page: Optional[str] = None,
                          page_size: Optional[int] = 5000,
                          unit_system: Optional[str] = None,
                          office: Optional[str] = None,
                          like: Optional[str] = None,
                          timeseries_category_like: Optional[str] = None,
                          timeseries_group_like: Optional[str] = None,
                          location_category_like: Optional[str] = None,
                          location_group_like: Optional[str] = None,
                          bounding_office_like: Optional[str] = None,
                          ) -> Data:
    params = {
        "page": page,
        "page-size": page_size,
        "unit-system": unit_system,
        "office": office,
        "like": like,
        "timeseries-category-like": timeseries_category_like,
        "timeseries-group-like": timeseries_group_like,
        "location-category-like": location_category_like,
        "location-group-like": location_group_like,
        "bounding-office-like": bounding_office_like
    }
    return ct.get_catalog("LOCATIONS", params)


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
