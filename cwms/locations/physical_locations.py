from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms.api as api
from cwms.types import JSON
from cwms.utils import return_df


def retreive_loc_group_df(
    loc_group_id: str, category_id: str, office_id: str
) -> DataFrame:

    response = retreive_loc_group_json(loc_group_id, category_id, office_id)
    return return_df(response, dict_key=["assigned-locations"])


def retreive_loc_group_json(
    loc_group_id: str, category_id: str, office_id: str
) -> JSON:

    endpoint = f"location/group/{loc_group_id}"
    params = {"office": office_id, "category-id": category_id}

    return api.get(endpoint, params, api_version=1)


def retrieve_locs_df(
    office_id: Optional[str] = None,
    loc_ids: Optional[str] = None,
    units: Optional[str] = None,
    datum: Optional[str] = None,
) -> DataFrame:

    response = retreive_locs_json(office_id, loc_ids, units, datum)
    return return_df(response, dict_key=["locations", "locations"])


def retreive_locs_json(
    office_id: Optional[str] = None,
    loc_ids: Optional[str] = None,
    units: Optional[str] = None,
    datum: Optional[str] = None,
) -> JSON:

    endpoint = "location/group"
    params = {
        "office": office_id,
        "names": loc_ids,
        "units": units,
        "datum": datum,
    }

    return api.get(endpoint, params)


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
