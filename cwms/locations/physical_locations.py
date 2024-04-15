from typing import Optional

import pandas as pd
from pandas import DataFrame

import cwms._constants as constants
from cwms.core import CwmsApiSession, _CwmsBase
from cwms.types import JSON
from cwms.utils import queryCDA, return_df


class CwmsLoc(_CwmsBase):
    _LOCATION_ENDPOINT = "location"
    _LOCATION_GROUP_ENDPOINT = "location/group"

    def __init__(self, cwms_api_session: CwmsApiSession):
        super().__init__(cwms_api_session)

    def retreive_loc_group_df(
        self, loc_group_id: str, category_id: str, office_id: str
    ) -> DataFrame:

        responce = CwmsLoc.retreive_loc_group_json(
            self, loc_group_id, category_id, office_id
        )

        df = return_df(responce, dict_key=["assigned-locations"])

        return df

    def retreive_loc_group_json(
        self, loc_group_id: str, category_id: str, office_id: str
    ) -> JSON:

        end_point = f"{CwmsLoc._LOCATION_GROUP_ENDPOINT}/{loc_group_id}"

        params = {constants.OFFICE_PARAM: office_id, "category-id": category_id}

        header = {"Accept": constants.HEADER_JSON_V1}

        responce = queryCDA(self, end_point, params, header)

        # if dataframe:
        #   responce = pd.DataFrame(responce['assigned-locations'])

        return responce

    def retrieve_locs_df(
        self,
        office_id: Optional[str] = None,
        loc_ids: Optional[str] = None,
        units: Optional[str] = None,
        datum: Optional[str] = None,
    ) -> DataFrame:
        responce = CwmsLoc.retreive_locs_json(self, office_id, loc_ids, units, datum)

        df = return_df(responce, dict_key=["locations", "locations"])

        return df

    def retreive_locs_json(
        self,
        office_id: Optional[str] = None,
        loc_ids: Optional[str] = None,
        units: Optional[str] = None,
        datum: Optional[str] = None,
    ) -> JSON:

        end_point = CwmsLoc._LOCATION_GROUP_ENDPOINT

        params = {
            constants.OFFICE_PARAM: office_id,
            "names": loc_ids,
            "units": units,
            "datum": datum,
        }
        header = {"Accept": constants.HEADER_JSON_V2}

        responce = queryCDA(self, end_point, params, header)
        # if output = 'dataframe':
        # responce =
        return responce

    def ExpandLocations(self, df: DataFrame) -> DataFrame:
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
