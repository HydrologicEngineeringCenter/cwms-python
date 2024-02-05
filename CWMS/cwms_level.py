#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json

from ._constants import *
from .core import CwmsApiSession
from .core import _CwmsBase
from .utils import queryCDA


class CwmsLevel(_CwmsBase):

    _SPECIFIED_LEVELS_ENDPOINT = "specified-levels"

    def __init__(self, cwms_api_session: CwmsApiSession):
        super().__init__(cwms_api_session)

    def retrieve_specified_levels_json(self, specified_level_mask: str = "*", office_id: str = "*"):
        end_point = CwmsLevel._SPECIFIED_LEVELS_ENDPOINT

        params = {
            OFFICE_PARAM: office_id,
            TEMPLATE_ID_MASK_PARAM: specified_level_mask
        }
        headers = {
            "Accept": HEADER_JSON_V2
        }
        response = queryCDA(self, end_point, params, headers, DICT_FORMAT, None)
        return response

    def retrieve_specified_level_json(self, specified_level_id: str, office_id: str):
        if specified_level_id is None:
            raise ValueError("Cannot retrieve a single specified level without an id")
        if office_id is None:
            raise ValueError("Cannot retrieve a single specified level without an office id")
        end_point = f"{CwmsLevel._SPECIFIED_LEVELS_ENDPOINT}/{specified_level_id}"

        params = {
            OFFICE_PARAM: office_id
        }
        headers = {
            "Accept": HEADER_JSON_V2
        }
        response = queryCDA(self, end_point, params, headers, DICT_FORMAT, None)
        return response

    def store_specified_level_json(self, data: dict, fail_if_exists: bool = True):
        if dict is None:
            raise ValueError("Cannot store a specified level without a JSON data dictionary")
        end_point = CwmsLevel._SPECIFIED_LEVELS_ENDPOINT

        params = {
            FAIL_IF_EXISTS: fail_if_exists
        }
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().post(end_point, params = params, headers = headers, data = json.dumps(data))
        response.raise_for_status()

    def delete_specified_level(self, specified_level_id: str, office_id: str):
        if specified_level_id is None:
            raise ValueError("Cannot delete a specified level without an id")
        if office_id is None:
            raise ValueError("Cannot delete a specified level without an office id")
        end_point = f"{CwmsLevel._SPECIFIED_LEVELS_ENDPOINT}/{specified_level_id}"

        params = {
            OFFICE_PARAM: office_id
        }
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().delete(end_point, params=params, headers=headers)
        response.raise_for_status()

    def update_specified_level(self, old_specified_level_id: str, new_specified_level_id: str, office_id: str):
        if old_specified_level_id is None:
            raise ValueError("Cannot update a specified level without an old id")
        if new_specified_level_id is None:
            raise ValueError("Cannot update a specified level without a new id")
        if office_id is None:
            raise ValueError("Cannot update a specified level without an office id")
        end_point = f"{CwmsLevel._SPECIFIED_LEVELS_ENDPOINT}/{old_specified_level_id}"

        params = {
            OFFICE_PARAM: office_id,
            "specified-level-id": new_specified_level_id
        }
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().patch(end_point, params=params, headers=headers)
        response.raise_for_status()

# retrieveLocationLevel
# retrieveLocationLevels
# storeLevel
# deleteLevel
# retrieveLevelAsTimeSeries
