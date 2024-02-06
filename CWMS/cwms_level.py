#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import datetime
import json

from ._constants import *
from .core import CwmsApiSession
from .core import _CwmsBase
from .utils import queryCDA


class CwmsLevel(_CwmsBase):
    _SPECIFIED_LEVELS_ENDPOINT = "specified-levels"
    _LEVELS_ENDPOINT = "levels"

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
        response = self.get_session().post(end_point, params=params, headers=headers, data=json.dumps(data))
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

    def retrieve_location_levels_json(self, level_id_mask: str = "*", office_id: str = None,
                                      unit: str = None, datum: str = None, begin: datetime = None,
                                      end: datetime = None, page: str = None, page_size: int = None):
        end_point = CwmsLevel._LEVELS_ENDPOINT

        params = {
            OFFICE_PARAM: office_id,
            "level-id-mask": level_id_mask,
            UNIT: unit,
            DATUM: datum,
            BEGIN: begin.isoformat() if begin else None,
            END: end.isoformat() if begin else None,
            PAGE: page,
            PAGE_SIZE: page_size
        }
        headers = {
            "Accept": HEADER_JSON_V2
        }
        response = queryCDA(self, end_point, params, headers, DICT_FORMAT, None)
        return response

    def retrieve_location_level_json(self, level_id: str, office_id: str,
                                     effective_date: datetime, unit: str = None):
        if level_id is None:
            raise ValueError("Cannot retrieve a single location level without an id")
        if office_id is None:
            raise ValueError("Cannot retrieve a single location level without an office id")
        if effective_date is None:
            raise ValueError("Cannot retrieve a single location level without an effective date")
        end_point = f"{CwmsLevel._LEVELS_ENDPOINT}/{level_id}"

        params = {
            OFFICE_PARAM: office_id,
            UNIT: unit,
            EFFECTIVE_DATE: effective_date.isoformat()
        }
        headers = {
            "Accept": HEADER_JSON_V2
        }
        response = queryCDA(self, end_point, params, headers, DICT_FORMAT, None)
        return response

    def store_location_level_json(self, data: dict):
        if dict is None:
            raise ValueError("Cannot store a location level without a JSON data dictionary")
        end_point = CwmsLevel._LEVELS_ENDPOINT
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().post(end_point, params=None, headers=headers, data=json.dumps(data))
        response.raise_for_status()

    def delete_location_level(self, location_level_id: str, office_id: str, effective_date: datetime = None,
                              cascade_delete: bool = None):
        if location_level_id is None:
            raise ValueError("Cannot delete a location level without an id")
        if office_id is None:
            raise ValueError("Cannot delete a specified level without an office id")
        end_point = f"{CwmsLevel._LEVELS_ENDPOINT}/{location_level_id}"

        params = {
            OFFICE_PARAM: office_id,
            EFFECTIVE_DATE: effective_date.isoformat() if effective_date else None,
            CASCADE_DELETE: cascade_delete,
        }
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().delete(end_point, params=params, headers=headers)
        response.raise_for_status()

    def retrieve_level_as_timeseries_json(self, location_level_id: str, office_id: str, begin: datetime = None,
                                          end: datetime = None, interval: str = None):
        if location_level_id is None:
            raise ValueError("Cannot retrieve a time series for a location level without an id")
        if office_id is None:
            raise ValueError("Cannot retrieve a time series for a location level without an office id")
        end_point = f"{CwmsLevel._LEVELS_ENDPOINT}/{location_level_id}/timeseries"

        params = {
            OFFICE_PARAM: office_id,
            BEGIN: begin.isoformat() if begin else None,
            END: end.isoformat() if end else None,
            "interval": interval
        }
        headers = {
            "Accept": HEADER_JSON_V2
        }
        return queryCDA(self, end_point, params, headers, DICT_FORMAT, None)
