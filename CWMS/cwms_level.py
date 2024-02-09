#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import datetime
import json

from ._constants import BEGIN
from ._constants import CASCADE_DELETE
from ._constants import DATUM
from ._constants import DICT_FORMAT
from ._constants import EFFECTIVE_DATE
from ._constants import END
from ._constants import FAIL_IF_EXISTS
from ._constants import HEADER_JSON_V2
from ._constants import OFFICE_PARAM
from ._constants import PAGE
from ._constants import PAGE_SIZE
from ._constants import TEMPLATE_ID_MASK_PARAM
from ._constants import UNIT
from .core import CwmsApiSession
from .core import _CwmsBase
from .utils import queryCDA
from .utils import raise_for_status


class CwmsLevel(_CwmsBase):
    """
    `CwmsLevel` class

    This class provides methods to interact with specified levels and location levels in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """
    _SPECIFIED_LEVELS_ENDPOINT = "specified-levels"
    _LEVELS_ENDPOINT = "levels"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_specified_levels_json(self, specified_level_mask: str = "*", office_id: str = "*") -> dict:
        """
        Retrieve JSON data for multiple specified levels from CWMS Data API.

        Parameters
        ----------
        specified_level_mask : str
            The mask to filter the specified levels. Default value is "*" to include all offices.
        office_id : str
            The office for the specified levels. Default value is "*" in order to include all offices.

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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

    def retrieve_specified_level_json(self, specified_level_id: str, office_id: str) -> dict:
        """
        Retrieve JSON Data for a single specified level from CWMS Data API.

        Parameters
        ----------
        specified_level_id : str
            The ID of the specified level to retrieve.
        office_id : str
            The office for the specified level.

        Returns
        -------
        response : dict
            The response JSON text in a dict format of the specified level.

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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

    def store_specified_level_json(self, data: dict, fail_if_exists: bool = True) -> None:
        """
        This method is used to store a specified level through CWMS Data API.

        Parameters
        ----------
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.
        fail_if_exists : str, optional
            A boolean value indicating whether to fail if the specified level entry already exists.
            Default is True.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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
        raise_for_status(response)

    def delete_specified_level(self, specified_level_id: str, office_id: str) -> None:
        """
        Deletes a specified level with the given ID and office ID.

        Parameters
        ----------
        specified_level_id : str
            The ID of the specified level to be deleted. (str)
        office_id : str
            The ID of the office that the specified level belongs to. (str)

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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
        raise_for_status(response)

    def update_specified_level(self, old_specified_level_id: str, new_specified_level_id: str, office_id: str) -> None:
        """
        Parameters
        ----------
        old_specified_level_id : str
            The old specified level ID that needs to be updated.
        new_specified_level_id : str
            The new specified level ID that will replace the old ID.
        office_id : str
            The ID of the office associated with the specified level.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If either old_specified_level_id, new_specified_level_id, or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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
        raise_for_status(response)

    def retrieve_location_levels_json(self, level_id_mask: str = "*", office_id: str = None,
                                      unit: str = None, datum: str = None, begin: datetime = None,
                                      end: datetime = None, page: str = None, page_size: int = None) -> dict:
        """
        Parameters
        ----------
        level_id_mask : str, optional
            A string representing the mask for level IDs. Default is "*".

        office_id : str, optional
            A string representing the office ID.

        unit : str, optional
            A string representing the unit to retrieve values in.

        datum : str, optional
            A string representing the vertical datum.

        begin : datetime, optional
            A datetime object representing the beginning date.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

        end : datetime, optional
            A datetime object representing the end date.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

        page : str, optional
            A string representing the page to retrieve. If None then the first page will be retrieved.

        page_size : int, optional
            An integer representing the number of items per page.

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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
                                     effective_date: datetime, unit: str = None) -> dict:
        """
        Parameters
        ----------
        level_id : str
            The ID of the location level to retrieve.

        office_id : str
            The ID of the office associated with the location level.

        effective_date : datetime
            The effective date of the location level.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.

        unit : str, optional
            The unit of measurement for the location level.

        Returns
        -------
        response : dict
            The JSON response containing the location level information.

        Raises
        ------
        ValueError
            If `level_id`, `office_id`, or `effective_date` is None.

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
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

    def store_location_level_json(self, data: dict) -> None:
        """
        Parameters
        ----------
        data : dict
            The JSON data dictionary containing the location level information.

        Raises
        ------
        ValueError
            If either specified_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.

        """
        if dict is None:
            raise ValueError("Cannot store a location level without a JSON data dictionary")
        end_point = CwmsLevel._LEVELS_ENDPOINT
        headers = {
            "Content-Type": HEADER_JSON_V2
        }
        response = self.get_session().post(end_point, params=None, headers=headers, data=json.dumps(data))
        raise_for_status(response)

    def delete_location_level(self, location_level_id: str, office_id: str, effective_date: datetime = None,
                              cascade_delete: bool = None) -> None:
        """
        Parameters
        ----------
        location_level_id : str
            The ID of the location level to be deleted.
        office_id : str
            The ID of the office associated with the location level.
        effective_date : datetime, optional
            The effective date of the deletion. If not provided, the current date and time will be used.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
        cascade_delete : bool, optional
            If True, all related seasonal level data will also be deleted.
            If False or not provided and seasonal level data exists, an error will be thrown.

        Raises
        ------
        ValueError
            If either location_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.

        """
        if location_level_id is None:
            raise ValueError("Cannot delete a location level without an id")
        if office_id is None:
            raise ValueError("Cannot delete a location level without an office id")
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
        raise_for_status(response)

    def retrieve_level_as_timeseries_json(self, location_level_id: str, office_id: str, begin: datetime = None,
                                          end: datetime = None, interval: str = None) -> dict:
        """
        Parameters
        ----------
        location_level_id : str
            The ID of the location level for which the time series data will be retrieved.
        office_id : str
            The ID of the office for which the time series data will be retrieved.
        begin : datetime, optional
            The start datetime for the time series data. Defaults to None.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
        end : datetime, optional
            The end datetime for the time series data. Defaults to None.
            If the datetime has a timezone it will be used, otherwise it is assumed to be in UTC.
        interval : str, optional
            The interval at which the time series data will be established. Defaults to None.

        Returns
        -------
        response : dict
            The JSON response containing the time series data in JSON format.

        Raises
        ------
        ValueError
            If either location_level_id or office_id is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.

        """
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
