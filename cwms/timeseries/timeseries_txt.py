#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime
from enum import Enum, auto
from typing import Optional

import cwms._constants as constants
import requests
from cwms.core import CwmsApiSession
from cwms.core import _CwmsBase
from cwms.utils import queryCDA, raise_for_status

class TextTsMode(Enum):
    REGULAR = auto(),
    STANDARD = auto(),
    ALL = auto()

class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


class CwmsTextTs(_CwmsBase):
    """
    `CwmsTextTs` class

    This class provides methods to interact with text time series in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """

    _TEXT_TS_ENDPOINT = "timeseries/text"
    _STD_TEXT_ENDPOINT = f"{_TEXT_TS_ENDPOINT}/standard-text-id"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_text_ts_json(
        self,
        timeseries_id: str,
        office_id: str,
        begin: datetime,
        end: datetime,
        version_date: Optional[datetime] = None,
    ) -> JSON:
        """
        Parameters
        ----------
        timeseries_id : str
            The ID of the timeseries.
        office_id : str
            The ID of the office.
        begin : datetime
            The start date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        end : datetime
            The end date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        version_date : datetime, optional
            The time series date version to retrieve. If not supplied,
            the maximum date version for each time step in the retrieval
            window will be returned.

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

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

        if timeseries_id is None:
            raise ValueError("Retrieve text timeseries requires an id")
        if office_id is None:
            raise ValueError("Retrieve text timeseries requires an office")
        if begin is None:
            raise ValueError("Retrieve text timeseries requires a time window")
        if end is None:
            raise ValueError("Retrieve text timeseries requires a time window")

        end_point = CwmsTextTs._TEXT_TS_ENDPOINT
        version_date_str = version_date.isoformat() if version_date else ""
        params = {
            constants.OFFICE_PARAM: office_id,
            constants.NAME: timeseries_id,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.VERSION_DATE: version_date_str,
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def retrieve_large_clob(self, url):
        """
        Retrieves large clob data from CWMS data api
        :param url:
            Url used by CDA in query
        :return:
            Large text data
        """
        response = requests.get(url)
        return response.content.decode('utf-8')

    def store_text_ts_json(self, data: dict,
                           replace_all: bool = False) -> None:
        """
        This method is used to store a text time series through CWMS Data API.

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
            raise ValueError(
                "Cannot store a text time series without a JSON data dictionary"
            )
        end_point = CwmsTextTs._TEXT_TS_ENDPOINT

        params = {"replace-all": replace_all}
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().post(
            end_point, params=params, headers=headers, data=json.dumps(data)
        )
        raise_for_status(response)

    def delete_text_ts(
        self,
        timeseries_id: str,
        office_id: str,
        begin: datetime,
        end: datetime,
        version_date: Optional[datetime] = None,
        text_mask: Optional[str] = "*",
    ) -> None:
        """
        Deletes text timeseries data with the given ID and office ID and time range.

        Parameters
        ----------
        timeseries_id : str
            The ID of the text time series data to be deleted.
        office_id : str
            The ID of the office that the text time series belongs to.
        begin : datetime
            The start date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        end : datetime
            The end date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        version_date : datetime, optional
            The time series date version to retrieve. If not supplied,
            the maximum date version for each time step in the retrieval
            window will be deleted.
        text_mask : str, optional
            The standard text pattern to match.
            Use glob-style wildcard characters instead of sql-style wildcard
            characters for pattern matching.
            For StandardTextTimeSeries this should be the Standard_Text_Id
            (such as 'E' for ESTIMATED)
            Default value is `"*"`

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
        if timeseries_id is None:
            raise ValueError("Deleting text timeseries requires an id")
        if office_id is None:
            raise ValueError("Deleting text timeseries requires an office")
        if begin is None:
            raise ValueError("Deleting text timeseries requires a time window")
        if end is None:
            raise ValueError("Deleting text timeseries requires a time window")
        end_point = f"{CwmsTextTs._TEXT_TS_ENDPOINT}/{timeseries_id}"

        version_date_str = version_date.isoformat() if version_date else ""
        params = {
            constants.OFFICE_PARAM: office_id,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.VERSION_DATE: version_date_str,
            "text-mask": text_mask,
        }
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().delete(end_point, params=params, headers=headers)
        raise_for_status(response)

    def retrieve_std_txt_cat_json(
        self, text_id_mask: Optional[str] = None, office_id_mask: Optional[str] = None
    ) -> JSON:
        """
        Retrieves standard text catalog for the given ID and office ID filters.

        Parameters
        ----------
        text_id_mask : str
            The ID filter of the standard text value to retrieve.
        office_id_mask : str
            The ID filter of the office that the standard text belongs to.

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

        Raises
        ------
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """

        params = {"text-id-mask": text_id_mask, "office-id-mask": office_id_mask}
        headers = {"Accept": constants.HEADER_JSON_V2}
        end_point = f"{CwmsTextTs._STD_TEXT_ENDPOINT}"
        return queryCDA(self, end_point, params, headers)

    def retrieve_std_txt_json(self, text_id: str, office_id: str) -> JSON:
        """
        Retrieves standard text for the given ID and office ID.

        Parameters
        ----------
        text_id : str
            The ID of the standard text value to retrieve.
        office_id : str
            The ID of the office that the standard text belongs to.

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

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
        if text_id is None:
            raise ValueError("Retrieving standard text requires an id")
        if office_id is None:
            raise ValueError("Retrieving standard timeseries requires an office")

        params = {constants.OFFICE_PARAM: office_id}
        headers = {"Accept": constants.HEADER_JSON_V2}
        end_point = f"{CwmsTextTs._STD_TEXT_ENDPOINT}/{text_id}"
        return queryCDA(self, end_point, params, headers)

    def delete_std_txt(
        self, text_id: str, delete_method: DeleteMethod, office_id: str
    ) -> None:
        """
        Deletes standard text for the given ID and office ID.

        Parameters
        ----------
        text_id : str
            The ID of the standard text value to be deleted.
        office_id : str
            The ID of the office that the standard text belongs to.
        delete_method : str
            Delete method for the standard text id.
            DELETE_ALL - deletes the key and the value from the clob table
            DELETE_KEY - deletes the text id key, but leaves the value in the clob table
            DELETE_DATA - deletes the value from the clob table but leaves the text id key

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
        if text_id is None:
            raise ValueError("Deleting standard text requires an id")
        if office_id is None:
            raise ValueError("Deleting standard timeseries requires an office")
        if delete_method is None:
            raise ValueError("Deleting standard timeseries requires a delete method")

        params = {constants.OFFICE_PARAM: office_id, "method": delete_method.name}
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        end_point = f"{CwmsTextTs._STD_TEXT_ENDPOINT}/{text_id}"
        response = self.get_session().delete(end_point, params=params, headers=headers)
        raise_for_status(response)

    def store_std_txt_json(self, data: JSON, fail_if_exists: bool = False) -> None:
        """
        This method is used to store a standard text value through CWMS Data API.

        Parameters
        ----------
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.
        fail_if_exists : str, optional
            Throw a ClientError if the text id already exists.
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
            raise ValueError(
                "Cannot store a standard text without a JSON data dictionary"
            )
        end_point = CwmsTextTs._STD_TEXT_ENDPOINT

        params = {"fail-if-exists": fail_if_exists}
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().post(
            end_point, params=params, headers=headers, data=json.dumps(data)
        )
        raise_for_status(response)
