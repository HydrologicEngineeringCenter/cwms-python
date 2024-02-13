#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import datetime
import json
from enum import Enum, auto

import CWMS._constants as constants
from .core import CwmsApiSession
from .core import _CwmsBase
from .utils import queryCDA
from .utils import raise_for_status


class TextTsMode(Enum):
    REGULAR = auto(),
    STANDARD = auto(),
    ALL = auto()


class CwmsTextTs(_CwmsBase):
    """
    `CwmsTextTs` class

    This class provides methods to interact with text time series in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """
    _TEXT_TS_ENDPOINT = "timeseries/text"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_text_ts_json(self, timeseries_id: str, office_id: str,
                              begin: datetime, end: datetime,
                              mode: TextTsMode = TextTsMode.REGULAR,
                              min_attribute: float = None,
                              max_attribute: float = None) -> dict:
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
        mode : TextTsMode, optional
            The mode for retrieving text timeseries data.
            Default is `TextTsMode.REGULAR`.
        min_attribute : float, optional
            The minimum attribute value to filter the timeseries data.
            Default is `None`.
        max_attribute : float, optional
            The maximum attribute value to filter the timeseries data.
            Default is `None`.

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

        params = {
            constants.OFFICE_PARAM: office_id,
            constants.NAME: timeseries_id,
            constants.MIN_ATTRIBUTE: min_attribute,
            constants.MAX_ATTRIBUTE: max_attribute,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.MODE: mode.name
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

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
        if dict is None:
            raise ValueError(
                "Cannot store a text time series without a JSON data dictionary")
        end_point = CwmsTextTs._TEXT_TS_ENDPOINT

        params = {
            "replace-all": replace_all
        }
        headers = {
            "Content-Type": constants.HEADER_JSON_V2
        }
        response = self.get_session().post(end_point, params=params,
                                           headers=headers,
                                           data=json.dumps(data))
        raise_for_status(response)

    def delete_text_ts(self, timeseries_id: str,
                               office_id: str, begin: datetime, end: datetime,
                               mode: TextTsMode = TextTsMode.REGULAR,
                               text_mask: str = "*",
                               min_attribute: float = None,
                               max_attribute: float = None) -> None:
        """
        Deletes text timeseries data with the given ID and office ID and time range.

        Parameters
        ----------
        timeseries_id : str
            The ID of the text time series data to be deleted.
        office_id : str
            The ID of the office that the text time series belongs to.
        text_mask : str, optional
            The standard text pattern to match.
            Use glob-style wildcard characters instead of sql-style wildcard
            characters for pattern matching.
            For StandardTextTimeSeries this should be the Standard_Text_Id
            (such as 'E' for ESTIMATED)
            Default value is `"*"`
        begin : datetime
            The start date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        end : datetime
            The end date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        mode : TextTsMode, optional
            The mode for deleting text timeseries data.
            Default is `TextTsMode.REGULAR`.
        min_attribute : float, optional
            The minimum attribute value to filter the timeseries data.
            Default is `None`.
        max_attribute : float, optional
            The maximum attribute value to filter the timeseries data.
            Default is `None`.

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

        params = {
            constants.OFFICE_PARAM: office_id,
            constants.MIN_ATTRIBUTE: min_attribute,
            constants.MAX_ATTRIBUTE: max_attribute,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.MODE: mode.name,
            "text-mask": text_mask
        }
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().delete(end_point, params=params,
                                             headers=headers)
        raise_for_status(response)
