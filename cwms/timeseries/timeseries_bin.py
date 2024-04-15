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


class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


class CwmsBinTs(_CwmsBase):
    """
    `CwmsBinTs` class

    This class provides methods to interact with binary time series
    in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """

    _BIN_TS_ENDPOINT = "timeseries/binary"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_bin_ts_json(
        self,
        timeseries_id: str,
        office_id: str,
        begin: datetime,
        end: datetime,
        version_date: Optional[datetime] = None,
        bin_type_mask: Optional[str] = "*",
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
            window will be retrieved.
        bin_type_mask : str, optional
            The binary media type pattern to match.
            Use glob-style wildcard characters instead of sql-style wildcard
            characters for pattern matching.
            Default value is `"*"`

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
            raise ValueError("Retrieve binary timeseries requires an id")
        if office_id is None:
            raise ValueError("Retrieve binary timeseries requires an office")
        if begin is None:
            raise ValueError("Retrieve binary timeseries requires a time window")
        if end is None:
            raise ValueError("Retrieve binary timeseries requires a time window")

        end_point = CwmsBinTs._BIN_TS_ENDPOINT

        version_date_str = version_date.isoformat() if version_date else ""
        params = {
            constants.OFFICE_PARAM: office_id,
            constants.NAME: timeseries_id,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.VERSION_DATE: version_date_str,
            constants.BINARY_TYPE_MASK: bin_type_mask,
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def retrieve_large_blob(self, url):
        """
        Retrieves large clob data from CWMS data api
        :param url:
            Url used by CDA in query
        :return:
            Large text data
        """
        response = requests.get(url)
        return response.content

    def store_bin_ts_json(self, data: JSON, replace_all: bool = False) -> None:
        """
        This method is used to store a binary time series through CWMS Data API.

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
                "Storing binary time series requires a JSON data dictionary"
            )
        end_point = CwmsBinTs._BIN_TS_ENDPOINT

        params = {constants.REPLACE_ALL: replace_all}
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().post(
            end_point, params=params, headers=headers, data=json.dumps(data)
        )
        raise_for_status(response)

    def delete_bin_ts(
        self,
        timeseries_id: str,
        office_id: str,
        begin: datetime,
        end: datetime,
        version_date: Optional[datetime] = None,
        bin_type_mask: Optional[str] = "*",
    ) -> None:
        """
        Deletes binary timeseries data with the given ID,
        office ID and time range.

        Parameters
        ----------
        timeseries_id : str
            The ID of the binary time series data to be deleted.
        office_id : str
            The ID of the office that the binary time series belongs to.
        begin : datetime
            The start date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        end : datetime
            The end date and time of the time range.
            If the datetime has a timezone it will be used,
            otherwise it is assumed to be in UTC.
        version_date : Optional[datetime]
            The time series date version to retrieve. If not supplied,
            the maximum date version for each time step in the retrieval
            window will be deleted.
        bin_type_mask : str, optional
            The binary media type pattern to match.
            Use glob-style wildcard characters instead of sql-style wildcard
            characters for pattern matching.
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
            raise ValueError("Deleting binary timeseries requires an id")
        if office_id is None:
            raise ValueError("Deleting binary timeseries requires an office")
        if begin is None:
            raise ValueError("Deleting binary timeseries requires a time window")
        if end is None:
            raise ValueError("Deleting binary timeseries requires a time window")
        end_point = f"{CwmsBinTs._BIN_TS_ENDPOINT}/{timeseries_id}"

        version_date_str = version_date.isoformat() if version_date else ""
        params = {
            constants.OFFICE_PARAM: office_id,
            constants.BEGIN: begin.isoformat(),
            constants.END: end.isoformat(),
            constants.VERSION_DATE: version_date_str,
            constants.BINARY_TYPE_MASK: bin_type_mask,
        }
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().delete(end_point, params=params, headers=headers)
        raise_for_status(response)
