#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from datetime import datetime
from typing import Optional

import requests

import cwms._constants as constants
from cwms.core import CwmsApiSession, _CwmsBase
from cwms.types import JSON
from cwms.utils import queryCDA, raise_for_status


class CwmsForecastInstance(_CwmsBase):
    """
    `CwmsForecastInstance` class

    This class provides methods to interact with forecast instances
    in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """

    _FORECAST_INSTANCE_ENDPOINT = "forecast-instance"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_forecast_instances_json(
        self,
        spec_id: Optional[str] = None,
        office: Optional[str] = None,
        designator: Optional[str] = None,
    ) -> JSON:
        """
        Parameters
        ----------
        spec_id : str, optional
            The forecast spec id.
        office : str, optional
            The spec office id.
        designator : str, optional
            The spec designator.

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

        Raises
        ------
        ValueError
            If any of spec_id, office, or designator is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
        if spec_id is None:
            raise ValueError("Retrieving forecast instances requires an id")
        if office is None:
            raise ValueError("Retrieving forecast instances requires an office")
        if designator is None:
            raise ValueError("Retrieving forecast instances requires a designator")
        end_point = CwmsForecastInstance._FORECAST_INSTANCE_ENDPOINT

        params = {
            constants.OFFICE_PARAM: office,
            constants.NAME: spec_id,
            constants.DESIGNATOR: designator,
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def retrieve_forecast_instance_json(
        self,
        spec_id: str,
        office: str,
        designator: str,
        forecast_date: datetime,
        issue_date: datetime,
    ) -> JSON:
        """
        Parameters
        ----------
        spec_id : str
            The ID of the forecast spec.
        office : str
            The ID of the office.
        designator : str
            The designator of the forecast spec

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

        Raises
        ------
        ValueError
            If any of spec_id, office, or designator is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """

        if spec_id is None:
            raise ValueError("Retrieve forecast instance requires an id")
        if office is None:
            raise ValueError("Retrieve a forecast instance requires an office")
        if designator is None:
            raise ValueError("Retrieve a forecast instance requires a designator")
        if forecast_date is None:
            raise ValueError("Retrieve a forecast instance requires a forecast date")
        if issue_date is None:
            raise ValueError("Retrieve a forecast instance requires a issue date")

        end_point = f"{CwmsForecastInstance._FORECAST_INSTANCE_ENDPOINT}/{spec_id}"

        params = {
            constants.OFFICE_PARAM: office,
            constants.DESIGNATOR: designator,
            constants.FORECAST_DATE: forecast_date.isoformat(),
            constants.ISSUE_DATE: issue_date.isoformat(),
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def store_forecast_instance_json(self, data: JSON) -> None:
        """
        This method is used to store a forecast instance through CWMS Data API.

        Parameters
        ----------
        data : dict
            A dictionary representing the JSON data to be stored.
            If the `data` value is None, a `ValueError` will be raised.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If dict is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
        if data is None:
            raise ValueError(
                "Storing a forecast instance requires a JSON data dictionary"
            )
        end_point = CwmsForecastInstance._FORECAST_INSTANCE_ENDPOINT

        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().post(
            end_point, headers=headers, data=json.dumps(data)
        )
        raise_for_status(response)

    def delete_forecast_instance(
        self,
        spec_id: str,
        office: str,
        designator: str,
        forecast_date: datetime,
        issue_date: datetime,
    ) -> None:
        """
        Parameters
        ----------
        spec_id : str
            The ID of the forecast spec.
        office : str
            The ID of the office.
        designator : str
            The designator of the forecast spec
        forecast_date : datetime
            The forecast date of the forecast instance
        issue_date : datetime
            The forecast issue date of the forecast instance

        Returns
        -------
        response : dict
            the JSON response from CWMS Data API.

        Raises
        ------
        ValueError
            If any of spec_id, office, designator,
            forecast_date, or issue_date is None.
        ClientError
            If a 400 range error code response is returned from the server.
        NoDataFoundError
            If a 404 range error code response is returned from the server.
        ServerError
            If a 500 range error code response is returned from the server.
        """
        if spec_id is None:
            raise ValueError("Deleting a forecast instance requires an id")
        if office is None:
            raise ValueError("Deleting a forecast instance requires an office")
        if designator is None:
            raise ValueError("Deleting a forecast instance requires a designator")
        if forecast_date is None:
            raise ValueError("Deleting a forecast instance requires a forecast date")
        if issue_date is None:
            raise ValueError("Deleting a forecast instance requires a issue date")

        end_point = f"{CwmsForecastInstance._FORECAST_INSTANCE_ENDPOINT}/{spec_id}"

        params = {
            constants.OFFICE_PARAM: office,
            constants.DESIGNATOR: designator,
            constants.FORECAST_DATE: forecast_date.isoformat(),
            constants.ISSUE_DATE: issue_date.isoformat(),
        }
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().delete(end_point, params=params, headers=headers)
        raise_for_status(response)

    def retrieve_large_forecast_file(self, url: str) -> bytes:
        """
        Retrieves large blob forecast file greater than 64kb from CWMS data api
        :param url: str
            Url used in query by CDA
        :return: bytes
            Large binary data
        """
        response = requests.get(url)
        return response.content
