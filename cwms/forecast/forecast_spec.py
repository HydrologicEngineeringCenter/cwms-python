#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import json
from enum import Enum, auto
from typing import Optional

import cwms._constants as constants
from cwms.core import CwmsApiSession, _CwmsBase
from cwms.types import JSON
from cwms.utils import queryCDA, raise_for_status


class DeleteMethod(Enum):
    DELETE_ALL = auto()
    DELETE_KEY = auto()
    DELETE_DATA = auto()


class CwmsForecastSpec(_CwmsBase):
    """
    `CwmsForecastSpec` class

    This class provides methods to interact with forecast specs
    in CWMS Data API.

    Notes
    -----
    Write operations require authentication
    """

    _FORECAST_SPEC_ENDPOINT = "forecast-spec"

    def __init__(self, cwms_api_session: CwmsApiSession):
        """
        Initialize the class.

        Parameters
        ----------
        cwms_api_session : CwmsApiSession
            The CWMS API session object.

        """
        super().__init__(cwms_api_session)

    def retrieve_forecast_specs_json(
        self,
        id_mask: Optional[str] = None,
        office: Optional[str] = None,
        designator_mask: Optional[str] = None,
        source_entity: Optional[str] = None,
    ) -> JSON:
        """
        Parameters
        ----------
        id_mask : str, optional
            The regex filter for the forecast spec id.
        office : str, optional
            The regex filter for the forecast spec id.
        designator_mask : str, optional
            The regex filter for the forecast spec id.
        source_entity : str, optional
            The regex filter for the forecast spec id.

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
        end_point = CwmsForecastSpec._FORECAST_SPEC_ENDPOINT

        params = {
            constants.OFFICE_PARAM: office,
            constants.ID_MASK: id_mask,
            constants.DESIGNATOR_MASK: designator_mask,
            constants.SOURCE_ENTITY: source_entity,
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def retrieve_forecast_spec_json(
        self, spec_id: str, office: str, designator: str
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
            raise ValueError("Retrieve forecast spec requires an id")
        if office is None:
            raise ValueError("Retrieve a forecast spec requires an office")
        if designator is None:
            raise ValueError("Retrieve a forecast spec requires a designator")

        end_point = f"{CwmsForecastSpec._FORECAST_SPEC_ENDPOINT}/{spec_id}"

        params = {
            constants.OFFICE_PARAM: office,
            constants.DESIGNATOR: designator,
        }

        headers = {"Accept": constants.HEADER_JSON_V2}

        return queryCDA(self, end_point, params, headers)

    def store_forecast_spec_json(self, data: JSON) -> None:
        """
        This method is used to store a forecast spec through CWMS Data API.

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
            raise ValueError("Storing a forecast spec requires a JSON data dictionary")
        end_point = CwmsForecastSpec._FORECAST_SPEC_ENDPOINT

        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().post(
            end_point, headers=headers, data=json.dumps(data)
        )
        raise_for_status(response)

    def delete_forecast_spec(
        self,
        spec_id: str,
        office: str,
        designator: str,
        delete_method: DeleteMethod = DeleteMethod.DELETE_KEY,
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
        delete_method: DeleteMethod
            The method to use to delete forecast spec data

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
            raise ValueError("Deleting a forecast spec requires an id")
        if office is None:
            raise ValueError("Deleting a forecast spec requires an office")
        if designator is None:
            raise ValueError("Deleting a forecast spec requires a designator")

        end_point = f"{CwmsForecastSpec._FORECAST_SPEC_ENDPOINT}/{spec_id}"
        print(delete_method.name)
        params = {
            constants.OFFICE_PARAM: office,
            constants.DESIGNATOR: designator,
            constants.METHOD: delete_method.name,
        }
        headers = {"Content-Type": constants.HEADER_JSON_V2}
        response = self.get_session().delete(end_point, params=params, headers=headers)
        raise_for_status(response)
