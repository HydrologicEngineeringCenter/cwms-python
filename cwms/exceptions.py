#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from requests.exceptions import HTTPError
from requests.models import Response


class CwmsDataApiError(HTTPError):
    """
    Class representing an error thrown by the CwmsDataApi.

    This class inherits from the requests.exceptions HTTPError class.

    Attributes
    ----------
    incident_identifier : str
        The incident identifier extracted from the response object.
    response : Response
        The response from the server
    request : Request
        The request made to the server
    """

    def __init__(self, message: str, response: Response):
        """
        Parameters
        ----------
        message : str
            The message to be passed to the parent class constructor.
        response : Response
            The response object containing information about the incident identifier.

        """
        if response.text:
            self.incident_identifier = response.json().get("incidentIdentifier")
        super().__init__(message, response=response)


class ClientError(CwmsDataApiError):
    """
    ClientError
        Exception class representing the error when an error code in the 400 range is returned for a request.

    Attributes
    ----------
    incident_identifier : str
        The incident identifier extracted from the response object.
    response : Response
        The response from the server
    request : Request
        The request made to the server
    """

    def __init__(self, response: Response):
        """
        Parameters
        ----------
        response : Response
            The response object that triggered the error.
        """
        message = (
            f"CWMS Client error occurred for request:\n{response.request.url}\n"
            f"Response was:\n{response.text}"
        )
        super().__init__(message, response=response)


class ServerError(CwmsDataApiError):
    """
    ServerError
        Exception class representing the error when an error code in the 500 range is returned for a request.

    Attributes
    ----------
    incident_identifier : str
        The incident identifier extracted from the response object.
    response : Response
        The response from the server
    request : Request
        The request made to the server
    """

    def __init__(self, response: Response):
        """
        Constructs a ServerError instance.

        Parameters
        ----------
        response : Response
            The response object from the CWMS server.

        """
        message = (
            f"CWMS Server error occurred for request:\n{response.request.url}\n"
            f"Response was:\n{response.text}"
        )
        super().__init__(message, response=response)


class NoDataFoundError(CwmsDataApiError):
    """
    NoDataFoundError
        Exception class representing the error when no data is found for a request and an error code 404 is returned.

    Attributes
    ----------
    incident_identifier : str
        The incident identifier extracted from the response object.
    response : Response
        The response from the server
    request : Request
        The request made to the server
    """

    def __init__(self, response: Response):
        """
        Constructs a NoDataFoundError instance.

        Parameters
        ----------
        response : Response
            The response object from the CWMS server.

        """
        message = (
            f"No data found for request:\n{response.request.url}\n"
            f"Response was:\n{response.text}"
        )
        super().__init__(message, response=response)
