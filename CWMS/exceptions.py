#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from requests.exceptions import HTTPError
from requests.models import Response


class CwmsDataApiError(HTTPError):
    def __init__(self, message: str, response: Response):
        self.incident_identifier = response.json().get("incidentIdentifier")
        super().__init__(message, response=response)


class ClientError(CwmsDataApiError):
    def __init__(self, response: Response):
        message = (f"CWMS Client error occurred for request:\n{response.request.url}\n"
                   f"Response was:\n{response.text}")
        super().__init__(message, response=response)


class ServerError(CwmsDataApiError):
    def __init__(self, response: Response):
        message = (f"CWMS Server error occurred for request:\n{response.request.url}\n"
                   f"Response was:\n{response.text}")
        super().__init__(message, response=response)


class NoDataFoundError(CwmsDataApiError):
    def __init__(self, response: Response):
        message = (f"No data found for request:\n{response.request.url}\n"
                   f"Response was:\n{response.text}")
        super().__init__(message, response=response)
