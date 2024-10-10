""" Session management and REST functions for CWMS Data API.

This module provides functions for making REST calls to the CWMS Data API (CDA). These
functions should be used internally to interact with the API. The user should not have to
interact with these directly.

The `init_session()` function can be used to specify an alternative root URL, and to
provide an authentication key (if required). If `init_session()` is not called, the
default root URL (see `API_ROOT` below) will be used, and no authentication keys will be
included when making API calls.

Example: Initializing a session

    # Specify an alternate URL
    init_session(api_root="https://example.com/cwms-data")

    # Specify an alternate URL and an auth key
    init_session(api_root="https://example.com/cwms-data", api_key="API_KEY")

Functions which make API calls that _may_ return a JSON response will return a `dict`
containing the deserialized data. If the API response does not include data, an empty
`dict` will be returned.

In the event the API returns an error response, the function will raise an `APIError`
which includes the response object and provides some hints to the user on how to address
the error.
"""

import json
import logging
from json import JSONDecodeError
from typing import Any, Optional, cast

from requests import Response, adapters
from requests_toolbelt import sessions  # type: ignore
from requests_toolbelt.sessions import BaseUrlSession  # type: ignore

from cwms.cwms_types import JSON, RequestParams

# Specify the default API root URL and version.
API_ROOT = "https://cwms-data.usace.army.mil/cwms-data/"
API_VERSION = 2

# Initialize a non-authenticated session with the default root URL and set default pool connections.
SESSION = sessions.BaseUrlSession(base_url=API_ROOT)
adapter = adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
SESSION.mount("https://", adapter)


class InvalidVersion(Exception):
    pass


class ApiError(Exception):
    """CWMS Data Api Error.

    This class is a light wrapper around a `requests.Response` object. Its primary purpose
    is to generate an error message that includes the request URL and provide additional
    information to the user to help them resolve the error.
    """

    def __init__(self, response: Response):
        self.response = response

    def __str__(self) -> str:
        # Include the request URL in the error message.
        message = f"CWMS API Error ({self.response.url})"

        # If a reason is provided in the response, add it to the message.
        if reason := self.response.reason:
            message += f" {reason}"

        message += "."

        # Add additional context to help the user resolve the issue.
        if hint := self.hint():
            message += f" {hint}"

        if content := self.response.content:
            message += f" {content.decode('utf8')}"

        return message

    def hint(self) -> str:
        """Return a message with additional information on how to resolve the error."""

        if self.response.status_code == 400:
            return "Check that your parameters are correct."
        elif self.response.status_code == 404:
            return "May be the result of an empty query."
        else:
            return ""


def init_session(
    *,
    api_root: Optional[str] = None,
    api_key: Optional[str] = None,
    pool_connections: int = 100,
) -> BaseUrlSession:
    """Specify a root URL and authentication key for the CWMS Data API.

    This function can be used to change the root URL used when interacting with the CDA.
    All API calls made after this function is called will use the specified URL. If an
    authentication key is given it will be included in all future request headers.

    Keyword Args:
        api_root (optional): The root URL for the CWMS Data API.
        api_key (optional): An authentication key.

    Returns:
        Returns the updated session object.
    """

    global SESSION

    if api_root:
        logging.debug(f"Initializing root URL: api_root={api_root}")
        SESSION = sessions.BaseUrlSession(base_url=api_root)
        adapter = adapters.HTTPAdapter(
            pool_connections=pool_connections, pool_maxsize=pool_connections
        )
        SESSION.mount("https://", adapter)
    if api_key:
        logging.debug(f"Setting authorization key: api_key={api_key}")
        SESSION.headers.update({"Authorization": api_key})

    return SESSION


def return_base_url() -> str:
    """returns the base URL for the CDA instance that is connected to.

    Returns:
        str: base URL
    """

    return str(SESSION.base_url)


def api_version_text(api_version: int) -> str:
    """Initialize CDA request headers.

    The CDA supports multiple versions. To request a specific version, the version number
    must be included in the request headers.

    Args:
        api_version: The CDA version to use for the request.

    Returns:
        A dict containing the request headers.

    Raises:
        InvalidVersion: If an unsupported API version is specified.
    """

    if api_version == 1:
        version = "application/json"
    elif api_version == 2:
        version = "application/json;version=2"
    elif api_version == 102:
        version = "application/xml;version=2"
    else:
        raise InvalidVersion(f"API version {api_version} is not supported.")

    return version


def get_xml(
    endpoint: str,
    params: Optional[RequestParams] = None,
    *,
    api_version: int = API_VERSION,
) -> Any:
    """Make a GET request to the CWMS Data API.

    Args:
        endpoint: The CDA endpoint for the record(s).
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Returns:
        The deserialized JSON response data.

    Raises:
        ApiError: If an error response is return by the API.
    """

    headers = {"Accept": api_version_text(api_version)}
    response = SESSION.get(endpoint, params=params, headers=headers)

    if response.status_code < 200 or response.status_code >= 300:
        logging.error(f"CDA Error: response={response}")
        raise ApiError(response)

    try:
        return response.content.decode("utf-8")
    except JSONDecodeError as error:
        logging.error(f"Error decoding CDA response as xml: {error}")
        return {}


def get(
    endpoint: str,
    params: Optional[RequestParams] = None,
    *,
    api_version: int = API_VERSION,
) -> JSON:
    """Make a GET request to the CWMS Data API.

    Args:
        endpoint: The CDA endpoint for the record(s).
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Returns:
        The deserialized JSON response data.

    Raises:
        ApiError: If an error response is return by the API.
    """

    headers = {"Accept": api_version_text(api_version)}
    response = SESSION.get(endpoint, params=params, headers=headers)
    if response.status_code < 200 or response.status_code >= 300:
        logging.error(f"CDA Error: response={response}")
        raise ApiError(response)

    try:
        return cast(JSON, response.json())
    except JSONDecodeError as error:
        logging.error(f"Error decoding CDA response as json: {error}")
        return {}


def get_with_paging(
    selector: str,
    endpoint: str,
    params: RequestParams,
    *,
    api_version: int = API_VERSION,
) -> JSON:
    """Make a GET request to the CWMS Data API with paging.

    Args:
        endpoint: The CDA endpoint for the record(s).
        selector: The json key that will be merged though each page call
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Returns:
        The deserialized JSON response data.

    Raises:
        ApiError: If an error response is return by the API.
    """

    first_pass = True
    while (params["page"] is not None) or first_pass:
        temp = get(endpoint, params, api_version=api_version)
        if first_pass:
            response = temp
        else:
            response[selector] = response[selector] + temp[selector]
        if "next-page" in temp.keys():
            params["page"] = temp["next-page"]
        else:
            params["page"] = None
        first_pass = False
    return response


def post(
    endpoint: str,
    data: Any,
    params: Optional[RequestParams] = None,
    *,
    api_version: int = API_VERSION,
) -> None:
    """Make a POST request to the CWMS Data API.

    Args:
        endpoint: The CDA endpoint for the record type.
        data: A dict containing the new record data. Must be JSON-serializable.
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Returns:
        The deserialized JSON response data.

    Raises:
        ApiError: If an error response is return by the API.
    """

    # post requires different headers than get for
    headers = {"accept": "*/*", "Content-Type": api_version_text(api_version)}

    if isinstance(data, dict):
        data = json.dumps(data)

    response = SESSION.post(endpoint, params=params, headers=headers, data=data)

    if response.status_code < 200 or response.status_code >= 300:
        logging.error(f"CDA Error: response={response}")
        raise ApiError(response)


def patch(
    endpoint: str,
    data: Optional[Any] = None,
    params: Optional[RequestParams] = None,
    *,
    api_version: int = API_VERSION,
) -> None:
    """Make a PATCH request to the CWMS Data API.

    Args:
        endpoint: The CDA endpoint for the record.
        data: A dict containing the updated record data. Must be JSON-serializable.
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Returns:
        The deserialized JSON response data.

    Raises:
        ApiError: If an error response is return by the API.
    """

    headers = {"accept": "*/*", "Content-Type": api_version_text(api_version)}
    if data is None:
        response = SESSION.patch(endpoint, params=params, headers=headers)
    else:
        if isinstance(data, dict):
            data = json.dumps(data)
        response = SESSION.patch(endpoint, params=params, headers=headers, data=data)

    if response.status_code < 200 or response.status_code >= 300:
        logging.error(f"CDA Error: response={response}")
        raise ApiError(response)


def delete(
    endpoint: str,
    params: Optional[RequestParams] = None,
    *,
    api_version: int = API_VERSION,
) -> None:
    """Make a DELETE request to the CWMS Data API.

    Args:
        endpoint: The CDA endpoint for the record.
        params (optional): Query parameters for the request.

    Keyword Args:
        api_version (optional): The CDA version to use for the request. If not specified,
            the default API_VERSION will be used.

    Raises:
        ApiError: If an error response is return by the API.
    """

    headers = {"Accept": api_version_text(api_version)}
    response = SESSION.delete(endpoint, params=params, headers=headers)

    if response.status_code < 200 or response.status_code >= 300:
        logging.error(f"CDA Error: response={response}")
        raise ApiError(response)
