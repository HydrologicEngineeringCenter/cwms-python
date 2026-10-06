from dataclasses import dataclass
from typing import Optional

import pytest
import requests

from cwms.api import ApiError


@dataclass
class Response:
    url: str
    status_code: int
    reason: Optional[str] = None
    content: Optional[str] = None


def test_api_error():
    """Verify that the error object contains the response."""

    response = Response(
        url="https://api.example.com/test",
        status_code=404,
        reason="Not Found",
        content=b"incident identifier 34566432",
    )
    error = ApiError(response)

    assert error.response == response


def test_api_error_str():
    """Verify the string representation of the error."""

    # The error should include both the reason returned from the API, as well as a hint
    # message.
    response = Response(
        url="https://api.example.com/test",
        status_code=404,
        reason="Not Found",
        content=b"incident identifier 34566432",
    )
    error = ApiError(response)

    assert (
        str(error)
        == "CWMS API Error (https://api.example.com/test) 404 Not Found. May be the result of an empty query. incident identifier 34566432"
    )

    # The response should not include a reason, since it is not included in the response.
    response = Response(url="https://api.example.com/test", status_code=404)
    error = ApiError(response)

    assert (
        str(error)
        == "CWMS API Error (https://api.example.com/test) 404. May be the result of an empty query."
    )

    # Even without a reason or body, the URL and status are included.
    response = Response(url="https://api.example.com/test", status_code=500)
    error = ApiError(response)

    assert str(error) == "CWMS API Error (https://api.example.com/test) 500."


@pytest.mark.parametrize("method", ["GET", "POST", "PATCH", "DELETE"])
def test_404_hint_and_database_details_without_reason(method):
    """Internal HTTP CDA writes can return 404 with a database incident body."""
    response = requests.Response()
    response.status_code = 404
    response.url = "http://example.com/cwms-data/timeseries"
    response.request = requests.Request(method, response.url).prepare()
    response._content = (
        b'{"message":"ORA-20998: ERROR",'
        b'"incidentIdentifier":"test-incident","source":"Database","details":{}}'
    )
    error = ApiError(response)
    message = str(error)
    assert f"404 {method}" in message
    assert response.url in message
    assert response.text in message
    assert ("empty query" in message) == (method == "GET")
    assert error.response is response
