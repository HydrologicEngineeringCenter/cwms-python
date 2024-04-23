from dataclasses import dataclass
from typing import Optional

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
        == "CWMS API Error (https://api.example.com/test) Not Found. May be the result of an empty query. incident identifier 34566432"
    )

    # The response should not include a reason, since it is not included in the response.
    response = Response(url="https://api.example.com/test", status_code=404)
    error = ApiError(response)

    assert (
        str(error)
        == "CWMS API Error (https://api.example.com/test). May be the result of an empty query."
    )

    # In the most minimal case, only the URL is included.
    response = Response(url="https://api.example.com/test", status_code=500)
    error = ApiError(response)

    assert str(error) == "CWMS API Error (https://api.example.com/test)."
