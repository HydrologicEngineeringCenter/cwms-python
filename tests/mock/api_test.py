import pytest
from requests.exceptions import RetryError as RequestsRetryError
from urllib3.exceptions import MaxRetryError, ResponseError

import cwms.api
from cwms.api import SESSION, ApiError, api_version_text, init_session

TEST_ENDPOINT = "/test-endpoint"


def test_session_default():
    """Verify the default root URL and auth headers."""

    # The global session object should be initialized with the default root URL and no
    # authentication key.
    assert SESSION.base_url == "https://cwms-data.usace.army.mil/cwms-data/"
    assert "Authorization" not in SESSION.headers

    # Initializing the session with no arguments should not modify the session.
    session = init_session()

    assert session.base_url == "https://cwms-data.usace.army.mil/cwms-data/"
    assert "Authorization" not in session.headers


def test_session_init_api_root():
    """Verify that the root URL for the session can be set."""

    # Initialize the session with an alternate root URL.
    session = init_session(api_root="https://example.com/cwms-data")

    # Confirm that if a user does not specify a trailing slash, that one is added.
    assert session.base_url == "https://example.com/cwms-data/"
    assert "Authorization" not in session.headers

    # Confirm if a user adds too many trailing slashes, that only one is kept.
    session = init_session(api_root="https://example.com/cwms-data//")
    assert session.base_url == "https://example.com/cwms-data/"
    assert "Authorization" not in session.headers


def test_session_init_api_key():
    """Verify that the authentication key for the session can be set."""

    # Initialize a session with both an alternate root URL and an authentication key.
    session = init_session(api_root="https://example.com/", api_key="API_AUTH_KEY")

    # Both the URL and the auth key should be set on the session.
    assert session.base_url == "https://example.com/"
    assert session.headers["Authorization"] == "apikey API_AUTH_KEY"


def test_api_headers():
    """Verify that the API version headers are correct."""

    version = api_version_text(api_version=1)
    assert version == "application/json"

    version = api_version_text(api_version=2)
    assert version == "application/json;version=2"


def test_retry_strategy_configuration():
    """Verify retry behavior preserves the original CDA error path."""

    retries = SESSION.adapters["https://"].max_retries

    assert 500 not in retries.status_forcelist
    assert retries.raise_on_status is False


def test_post_500_raises_api_error(monkeypatch):
    """Verify a 500 response is surfaced directly as ApiError."""

    class ResponseStub:
        url = "https://example.com/cwms-data/test-endpoint"
        ok = False
        status_code = 500
        reason = "Internal Server Error"
        content = b"incident identifier 34566432"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class SessionStub:
        def post(self, endpoint, params=None, headers=None, data=None):
            return ResponseStub()

    monkeypatch.setattr(cwms.api, "SESSION", SessionStub())

    with pytest.raises(ApiError) as error:
        cwms.api._post_function(endpoint=TEST_ENDPOINT, data={})

    assert error.value.response.status_code == 500
    assert "Internal Server Error" in str(error.value)
    assert "incident identifier 34566432" in str(error.value)


def test_retry_error_unwraps_original_cause(monkeypatch):
    """Verify wrapped retry failures propagate the underlying cause."""

    original_error = ResponseError("too many 503 error responses")
    wrapped_error = RequestsRetryError(
        MaxRetryError(pool=None, url=TEST_ENDPOINT, reason=original_error)
    )

    class SessionStub:
        def get(self, endpoint, params=None, headers=None):
            raise wrapped_error

    monkeypatch.setattr(cwms.api, "SESSION", SessionStub())

    with pytest.raises(ResponseError, match="too many 503 error responses"):
        cwms.api.get(TEST_ENDPOINT)
