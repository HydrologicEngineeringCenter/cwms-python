import pytest

from cwms.api import SESSION, InvalidVersion, api_version_text, init_session


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
    session = init_session(api_root="https://example.com")

    # The URL should be set on the session.
    assert session.base_url == "https://example.com"
    assert "Authorization" not in session.headers


def test_session_init_api_key():
    """Verify that the authentication key for the session can be set."""

    # Initialize a session with both an alternate root URL and an authentication key.
    session = init_session(api_root="https://example.com", api_key="API_AUTH_KEY")

    # Both the URL and the auth key should be set on the session.
    assert session.base_url == "https://example.com"
    assert session.headers["Authorization"] == "API_AUTH_KEY"


def test_api_headers():
    """Verify that the API version headers are correct."""

    version = api_version_text(api_version=1)
    assert version == "application/json"

    version = api_version_text(api_version=2)
    assert version == "application/json;version=2"


def test_api_headers_invalid_version():
    """An exception should be raised if the API version is not valid."""

    with pytest.raises(InvalidVersion):
        version = api_version_text(api_version=3)
