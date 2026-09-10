"""Local error-path coverage; all HTTP responses are mocked."""

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from requests import Response
from requests.exceptions import Timeout

import cwms.api as api
import cwms.timeseries.timeseries as ts
import cwms.users.users as users

ROOT = "https://errors.example.test/"
CALLS = [
    ("GET", lambda: api.get("test")),
    ("GET", lambda: api.get_xml("test")),
    ("GET", lambda: api.get_with_paging("values", "test", {"page": None})),
    ("POST", lambda: api.post("test", {})),
    ("POST", lambda: api.post_with_returned_data("test", {})),
    ("PATCH", lambda: api.patch("test", {})),
    ("DELETE", lambda: api.delete("test")),
]


@pytest.fixture(autouse=True)
def session(monkeypatch):
    monkeypatch.setattr(api, "SESSION", api.sessions.BaseUrlSession(base_url=ROOT))


@pytest.mark.parametrize("method,call", CALLS)
@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 429, 500, 503])
def test_http_errors_keep_response_details(requests_mock, method, call, status):
    requests_mock.register_uri(
        method,
        ROOT + "test",
        status_code=status,
        json={"message": "synthetic failure", "incident-id": "test-123"},
    )
    with pytest.raises(api.ApiError) as caught:
        call()
    assert caught.value.response.status_code == status
    assert str(status) in str(caught.value)
    assert method in str(caught.value)
    assert "synthetic failure" in str(caught.value)
    assert "test-123" in str(caught.value)


@pytest.mark.parametrize("method,call", CALLS)
def test_transport_errors_propagate(requests_mock, method, call):
    original = Timeout("synthetic timeout")
    requests_mock.register_uri(method, ROOT + "test", exc=original)
    with pytest.raises(Timeout) as caught:
        call()
    assert caught.value is original


@pytest.mark.parametrize("method,call", [CALLS[0], CALLS[4]])
def test_invalid_json_is_an_error(requests_mock, method, call):
    requests_mock.register_uri(
        method,
        ROOT + "test",
        text="{broken",
        headers={"Content-Type": "application/json"},
    )
    with pytest.raises(api.ApiError, match="Invalid JSON") as caught:
        call()
    assert caught.value.response.text == "{broken"
    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.parametrize(
    "content_type,text,expected",
    [
        ("application/json", "", {}),
        ("text/plain", "plain response", "plain response"),
        ("application/xml", "<rating/>", "<rating/>"),
    ],
)
def test_valid_response_formats(requests_mock, content_type, text, expected):
    requests_mock.get(ROOT + "test", text=text, headers={"Content-Type": content_type})
    assert api.get("test") == expected


def test_later_page_error_is_not_partial_success(requests_mock):
    requests_mock.get(
        ROOT + "test",
        [
            {"json": {"values": [1], "next-page": "page-two"}},
            {"status_code": 500, "text": "second page failed"},
        ],
    )
    with pytest.raises(api.ApiError, match="second page failed"):
        api.get_with_paging("values", "test", {"page": None})
    assert requests_mock.call_count == 2


def test_custom_user_error_keeps_server_details(requests_mock):
    requests_mock.delete(
        ROOT + "user/test-user/roles/TEST", status_code=403, text="incident test-456"
    )
    with pytest.raises(api.PermissionError) as caught:
        users.delete_user_roles("test-user", "TEST", ["test-role"])
    assert "incident test-456" in str(caught.value)
    assert "role deletion" in str(caught.value)
    assert isinstance(caught.value.__cause__, api.ApiError)
    assert requests_mock.last_request.json() == ["test-role"]


def test_debug_outcome_does_not_log_credentials_or_body(requests_mock, caplog):
    api.SESSION.headers["Authorization"] = "Bearer synthetic-secret"
    requests_mock.post(ROOT + "test", status_code=204)
    with caplog.at_level(logging.DEBUG, logger="cwms.api"):
        api.post("test", {"private": "synthetic-body"})
    assert "CDA POST test returned HTTP 204" in caplog.text
    assert "synthetic-secret" not in caplog.text
    assert "synthetic-body" not in caplog.text


def test_multi_fetch_reports_all_failures_without_partial_result(monkeypatch):
    originals = {"bad-one": ValueError("first"), "bad-two": Timeout("second")}
    visited = []

    def fetch(ts_id, **kwargs):
        visited.append(ts_id)
        if ts_id in originals:
            raise originals[ts_id]
        return SimpleNamespace(json={"units": "ft"}, df=pd.DataFrame())

    monkeypatch.setattr(ts, "get_timeseries", fetch)
    with pytest.raises(RuntimeError) as caught:
        ts.get_multi_timeseries_df(["bad-one", "good", "bad-two"], "TEST")
    assert set(visited) == {"bad-one", "good", "bad-two"}
    assert dict(caught.value.failures) == originals
    assert caught.value.__cause__ in originals.values()


def test_chunk_fetch_keeps_original_exceptions(monkeypatch):
    original = ValueError("invalid chunk")
    monkeypatch.setattr(ts, "get_timeseries_chunk", Mock(side_effect=original))
    begin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    chunks = [
        (begin, begin + timedelta(days=1)),
        (begin + timedelta(days=1), begin + timedelta(days=2)),
    ]
    with pytest.raises(RuntimeError) as caught:
        ts.fetch_timeseries_chunks(chunks, {}, "values", "timeseries", 2)
    assert len(caught.value.failures) == 2
    assert all(error is original for _, error in caught.value.failures)
    assert caught.value.__cause__ is original


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409])
def test_permanent_chunk_errors_are_not_retried(status):
    response = Response()
    response.status_code = status
    original = api.ApiError(response)
    call = Mock(side_effect=original)
    with pytest.raises(api.ApiError) as caught:
        ts._call_with_retry(call)
    assert caught.value is original
    assert call.call_count == 1


def test_programming_errors_are_not_retried():
    call = Mock(side_effect=TypeError("not serializable"))
    with pytest.raises(TypeError):
        ts._call_with_retry(call)
    assert call.call_count == 1


def test_transient_errors_retry_and_preserve_final_error():
    original = Timeout("timeout")
    call = Mock(side_effect=[original, "success"])
    assert ts._call_with_retry(call, attempts=2) == "success"
    call = Mock(side_effect=original)
    with pytest.raises(Timeout) as caught:
        ts._call_with_retry(call, attempts=2)
    assert caught.value is original
    assert call.call_count == 2


def test_extent_failure_is_not_suppressed(monkeypatch):
    original = ValueError("invalid extents")
    monkeypatch.setattr(ts, "get_ts_extents", Mock(side_effect=original))
    fallback = Mock()
    monkeypatch.setattr(api, "get_with_paging", fallback)
    with pytest.raises(ValueError, match="invalid extents"):
        ts.get_timeseries(
            "test-series", "TEST", begin=datetime(2010, 1, 1, tzinfo=timezone.utc)
        )
    fallback.assert_not_called()


def test_chunk_store_preserves_failures_and_initialization_order(monkeypatch):
    original = ValueError("bad value")
    visited = []

    def post(endpoint, data, params):
        value = data["values"][0][0]
        if value != 0:
            assert 0 in visited
        visited.append(value)
        if value == 1:
            raise original

    monkeypatch.setattr(api, "post", post)
    data = {
        "name": "test-series",
        "office-id": "TEST",
        "units": "ft",
        "values": [[0, 1, 0], [1, 2, 0], [2, 3, 0]],
    }
    with pytest.raises(RuntimeError) as caught:
        ts.store_timeseries(data, chunk_size=1)
    assert sorted(visited) == [0, 1, 2]
    assert caught.value.failures == [("Error storing chunk from 1 to 1", original)]
    assert caught.value.__cause__ is original


def test_initial_chunk_error_stops_remaining_writes(monkeypatch):
    original = TypeError("invalid value")
    post = Mock(side_effect=original)
    monkeypatch.setattr(api, "post", post)
    data = {
        "name": "test-series",
        "office-id": "TEST",
        "units": "ft",
        "values": [[0, 1, 0], [1, 2, 0]],
    }
    with pytest.raises(TypeError) as caught:
        ts.store_timeseries(data, chunk_size=1)
    assert caught.value is original
    assert post.call_count == 1


def test_multi_store_preserves_all_original_failures(monkeypatch):
    originals = {"test-one": ValueError("first"), "test-two": TypeError("second")}

    def store(data, **kwargs):
        raise originals[data["name"]]

    monkeypatch.setattr(ts, "store_timeseries", store)
    data = pd.DataFrame(
        {
            "ts_id": list(originals),
            "units": ["ft", "ft"],
            "date-time": pd.to_datetime(["2025-01-01", "2025-01-01"], utc=True),
            "value": [1.0, 2.0],
        }
    )
    with pytest.raises(RuntimeError) as caught:
        ts.store_multi_timeseries_df(data, "TEST")
    assert {
        context.split(":")[0]: error for context, error in caught.value.failures
    } == originals


@pytest.mark.parametrize("attempts", [0, -1])
def test_invalid_retry_count_is_rejected(attempts):
    with pytest.raises(ValueError, match="attempts"):
        ts._call_with_retry(Mock(), attempts=attempts)


@pytest.mark.parametrize("days", [0, -1])
def test_invalid_chunk_duration_is_rejected(days):
    begin = datetime(2025, 1, 1)
    with pytest.raises(ValueError, match="chunk_size"):
        ts.chunk_timeseries_time_range(
            begin, begin + timedelta(days=1), timedelta(days=days)
        )
