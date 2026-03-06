#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json

import pytest

import cwms.api
import cwms.users.users as users

_MOCK_ROOT = "https://mockwebserver.cwms.gov"


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_store_user(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/user/Actions/roles/SPK",
        status_code=200,
    )

    users.store_user("Actions", "SPK", ["CWMS User", "Viewer"])

    assert requests_mock.called
    assert requests_mock.call_count == 1
    assert requests_mock.request_history[0].method == "POST"
    assert json.loads(requests_mock.request_history[0].text) == ["CWMS User", "Viewer"]


def test_update_user(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/users/Actions",
        json={
            "user-name": "Actions",
            "principal": "actions@github",
            "email": "actions@github.com",
            "roles": {"SPK": ["CWMS User", "Viewer"]},
        },
    )
    requests_mock.delete(
        f"{_MOCK_ROOT}/user/Actions/roles/SPK",
        status_code=200,
    )
    requests_mock.post(
        f"{_MOCK_ROOT}/user/Actions/roles/SPK",
        status_code=200,
    )

    users.update_user("Actions", "SPK", ["CWMS User", "Manager"])

    assert requests_mock.called
    assert requests_mock.call_count == 3
    assert requests_mock.request_history[0].method == "GET"
    assert requests_mock.request_history[1].method == "DELETE"
    assert requests_mock.request_history[2].method == "POST"
    assert json.loads(requests_mock.request_history[1].text) == ["Viewer"]
    assert json.loads(requests_mock.request_history[2].text) == ["Manager"]


def test_update_user_no_role_changes(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/users/Actions",
        json={
            "user-name": "Actions",
            "principal": "actions@github",
            "email": "actions@github.com",
            "roles": {"SPK": ["CWMS User", "Viewer"]},
        },
    )

    users.update_user("Actions", "SPK", ["CWMS User", "Viewer"])

    assert requests_mock.called
    assert requests_mock.call_count == 1
    assert requests_mock.request_history[0].method == "GET"
