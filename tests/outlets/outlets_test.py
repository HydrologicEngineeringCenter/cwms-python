#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
# constant for mock root url

import cwms.outlets.outlets as outlets
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

# constants for json payloads, replace with your actual json payloads
_OUTLET_JSON = read_resource_file("outlet.json")
_OUTLETS_JSON = read_resource_file("outlets.json")


def test_get_outlet(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/outlets/BIGH-TG1?office=SPK",
        json=_OUTLET_JSON,
    )
    data = outlets.get_outlet("SPK", "BIGH-TG1")
    assert data.json == _OUTLET_JSON


def test_get_outlets(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/outlets?office=SPK&project-id=BIGH",
        json=_OUTLETS_JSON,
    )
    data = outlets.get_outlets("SPK", "BIGH")
    assert data.json == _OUTLETS_JSON


def test_store_outlet(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/projects/outlets?fail-if-exists=True",
        status_code=200,
        json=_OUTLET_JSON,
    )
    outlets.store_outlet(_OUTLET_JSON, True)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_outlet(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/projects/outlets/Test?office=SPK&method=DELETE_ALL",
        status_code=200,
    )
    outlets.delete_outlet("SPK", "Test", DeleteMethod.DELETE_ALL)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_rename_outlet(requests_mock):
    requests_mock.patch(
        f"{_MOCK_ROOT}/projects/outlets/Test?office=SPK&name=Test2",
        status_code=200,
    )
    outlets.rename_outlet("SPK", "Test", "Test2")
    assert requests_mock.called
    assert requests_mock.call_count == 1
