#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz
from requests.exceptions import HTTPError

import cwms.api
import cwms.levels.specified_levels as specified_levels
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_SPEC_LEVELS_JSON = read_resource_file("specified_levels.json")
_SPEC_LEVEL_JSON = read_resource_file("specified_level.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_specified_levels_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/specified-levels?office=%2A&template-id-mask=%2A",
        json=_SPEC_LEVELS_JSON,
    )
    levels = specified_levels.get_specified_levels()
    assert levels.json == _SPEC_LEVELS_JSON


def test_get_specified_levels(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/specified-levels?office=SWT&template-id-mask=%2A",
        json=_SPEC_LEVELS_JSON,
    )
    levels = specified_levels.get_specified_levels("*", "SWT")
    assert levels.json == _SPEC_LEVELS_JSON


def test_store_specified_level(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/specified-levels?fail-if-exists=True",
        status_code=200,
        json=_SPEC_LEVEL_JSON,
    )
    specified_levels.store_specified_level(_SPEC_LEVEL_JSON)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_specified_level(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/specified-levels/Test?office=SWT",
        status_code=200,
    )
    specified_levels.delete_specified_level("Test", "SWT")
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_update_specified_level(requests_mock):
    requests_mock.patch(
        f"{_MOCK_ROOT}/specified-levels/Test?specified-level-id=TEst2&office=SWT",
        status_code=200,
    )
    specified_levels.update_specified_level("Test", "Test2", "SWT")
    assert requests_mock.called
    assert requests_mock.call_count == 1
