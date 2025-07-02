#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pytest

import cwms.api
import cwms.timeseries.timeseries_profile_parser as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TSP_PARSER_JSON = read_resource_file("timeseries_profile_indexed.json")
_TSP_PARSER_ARRAY_JSON = read_resource_file("timeseries_profiles_indexed.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_timeseries_profile_parser(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/timeseries/profile-parser/SWAN/Temp?office=SWT",
        json=_TSP_PARSER_JSON,
    )

    location_id = "SWAN"
    office_id = "SWT"
    parameter_id = "Temp"

    data = timeseries.get_timeseries_profile_parser(
        office_id, location_id, parameter_id
    )

    assert data.json == _TSP_PARSER_JSON


def test_store_timeseries_profile_parser(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/timeseries/profile-parser?fail-if-exists=False")

    data = _TSP_PARSER_JSON
    timeseries.store_timeseries_profile_parser(data, False)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_timeseries_profile_parser(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}" "/timeseries/profile-parser/SWAN/Length?office=SWT",
        json=_TSP_PARSER_JSON,
    )

    parameter_id = "Length"
    location_id = "SWAN"
    office_id = "SWT"

    timeseries.delete_timeseries_profile_parser(office_id, location_id, parameter_id)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_all_timeseries_profile_parser(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/timeseries/profile-parser",
        json=_TSP_PARSER_ARRAY_JSON,
    )

    data = timeseries.get_timeseries_profile_parsers("*", "*", "*")

    assert data.json == _TSP_PARSER_ARRAY_JSON
