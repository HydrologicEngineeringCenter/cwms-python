#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pytest

import cwms.api
import cwms.timeseries.timeseries_profile as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TSP_JSON = read_resource_file("timeseries_profile.json")
_TSP_ARRAY_JSON = read_resource_file("timeseries_profiles.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_timeseries_profile(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/profile/Depth?office=SWT&"
        "location-id=SWT.TEST.Text.Inst.1Hour.0.MockTest",
        json=_TSP_JSON,
    )

    parameter_id = "Depth"
    office_id = "SWT"
    location_id = "SWT.TEST.Text.Inst.1Hour.0.MockTest"

    data = timeseries.get_timeseries_profile(office_id, location_id, parameter_id)
    assert data.json == _TSP_JSON


def test_store_timeseries_profile(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/timeseries/profile?fail-if-exists=False")

    data = _TSP_JSON
    timeseries.store_timeseries_profile(data, False)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_all_timeseries_profile(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/timeseries/profile",
        json=_TSP_ARRAY_JSON,
    )

    data = timeseries.get_timeseries_profiles("*", "*", "*")

    assert data.json == _TSP_ARRAY_JSON


def test_delete_timeseries_profile(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}"
        "/timeseries/profile/Depth?location-id=TEST.Text.Inst.1Hour.0.MockTest&office=SWT",
        json=_TSP_JSON,
    )

    location_id = "TEST.Text.Inst.1Hour.0.MockTest"
    parameter_id = "Depth"
    office_id = "SWT"

    timeseries.delete_timeseries_profile(office_id, parameter_id, location_id)

    assert requests_mock.called
    assert requests_mock.call_count == 1
