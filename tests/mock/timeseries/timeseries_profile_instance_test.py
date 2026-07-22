#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import urllib.parse
from datetime import datetime
from pathlib import Path

import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries_profile_instance as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TSP_INST_JSON = read_resource_file("timeseries_profile_instance.json")
_TSP_INST_ARRAY_JSON = read_resource_file("timeseries_profile_instances.json")
current_path = Path(__file__).resolve().parent.parent.parent
resource_path = current_path / "resources" / "timeseries_profile_data.txt"
with open(resource_path, "r") as file:
    _TSP_PROFILE_DATA = file.read().strip("\n")

tz = pytz.timezone("UTC")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_timeseries_profile_instance(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/profile-instance/SWAN/Temp-Water/Raw?office=SWT&"
        "unit=C",
        json=_TSP_INST_JSON,
    )

    location_id = "SWAN"
    parameter_id = "Temp-Water"
    version = "Raw"
    unit = "C"
    office_id = "SWT"
    version_date = tz.localize(datetime(2014, 8, 16, 4, 55, 0))
    start = tz.localize(datetime(2015, 3, 3, 6, 45, 0))
    end = tz.localize(datetime(2015, 3, 3, 7, 15, 0))

    data = timeseries.get_timeseries_profile_instance(
        office_id,
        location_id,
        parameter_id,
        version,
        unit,
        version_date,
        start,
        end,
    )

    assert data.json == _TSP_INST_JSON


def test_store_timeseries_profile_instance(requests_mock):
    data = urllib.parse.quote_plus(_TSP_PROFILE_DATA)
    requests_mock.post(
        f"{_MOCK_ROOT}/timeseries/profile-instance"
        f"?profile-data={data}&"
        "version=Raw&override-protection=False"
        "&version-date=2020-01-01T13%3A30%3A00%2B00%3A00"
    )

    version = "Raw"
    version_date = tz.localize(datetime(2020, 1, 1, 13, 30, 0))

    timeseries.store_timeseries_profile_instance(
        _TSP_PROFILE_DATA, version, version_date, None, False
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_timeseries_profile_instance(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}" "/timeseries/profile-instance/SWAN/Length/Raw?office=SWT",
        json=_TSP_INST_JSON,
    )

    location_id = "SWAN"
    office_id = "SWT"
    parameter_id = "Length"
    version = "Raw"
    version_date = tz.localize(datetime(2010, 6, 4, 12, 0, 0))
    date = tz.localize(datetime(2010, 6, 4, 14, 0, 0))

    timeseries.delete_timeseries_profile_instance(
        office_id, location_id, parameter_id, version, version_date, date
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_all_timeseries_profile_instance(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/timeseries/profile-instance",
        json=_TSP_INST_ARRAY_JSON,
    )

    data = timeseries.get_timeseries_profile_instances("*", "*", "*", "*")

    assert data.json == _TSP_INST_ARRAY_JSON
