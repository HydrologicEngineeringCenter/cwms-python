#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.levels.location_levels as location_levels
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_LOC_LEVELS_JSON = read_resource_file("location_levels.json")
_LOC_LEVEL_JSON = read_resource_file("location_level.json")
_LOC_LEVEL_TS_JSON = read_resource_file("level_timeseries.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_retrieve_loc_levels_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/levels?level-id-mask=%2A",
        json=_LOC_LEVELS_JSON,
    )
    levels = location_levels.get_location_levels()
    assert levels.json == _LOC_LEVELS_JSON


def test_get_loc_levels(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/levels?office=SWT&level-id-mask=AARK.Elev.Inst.0.Bottom+of+Inlet&"
        "unit=m&datum=NAV88&begin=2020-02-14T10%3A30%3A00-08%3A00&end=2020-03-30T10%3A30%3A00-07%3A00&"
        "page=MHx8bnVsbHx8MTAw&page-size=100",
        json=_LOC_LEVELS_JSON,
    )

    level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
    office_id = "SWT"
    unit = "m"
    datum = "NAV88"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
    end = timezone.localize(datetime(2020, 3, 30, 10, 30, 0))
    page = "MHx8bnVsbHx8MTAw"
    page_size = 100
    levels = location_levels.get_location_levels(
        level_id, office_id, unit, datum, begin, end, page, page_size
    )
    assert levels.json == _LOC_LEVELS_JSON


def test_get_loc_level(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet?office=SWT&"
        "unit=m&effective-date=2020-02-14T10%3A30%3A00-08%3A00",
        json=_LOC_LEVEL_JSON,
    )
    level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
    office_id = "SWT"
    unit = "m"
    timezone = pytz.timezone("US/Pacific")
    effective_date = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
    levels = location_levels.get_location_level(
        level_id, office_id, effective_date, unit
    )
    assert levels.json == _LOC_LEVEL_JSON


def test_store_loc_level_json(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/levels")
    data = _LOC_LEVEL_JSON
    location_levels.store_location_level(data)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_loc_level(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet?office=SWT&"
        "effective-date=2020-02-14T10%3A30%3A00-08%3A00&cascade-delete=True",
        json=_LOC_LEVEL_JSON,
    )
    level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
    office_id = "SWT"
    timezone = pytz.timezone("US/Pacific")
    effective_date = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
    location_levels.delete_location_level(level_id, office_id, effective_date, True)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_loc_level_ts(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/levels/AARK.Elev.Inst.0.Bottom%20of%20Inlet/timeseries?office=SWT&unit=m&"
        "begin=2020-02-14T10%3A30%3A00-08%3A00&end=2020-03-14T10%3A30%3A00-07%3A00&interval=1Day",
        json=_LOC_LEVEL_TS_JSON,
    )
    level_id = "AARK.Elev.Inst.0.Bottom of Inlet"
    office_id = "SWT"
    interval = "1Day"
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2020, 2, 14, 10, 30, 0))
    end = timezone.localize(datetime(2020, 3, 14, 10, 30, 0))
    levels = location_levels.get_level_as_timeseries(
        level_id, office_id, "m", begin, end, interval
    )
    assert levels.json == _LOC_LEVEL_TS_JSON
