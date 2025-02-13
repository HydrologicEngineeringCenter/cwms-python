#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pandas as pd
import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries as timeseries
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_VERS_TS_JSON = read_resource_file("versioned_num_ts.json")
_UNVERS_TS_JSON = read_resource_file("unversioned_num_ts.json")
_TS_GROUP = read_resource_file("time_series_group.json")
_EMPTY_TS_JSON = read_resource_file("empty_num_ts.json")
_TS_PAGE1 = read_resource_file("paging_num_ts_page1.json")
_TS_PAGE2 = read_resource_file("paging_num_ts_page2.json")
_TS_PAGE3 = read_resource_file("paging_num_ts_page3.json")
_TS_PAGE_ALL = read_resource_file("paging_num_ts_allpages.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_update_timeseries_groups(requests_mock):
    group_id = "USGS TS Data Acquisition"
    office_id = "CWMS"
    replace_assigned_ts = True
    data = _TS_GROUP

    requests_mock.patch(
        f"{_MOCK_ROOT}/timeseries/group/USGS%20TS%20Data%20Acquisition?replace-assigned-ts=True&office=CWMS",
        status_code=200,
    )

    timeseries.update_timeseries_groups(
        data=data,
        group_id=group_id,
        office_id=office_id,
        replace_assigned_ts=replace_assigned_ts,
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_timeseries_group_df_to_json_valid_data():
    data = pd.DataFrame(
        {
            "office-id": ["office123", "office456"],
            "timeseries-id": ["ts1", "ts2"],
            "alias-id": [None, "alias2"],
            "attribute": [0, 10],
            "ts-code": ["code1", None],
        }
    )

    # Clean DataFrame by removing NaN from required columns and fix optional ones
    required_columns = ["office-id", "timeseries-id"]
    data = data.dropna(subset=required_columns)
    optional_columns = ["alias-id", "ts-code"]
    for col in optional_columns:
        if col in data.columns:
            data[col] = data[col].where(pd.notnull(data[col]), None)

    expected_json = {
        "office-id": "office123",
        "id": "group123",
        "time-series-category": {
            "office-id": "office123",
            "id": "cat123",
        },
        "assigned-time-series": [
            {
                "office-id": "office123",
                "timeseries-id": "ts1",
                "alias-id": None,
                "attribute": 0,
                "ts-code": "code1",
            },
            {
                "office-id": "office456",
                "timeseries-id": "ts2",
                "alias-id": "alias2",
                "attribute": 10,
            },
        ],
    }

    result = timeseries.timeseries_group_df_to_json(
        data, "group123", "office123", "cat123"
    )
    assert result == expected_json


def test_timeseries_df_to_json():
    test_json = {
        "name": "TestLoc.Stage.Inst.1Hour.0.Testing",
        "office-id": "MVP",
        "units": "ft",
        "values": [
            ["2024-08-18T04:00:00+00:00", 1, 0],
            ["2024-08-18T05:00:00+00:00", 1, 0],
            ["2024-08-18T06:00:00+00:00", 1, 0],
            ["2024-08-18T07:00:00+00:00", 1, 0],
            ["2024-08-18T08:00:00+00:00", 1, 0],
        ],
        "version-date": None,
    }

    data = pd.DataFrame(
        {
            "date-time": pd.date_range(
                start="2024-08-18", end="2024-08-18 04:00", freq="1h"
            ),
            "value": 1,
        }
    )
    data["date-time"] = data["date-time"].dt.tz_localize("US/Eastern")
    office_id = "MVP"
    ts_id = "TestLoc.Stage.Inst.1Hour.0.Testing"
    data2 = data.copy()
    ts_json = cwms.timeseries_df_to_json(
        data=data, ts_id=ts_id, office_id=office_id, units="ft"
    )

    assert ts_json == test_json
    assert all(data == data2)


def test_get_timeseries_unversioned_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=500000",
        json=_UNVERS_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))

    data = timeseries.get_timeseries(
        ts_id=timeseries_id, office_id=office_id, begin=begin, end=end
    )
    assert data.json == _UNVERS_TS_JSON
    assert type(data.df) is pd.DataFrame
    assert "date-time" in data.df.columns
    assert data.df.shape == (4, 3)


def test_get_empty_ts_df(requests_mock):

    timeseries_id = "KEYS.Elev.Inst.1Hour.0.Ccp-Rev"
    office_id = "SWT"
    requests_mock.get(
        f"{_MOCK_ROOT}"
        f"/timeseries?office={office_id}&"
        f"name={timeseries_id}&"
        "unit=EN&"
        "begin=2024-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2024-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=500000&"
        "trim=true",
        json=_EMPTY_TS_JSON,
    )

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2024, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2024, 5, 1, 17, 0, 0))

    data = timeseries.get_timeseries(
        ts_id=timeseries_id, office_id=office_id, begin=begin, end=end
    )
    assert data.json == _EMPTY_TS_JSON
    assert type(data.df) is pd.DataFrame
    assert data.df.shape == (0, 3)


def test_get_timeseries_paging(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=NWDM&"
        "name=Test.Stage.Inst.15Minutes.0.TEST_PAGING&"
        "unit=EN&"
        "begin=2024-10-03T11%3A00%3A00%2B00%3A00&"
        "end=2024-10-04T11%3A00%3A00%2B00%3A00&"
        "page-size=10&"
        "trim=true",
        json=_TS_PAGE1,
    )

    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=NWDM&"
        "name=Test.Stage.Inst.15Minutes.0.TEST_PAGING&"
        "unit=EN&"
        "begin=2024-10-03T11%3A00%3A00%2B00%3A00&"
        "end=2024-10-04T11%3A00%3A00%2B00%3A00&"
        "page-size=10&"
        "page=MTcyNzk2MzEwMDAwMHx8OTZ8fDEw&"
        "trim=true",
        json=_TS_PAGE2,
    )

    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=NWDM&"
        "name=Test.Stage.Inst.15Minutes.0.TEST_PAGING&"
        "unit=EN&"
        "begin=2024-10-03T11%3A00%3A00%2B00%3A00&"
        "end=2024-10-04T11%3A00%3A00%2B00%3A00&"
        "page-size=10&"
        "page=MTcyNzk3MjEwMDAwMHx8OTZ8fDEw&"
        "trim=true",
        json=_TS_PAGE3,
    )
    timeseries_id = "Test.Stage.Inst.15Minutes.0.TEST_PAGING"
    office_id = "NWDM"

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2024, 10, 3, 11, 0, 0))
    end = timezone.localize(datetime(2024, 10, 4, 11, 0, 0))
    data = timeseries.get_timeseries(
        ts_id=timeseries_id, office_id=office_id, begin=begin, end=end, page_size=10
    )
    assert data.json == _TS_PAGE_ALL
    assert type(data.df) is pd.DataFrame
    assert "date-time" in data.df.columns
    assert data.df.shape == (30, 3)


def test_get_timeseries_group_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries/group/USGS%20TS%20Data%20Acquisition?office=CWMS&"
        "category-id=Data%20Acquisition",
        json=_TS_GROUP,
    )

    group_id = "USGS TS Data Acquisition"
    category_id = "Data Acquisition"
    office_id = "CWMS"

    data = timeseries.get_timeseries_group(
        group_id=group_id, category_id=category_id, office_id=office_id
    )

    assert data.json == _TS_GROUP
    assert type(data.df) is pd.DataFrame
    assert "timeseries-id" in data.df.columns
    assert data.df.shape == (11, 5)
    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "LRL",
        "Buckhorn-Lake.Stage.Inst.5Minutes.0.USGS-raw",
        6245109,
        "59905",
        0,
    ]


def test_get_multi_timeseries_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=500000&"
        "version-date=2021-06-20T08%3A00%3A00%2B00%3A00",
        json=_VERS_TS_JSON,
    )

    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=500000",
        json=_UNVERS_TS_JSON,
    )

    ts_ids = [
        "TEST.Text.Inst.1Hour.0.MockTest",
        "TEST.Text.Inst.1Hour.0.MockTest:2021-06-20 08:00:00-00:00",
    ]
    office_id = "SWT"

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))
    data = cwms.get_multi_timeseries_df(
        ts_ids=ts_ids, office_id=office_id, begin=begin, end=end, melted=False
    )

    assert type(data) is pd.DataFrame
    assert data.shape == (4, 2)


def test_create_timeseries_unversioned_default(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/timeseries?"
        f"create-as-lrts=False&"
        f"override-protection=False"
    )

    data = _UNVERS_TS_JSON
    timeseries.store_timeseries(data=data)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_get_timeseries_versioned_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=500000&"
        "version-date=2021-06-20T08%3A00%3A00%2B00%3A00",
        json=_VERS_TS_JSON,
    )

    timeseries_id = "TEST.Text.Inst.1Hour.0.MockTest"
    office_id = "SWT"

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))
    version_date = timezone.localize(datetime(2021, 6, 20, 8, 0, 0))

    data = timeseries.get_timeseries(
        ts_id=timeseries_id,
        office_id=office_id,
        begin=begin,
        end=end,
        version_date=version_date,
    )
    assert data.json == _VERS_TS_JSON
    assert type(data.df) is pd.DataFrame
    assert "date-time" in data.df.columns
    assert data.df.shape == (4, 3)


def test_create_timeseries_versioned_default(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/timeseries?"
        f"create-as-lrts=False&"
        f"override-protection=False"
    )

    data = _VERS_TS_JSON
    timeseries.store_timeseries(data=data)

    assert requests_mock.called
    assert requests_mock.call_count == 1
