#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json
import re
from datetime import datetime, timedelta
from datetime import timezone as datetime_timezone

import pandas as pd
import pytest
import pytz

import cwms.api
import cwms.timeseries.timeseries as timeseries
import cwms.timeseries.timeseries_group as timeseries_group
from cwms.cwms_types import Data
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
        f"{_MOCK_ROOT}/v2/timeseries/group/CWMS/USGS%20TS%20Data%20Acquisition?ignore-nulls=True",
        status_code=200,
    )

    timeseries_group.update_timeseries_groups(
        data=data,
        group_id=group_id,
        office_id=office_id,
        replace_assigned_ts=replace_assigned_ts,
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
    assert requests_mock.last_request.headers["Accept"] == "*/*"
    assert requests_mock.last_request.headers["Content-Type"] == "application/json"
    patch_data = json.loads(requests_mock.last_request.text)
    assert patch_data["membership"]["assign"] == _TS_GROUP["assigned-time-series"]
    assert "unassign" not in patch_data["membership"]


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

    result = timeseries_group.timeseries_group_df_to_json(
        data=data,
        group_id="group123",
        group_office_id="office123",
        category_office_id="office123",
        category_id="cat123",
    )
    assert result == expected_json


def test_timeseries_group_patch_df_to_json():
    assign_data = pd.DataFrame(
        {
            "office-id": ["CWMS", "MVP"],
            "timeseries-id": [
                "River.Stage.Inst.1Hour.0.Test",
                "Lake.Stage.Inst.1Hour.0.Test",
            ],
            "alias-id": ["river-stage", None],
            "ref-ts-id": [None, "Lake.Stage.Inst.1Hour.0.Reference"],
            "attribute": [2, 0],
        }
    )
    unassign_data = pd.DataFrame(
        {
            "office-id": ["CWMS"],
            "timeseries-id": ["Old.Stage.Inst.1Hour.0.Test"],
        }
    )

    result = timeseries_group.timeseries_group_patch_df_to_json(
        group_id="group123",
        group_office_id="CWMS",
        category_office_id="CWMS",
        category_id="category123",
        assign_data=assign_data,
        unassign_data=unassign_data,
    )

    assert result == {
        "office-id": "CWMS",
        "id": "group123",
        "time-series-category": {"office-id": "CWMS", "id": "category123"},
        "membership": {
            "assign": [
                {
                    "office-id": "CWMS",
                    "timeseries-id": "River.Stage.Inst.1Hour.0.Test",
                    "attribute": 2,
                    "alias-id": "river-stage",
                },
                {
                    "office-id": "MVP",
                    "timeseries-id": "Lake.Stage.Inst.1Hour.0.Test",
                    "attribute": 0,
                    "ref-ts-id": "Lake.Stage.Inst.1Hour.0.Reference",
                },
            ],
            "unassign": [{"office-id": "CWMS", "name": "Old.Stage.Inst.1Hour.0.Test"}],
        },
    }


@pytest.mark.parametrize(
    ("assign_data", "unassign_data", "membership_key"),
    [
        (
            pd.DataFrame({"office-id": ["CWMS"], "timeseries-id": ["Test"]}),
            None,
            "assign",
        ),
        (
            None,
            pd.DataFrame({"office-id": ["CWMS"], "timeseries-id": ["Test"]}),
            "unassign",
        ),
    ],
)
def test_timeseries_group_patch_df_to_json_supports_either_dataframe(
    assign_data, unassign_data, membership_key
):
    result = timeseries_group.timeseries_group_patch_df_to_json(
        group_id="group123",
        group_office_id="CWMS",
        category_office_id="CWMS",
        category_id="category123",
        assign_data=assign_data,
        unassign_data=unassign_data,
    )

    assert list(result["membership"]) == [membership_key]


def test_timeseries_group_patch_df_to_json_requires_at_least_one_dataframe():
    with pytest.raises(ValueError, match="Provide data"):
        timeseries_group.timeseries_group_patch_df_to_json(
            group_id="group123",
            group_office_id="CWMS",
            category_office_id="CWMS",
            category_id="category123",
        )


def test_timeseries_group_patch_df_to_json_validates_required_columns():
    with pytest.raises(TypeError, match="assign_data is missing required columns"):
        timeseries_group.timeseries_group_patch_df_to_json(
            group_id="group123",
            group_office_id="CWMS",
            category_office_id="CWMS",
            category_id="category123",
            assign_data=pd.DataFrame({"office-id": ["CWMS"]}),
        )


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


def test_get_timeseries_unversioned_default(requests_mock, monkeypatch):
    monkeypatch.setattr(
        timeseries,
        "get_ts_extents",
        lambda **kwargs: (
            datetime(2008, 1, 1, tzinfo=datetime_timezone.utc),
            None,
            None,
        ),
    )
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=-1",
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


def test_get_empty_ts_df(requests_mock, monkeypatch):
    monkeypatch.setattr(
        timeseries,
        "get_ts_extents",
        lambda **kwargs: (
            datetime(2008, 1, 1, tzinfo=datetime_timezone.utc),
            None,
            None,
        ),
    )
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.EMPTYTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=-1&"
        "trim=true",
        json=_EMPTY_TS_JSON,
    )
    timeseries_id = "TEST.Text.Inst.1Hour.0.EMPTYTest"
    office_id = "SWT"

    # explicitly format begin and end dates with default timezone as an example
    timezone = pytz.timezone("UTC")
    begin = timezone.localize(datetime(2008, 5, 1, 15, 0, 0))
    end = timezone.localize(datetime(2008, 5, 1, 17, 0, 0))

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


def test_call_with_retry_does_not_retry_404():
    class ResponseStub:
        url = "https://mockwebserver.cwms.gov/timeseries"
        status_code = 404
        reason = "Not Found"
        content = b""

    call_count = 0

    def failing_call():
        nonlocal call_count
        call_count += 1
        raise cwms.api.ApiError(ResponseStub())

    with pytest.raises(cwms.api.ApiError) as exc_info:
        timeseries._call_with_retry(failing_call)

    assert exc_info.value.response.status_code == 404
    assert call_count == 1


@pytest.mark.parametrize(
    ("ts_id", "expected_days"),
    [
        ("Test.Stage.Inst.2Minutes.0.Test", 49),
        ("Test.Stage.Inst.5Minutes.0.Test", 122),
        ("Test.Stage.Inst.15Minutes.0.Test", 365),
        ("Test.Stage.Inst.~15Minutes.0.Test", 365),
        ("Test.Stage.Inst.1Hour.0.Test", 365),
        ("Test.Stage.Inst.~1Hour.0.Test", 365),
        ("Test.Stage.Inst.6Hours.0.Test", 1460),
        ("Test.Stage.Inst.~6Hours.0.Test", 1460),
        ("Test.Stage.Inst.1Day.0.Test", 2920),
        ("Test.Stage.Inst.~1Day.0.Test", 2920),
        ("Test.Stage.Inst.1Week.0.Test", 2920),
        ("Test.Stage.Inst.1Month.0.Test", 2920),
        ("Test.Stage.Inst.1Year.0.Test", 2920),
    ],
)
def test_get_timeseries_chunk_size(ts_id, expected_days):
    assert timeseries.get_timeseries_chunk_size(ts_id) == timedelta(days=expected_days)


@pytest.mark.parametrize(
    "ts_id",
    [
        "Invalid",
        "Test.Stage.Inst.0.0.Test",
        "Test.Stage.Inst.1Second.0.Test",
        "Test.Stage.Inst.1Minute.0.Test",
        "Test.Stage.Inst.Irregular.0.Test",
    ],
)
def test_get_timeseries_chunk_size_falls_back_for_unknown_intervals(ts_id):
    assert timeseries.get_timeseries_chunk_size(ts_id) == timedelta(days=365)


def test_chunk_timeseries_time_range_rejects_nonpositive_size():
    now = datetime.now(tz=pytz.UTC)

    with pytest.raises(ValueError, match="chunk_size must be greater than zero"):
        timeseries.chunk_timeseries_time_range(
            now, now + timedelta(days=1), timedelta()
        )


def test_combine_timeseries_results_trims_only_outer_missing_values():
    first_json = _UNVERS_TS_JSON.copy()
    first_json["values"] = [[0, None, 0], [1000, 1.0, 0], [2000, None, 0]]
    second_json = _UNVERS_TS_JSON.copy()
    second_json["values"] = [
        [2000, None, 0],
        [3000, None, 0],
        [4000, 2.0, 0],
        [5000, None, 0],
    ]

    combined = timeseries.combine_timeseries_results(
        [Data(first_json, selector="values"), Data(second_json, selector="values")],
        trim=True,
    )

    assert combined.json["values"] == [
        [1000, 1.0, 0],
        [2000, None, 0],
        [3000, None, 0],
        [4000, 2.0, 0],
    ]
    assert combined.json["total"] == 4


@pytest.mark.parametrize(
    ("ts_id", "expected_days"),
    [
        ("Test.Stage.Inst.~15Minutes.0.DefaultChunk", 365),
        ("Test.Stage.Inst.1Day.0.DefaultChunk", 2920),
    ],
)
def test_get_timeseries_uses_interval_chunk_size_by_default(
    monkeypatch, ts_id, expected_days
):
    begin = datetime(2025, 1, 1, tzinfo=pytz.UTC)
    end = begin + timedelta(days=expected_days * 2 + 1)
    expected = object()
    captured = {}

    def fetch_chunks(chunks, params, selector, endpoint, max_workers):
        captured["chunks"] = chunks
        captured["max_workers"] = max_workers
        captured["trim"] = params["trim"]
        return [object()]

    monkeypatch.setattr(timeseries, "fetch_timeseries_chunks", fetch_chunks)
    monkeypatch.setattr(
        timeseries,
        "combine_timeseries_results",
        lambda results, trim=False: expected,
    )

    result = timeseries.get_timeseries(
        ts_id=ts_id,
        office_id="MVP",
        begin=begin,
        end=end,
    )

    assert result is expected
    assert captured["chunks"] == timeseries.chunk_timeseries_time_range(
        begin, end, timedelta(days=expected_days)
    )
    assert captured["max_workers"] == 3
    assert captured["trim"] is False


def test_get_timeseries_max_days_per_chunk_overrides_interval(monkeypatch):
    begin = datetime(2025, 1, 1, tzinfo=pytz.UTC)
    end = begin + timedelta(days=31)
    captured = {}

    def fetch_chunks(chunks, params, selector, endpoint, max_workers):
        captured["chunks"] = chunks
        return [object()]

    monkeypatch.setattr(timeseries, "fetch_timeseries_chunks", fetch_chunks)
    monkeypatch.setattr(
        timeseries,
        "combine_timeseries_results",
        lambda results, trim=False: object(),
    )

    timeseries.get_timeseries(
        ts_id="Test.Stage.Inst.1Day.0.OverrideChunk",
        office_id="MVP",
        begin=begin,
        end=end,
        max_days_per_chunk=10,
    )

    assert captured["chunks"] == timeseries.chunk_timeseries_time_range(
        begin, end, timedelta(days=10)
    )


def test_get_timeseries_rejects_nonpositive_max_days_per_chunk():
    with pytest.raises(
        ValueError, match="max_days_per_chunk must be greater than zero"
    ):
        timeseries.get_timeseries(
            ts_id="Test.Stage.Inst.15Minutes.0.InvalidChunk",
            office_id="MVP",
            max_days_per_chunk=0,
        )


def test_get_timeseries_group_default(requests_mock):
    group_id = "USGS TS Data Acquisition"
    category_id = "Data Acquisition"
    office_id = "LRL"
    category_office_id = "CMWS"
    group_office_id = "CWMS"

    requests_mock.get(
        f"{_MOCK_ROOT}/v2/timeseries/group/{group_office_id}/{group_id}?"
        f"office={office_id}&"
        f"category-id={category_id}&"
        f"category-office-id={category_office_id}",
        json=_TS_GROUP,
    )

    """
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/v2/timeseries/group/CWMS/USGS%20TS%20Data%20Acquisition?office=CWMS&"
        "category-id=Data%20Acquisition",
        json=_TS_GROUP,
    )
    """

    data = timeseries_group.get_timeseries_group(
        group_id=group_id,
        category_id=category_id,
        office_id=office_id,
        category_office_id=category_office_id,
        group_office_id=group_office_id,
    )

    assert data.json == _TS_GROUP
    assert type(data.df) is pd.DataFrame
    assert "timeseries-id" in data.df.columns
    assert data.df.shape == (5, 5)
    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "LRL",
        "Buckhorn-Lake.Stage.Inst.5Minutes.0.USGS-raw",
        6245109,
        "59905",
        0,
    ]
    assert requests_mock.last_request.headers["Accept"] == "application/json"


def test_timeseries_group_requests_use_v2(requests_mock):
    group_id = "USGS TS Data Acquisition"
    group_endpoint = re.compile(
        rf"^{re.escape(_MOCK_ROOT)}/v2/timeseries/group/CWMS(?:/[^?]*)?(?:\?.*)?$"
    )

    requests_mock.get(group_endpoint, json=_TS_GROUP)
    requests_mock.post(f"{_MOCK_ROOT}/v2/timeseries/group/CWMS", status_code=200)
    requests_mock.patch(group_endpoint, status_code=200)
    requests_mock.delete(group_endpoint, status_code=200)

    timeseries_group.get_timeseries_group(
        group_id=group_id, group_office_id="CWMS", office_id="CWMS"
    )
    timeseries_group.get_timeseries_groups(office_id="CWMS", group_office_id="CWMS")
    timeseries_group.store_timeseries_groups(data=_TS_GROUP)
    timeseries_group.update_timeseries_groups(
        data=_TS_GROUP, group_id=group_id, office_id="CWMS"
    )
    timeseries_group.delete_timeseries_group(
        group_id=group_id, category_id="Data Acquisition", office_id="CWMS"
    )

    assert requests_mock.call_count == 5
    assert all(
        f"{_MOCK_ROOT}/v2/timeseries/group/" in request.url
        for request in requests_mock.request_history
    )
    assert all(
        "version=2" not in request.headers.get("Accept", "")
        and "version=2" not in request.headers.get("Content-Type", "")
        for request in requests_mock.request_history
    )
    patch_data = json.loads(requests_mock.request_history[3].text)
    assert patch_data["membership"]["assign"] == _TS_GROUP["assigned-time-series"]
    assert "assigned-time-series" not in patch_data


def test_get_multi_timeseries_default(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=-1&"
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
        "page-size=-1",
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


def test_get_timeseries_versioned_default(requests_mock, monkeypatch):
    monkeypatch.setattr(
        timeseries,
        "get_ts_extents",
        lambda **kwargs: (
            datetime(2008, 1, 1, tzinfo=datetime_timezone.utc),
            None,
            None,
        ),
    )
    requests_mock.get(
        f"{_MOCK_ROOT}"
        "/timeseries?office=SWT&"
        "name=TEST.Text.Inst.1Hour.0.MockTest&"
        "unit=EN&"
        "begin=2008-05-01T15%3A00%3A00%2B00%3A00&"
        "end=2008-05-01T17%3A00%3A00%2B00%3A00&"
        "page-size=-1&"
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
