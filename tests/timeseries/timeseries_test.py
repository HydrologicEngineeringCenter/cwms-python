#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime
from unittest.mock import patch

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


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


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


@pytest.fixture
def sample_dataframe():
    # Sample DataFrame that fits the expected format
    data = {
        "office-id": ["Office1", "Office2"],
        "ts-id": ["TS1", "TS2"],
        "alias": ["Alias1", "Alias2"],
        "ts-code": [1234, 5678],
        "attribute": [0, 0],
    }
    df = pd.DataFrame(data)
    return df


def test_timeseries_group_df_to_json(sample_dataframe):
    # Mock group_id, office_id, and category_id
    group_id = "SampleGroup"
    office_id = "SampleOffice"
    category_id = "SampleCategory"

    # Convert DataFrame to JSON
    json_output = timeseries.timeseries_group_df_to_json(
        data=sample_dataframe,
        group_id=group_id,
        office_id=office_id,
        category_id=category_id,
    )

    # Check that the JSON output has the correct structure
    assert json_output["office-id"] == office_id
    assert json_output["id"] == group_id
    assert json_output["time-series-category"]["id"] == category_id
    assert len(json_output["time-series"]) == len(sample_dataframe)
    assert (
        json_output["time-series"][0]["office-id"]
        == sample_dataframe.iloc[0]["office-id"]
    )
    assert json_output["time-series"][0]["id"] == sample_dataframe.iloc[0]["ts-id"]

    return json_output  # Returning JSON to be used in other tests if needed


def test_update_timeseries_groups(json_output):
    group_id = "SampleGroup"
    office_id = "SampleOffice"

    # Mock cwms.api.patch to test update_timeseries_groups
    with patch("cwms.api.patch") as mock_patch:
        timeseries.update_timeseries_groups(
            group_id=group_id,
            office_id=office_id,
            replace_assigned_ts=False,
            JSON=json_output,
        )

        # Assert that the patch method was called with the correct endpoint and parameters
        mock_patch.assert_called_once_with(
            endpoint=f"timeseries/group/{group_id}",
            params={
                "replace-assigned-ts": False,
                "office": office_id,
            },
            body=json_output,
        )


def test_timeseries_group_df_to_json_missing_column():
    # Create a DataFrame missing the 'ts-id' column
    data = pd.DataFrame(
        {"office-id": ["LRL", "SWT"], "some-other-column": ["value1", "value2"]}
    )

    group_id = "group1"
    office_id = "office1"
    category_id = "category1"

    # Expect a TypeError to be raised due to the missing 'ts-id' column
    with pytest.raises(
        TypeError,
        match="ts-id is a required column in data when posting as a dataframe",
    ):
        cwms.timeseries_group_df_to_json(data, group_id, office_id, category_id)
