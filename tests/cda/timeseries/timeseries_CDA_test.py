from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import cwms
import cwms.timeseries.timeseries as ts

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_group"
TEST_TSID = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Test"


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    location = {
        "name": TEST_LOCATION_ID,
        "latitude": 40.0,
        "longitude": -105.0,
        "elevation": 1000.0,
        "horizontal-datum": "NAD83",
        "vertical-datum": "NAVD88",
        "office-id": TEST_OFFICE,
        "location-type": "TESTING",
        "location-kind": "SITE",
        "public-name": "Test Location",
        "long-name": "A pytest-generated location",
        "timezone-name": "America/Chicago",
        "nation": "US",
    }
    cwms.store_location(location)

    ts_json = {
        "name": TEST_TSID,
        "units": "ft",
        "office-id": TEST_OFFICE,
        "values": [
            [datetime.now(timezone.utc).replace(microsecond=0).isoformat(), 1.23, 0]
        ],
    }
    cwms.store_timeseries(ts_json)
    yield
    cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE, cascade_delete=True)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for timeseries tests...")


def test_get_multi_timeseries_df():
    df = ts.get_multi_timeseries_df([TEST_TSID], TEST_OFFICE)
    assert df is not None, "Returned DataFrame is None"
    assert not df.empty, "Returned DataFrame is empty"
    assert any(
        TEST_TSID in str(col) for col in df.columns
    ), f"{TEST_TSID} not found in DataFrame columns"


def test_timeseries_df_to_json():
    dt = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    df = pd.DataFrame(
        {
            "date-time": [dt.isoformat()],
            "value": [42.0],
            "quality-code": [0],
        }
    )
    ts_id = "A.B.C.D.E"
    office = "TEST"
    units = "ft"
    json_out = ts.timeseries_df_to_json(df, ts_id, units, office)
    assert json_out["name"] == ts_id, "Incorrect timeseries id in output"
    assert json_out["office-id"] == office, "Incorrect office-id in output"
    assert json_out["units"] == units, "Incorrect units in output"
    assert json_out["values"] == [
        [dt.isoformat(), 42.0, 0]
    ], "Values do not match expected"


def test_store_multi_timeseries_df():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = pd.DataFrame(
        {
            "date-time": [now.isoformat()],
            "value": [7.89],
            "quality-code": [0],
            "ts_id": [TEST_TSID],
            "units": ["ft"],
        }
    )
    ts.store_multi_timeseries_df(df, TEST_OFFICE)


def test_store_timeseries():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ts_json = {
        "name": TEST_TSID,
        "office-id": TEST_OFFICE,
        "units": "ft",
        "values": [[now.isoformat(), 99.9, 0]],
    }
    ts.store_timeseries(ts_json)


def test_delete_timeseries():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    begin = now - timedelta(minutes=15)
    end = now + timedelta(minutes=15)
    ts.delete_timeseries(TEST_TSID, TEST_OFFICE, begin, end)
