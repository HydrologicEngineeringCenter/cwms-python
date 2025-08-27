from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import cwms
import cwms.timeseries.timeseries as ts

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_group"
TEST_TSID = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Test"
TEST_TSID_MULTI = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi"
TEST_TSID_STORE = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Store"
TEST_TSID_DELETE = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Delete"


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

    yield
    cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE, cascade_delete=True)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for timeseries tests...")


def test_store_timeseries():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    now_epoch_ms = int(now.timestamp() * 1000)
    iso_now = now.isoformat()
    ts_json = {
        "name": TEST_TSID_STORE,
        "office-id": TEST_OFFICE,
        "units": "ft",
        "values": [[now_epoch_ms, 99, 0]],
        "begin": iso_now,
        "end": iso_now,
        "version-date": iso_now,
        "time-zone": "UTC",
    }
    ts.store_timeseries(ts_json)
    data = ts.get_timeseries(TEST_TSID_STORE, TEST_OFFICE).json
    assert data["name"] == TEST_TSID_STORE
    assert data["office-id"] == TEST_OFFICE
    assert data["units"] == "ft"
    assert data["values"][0][1] == pytest.approx(99)


def test_get_timeseries():
    data = ts.get_timeseries(TEST_TSID_STORE, TEST_OFFICE).json
    assert data["name"] == TEST_TSID_STORE
    assert data["office-id"] == TEST_OFFICE
    assert data["units"] == "ft"
    assert data["values"][0][1] == pytest.approx(99)


def test_timeseries_df_to_json():
    dt = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    df = pd.DataFrame(
        {
            "date-time": [dt],
            "value": [42],
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
        [dt.isoformat(), 42, 0]
    ], "Values do not match expected"


def test_store_multi_timeseries_df():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ts_id_rev_test = TEST_TSID_MULTI.replace("Raw-Multi", "Raw-Rev-Test")
    df = pd.DataFrame(
        {
            "date-time": [now, now],
            "value": [7, 8],
            "quality-code": [0, 0],
            "ts_id": [TEST_TSID_MULTI, ts_id_rev_test],
            "units": ["ft", "ft"],
        }
    )
    ts.store_multi_timeseries_df(df, TEST_OFFICE)
    data1 = ts.get_timeseries(TEST_TSID_MULTI, TEST_OFFICE).json
    data2 = ts.get_timeseries(ts_id_rev_test, TEST_OFFICE).json
    assert data1["name"] == TEST_TSID_MULTI
    assert data1["office-id"] == TEST_OFFICE
    assert data1["units"] == "ft"
    assert data1["values"][0][1] == pytest.approx(7)
    assert data2["name"] == ts_id_rev_test
    assert data2["office-id"] == TEST_OFFICE
    assert data2["units"] == "ft"
    assert data2["values"][0][1] == pytest.approx(8)


def test_get_multi_timeseries_df():
    ts_id_rev_test = TEST_TSID_MULTI.replace("Raw-Multi", "Raw-Rev-Test")
    df = ts.get_multi_timeseries_df([TEST_TSID_MULTI, ts_id_rev_test], TEST_OFFICE)
    assert df is not None, "Returned DataFrame is None"
    assert not df.empty, "Returned DataFrame is empty"
    assert any(
        TEST_TSID_MULTI in str(col) for col in df.columns
    ), f"{TEST_TSID_MULTI} not found in DataFrame columns"
    assert any(
        ts_id_rev_test in str(col) for col in df.columns
    ), f"{ts_id_rev_test} not found in DataFrame columns"


def test_delete_timeseries():
    ts_id_rev_test = TEST_TSID_MULTI.replace("Raw-Multi", "Raw-Rev-Test")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    begin = now - timedelta(minutes=15)
    end = now + timedelta(minutes=15)
    ts.delete_timeseries(ts_id_rev_test, TEST_OFFICE, begin, end)
    result = ts.get_timeseries(ts_id_rev_test, TEST_OFFICE)
    assert result is None or result.json.get("values", []) == []
