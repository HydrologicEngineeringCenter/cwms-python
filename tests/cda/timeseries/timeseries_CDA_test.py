from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import cwms
import cwms.timeseries.timeseries as ts

TEST_OFFICE = "SPK"
TEST_LOCATION_ID = "pytest_loc"
TEST_TS_ID = f"{TEST_LOCATION_ID}.Flow.Inst.1Hour.0.Raw"
TEST_UNIT = "cfs"
NOW = datetime.now(timezone.utc)
START_TIME = NOW - timedelta(hours=2)
END_TIME = NOW
TEST_DATA = pd.DataFrame(
    {
        "date-time": [
            START_TIME.isoformat(),
            (START_TIME + timedelta(hours=1)).isoformat(),
        ],
        "value": [65.0, 66.5],
        "quality-code": [0, 0],
    }
)

SECOND_TS_ID = f"{TEST_LOCATION_ID}.Flow.Inst.1Day.0.Raw"
SECOND_TEST_DATA = pd.DataFrame(
    {
        "date-time": [
            START_TIME.isoformat(),
            (START_TIME + timedelta(days=1)).isoformat(),
        ],
        "value": [72.1, 74.0],
        "quality-code": [0, 0],
    }
)

# Setup and teardown fixture for test location


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    TEST_LATITUDE = 45.1704758
    TEST_LONGITUDE = -92.8411439

    location_data = {
        "name": TEST_LOCATION_ID,
        "office-id": TEST_OFFICE,
        "latitude": TEST_LATITUDE,
        "longitude": TEST_LONGITUDE,
        "elevation": 250.0,
        "horizontal-datum": "NAD83",
        "vertical-datum": "NAVD88",
        "location-type": "TESTING",
        "public-name": "Test Location",
        "long-name": "A pytest-generated location",
        "timezone-name": "America/Los_Angeles",
        "location-kind": "SITE",
        "nation": "US",
    }
    # fmt: on

    # Store location before tests
    cwms.store_location(location_data)

    yield

    # Delete location after tests
    cwms.delete_location(
        location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE, cascade_delete=True
    )


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for timeseries operations test...")


def test_store_timeseries():
    json_data = ts.timeseries_df_to_json(
        data=TEST_DATA, ts_id=TEST_TS_ID, units=TEST_UNIT, office_id=TEST_OFFICE
    )
    ts.store_timeseries(json_data)
    df = ts.get_timeseries(TEST_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME).df
    assert not df.empty


def test_update_timeseries_value():
    updated_data = TEST_DATA.copy()
    updated_data.loc[1, "value"] = 67.2
    json_data = ts.timeseries_df_to_json(
        data=updated_data, ts_id=TEST_TS_ID, units=TEST_UNIT, office_id=TEST_OFFICE
    )
    ts.store_timeseries(json_data)
    df = ts.get_timeseries(TEST_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME).df
    assert 67.2 in df["value"].values


def test_delete_timeseries_data():
    json_data = ts.timeseries_df_to_json(
        data=TEST_DATA, ts_id=TEST_TS_ID, units=TEST_UNIT, office_id=TEST_OFFICE
    )
    ts.store_timeseries(json_data)
    ts.delete_timeseries(TEST_TS_ID, TEST_OFFICE, START_TIME, END_TIME)
    df = ts.get_timeseries(TEST_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME).df
    assert df.empty


def test_get_timeseries():
    json_data = ts.timeseries_df_to_json(
        data=TEST_DATA, ts_id=TEST_TS_ID, units=TEST_UNIT, office_id=TEST_OFFICE
    )
    ts.store_timeseries(json_data)
    df = ts.get_timeseries(TEST_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME).df
    assert df.shape[0] == 2
    assert 65.0 in df["value"].values and 66.5 in df["value"].values


def test_store_multi_timeseries_df():
    multi_df = pd.concat(
        [
            TEST_DATA.assign(ts_id=TEST_TS_ID, units=TEST_UNIT),
            SECOND_TEST_DATA.assign(ts_id=SECOND_TS_ID, units=TEST_UNIT),
        ],
        ignore_index=True,
    )
    ts.store_multi_timeseries_df(multi_df, office_id=TEST_OFFICE)

    df1 = ts.get_timeseries(TEST_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME).df
    df2 = ts.get_timeseries(
        SECOND_TS_ID, TEST_OFFICE, begin=START_TIME, end=END_TIME
    ).df

    assert not df1.empty
    assert not df2.empty


def test_get_multi_timeseries_df():
    combined_df = ts.get_multi_timeseries_df(
        [TEST_TS_ID, SECOND_TS_ID],
        office_id=TEST_OFFICE,
        begin=START_TIME,
        end=END_TIME,
        melted=True,
    )
    ts_ids = combined_df["ts_id"].unique().tolist()
    assert TEST_TS_ID in ts_ids
    assert SECOND_TS_ID in ts_ids
    assert combined_df.shape[0] >= 4
