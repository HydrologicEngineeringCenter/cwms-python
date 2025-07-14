"""
import pytest
import pandas as pd
import cwms.api
import cwms.timeseries.timeseries as timeseries
import cwms.locations.physical_locations as locations
from datetime import datetime, timedelta

#In progress

API_ROOT = "http://localhost:8081/cwms-data"
API_KEY = "1234567890abcdef1234567890abcdef"

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest-ts-loc"
TEST_TS_ID = "pytest-ts"
TEST_UNIT = "cfs"
TEST_INTERVAL = "1Hour"
TEST_START = datetime.utcnow() - timedelta(hours=2)
TEST_END = datetime.utcnow()


@pytest.fixture(scope="module", autouse=True)
def init_timeseries_session():
    cwms.api.init_session(api_root=API_ROOT, api_key=API_KEY)


def test_store_timeseries(requests_mock):
    locations_url = f"{API_ROOT}/locations/{TEST_OFFICE}/{TEST_LOCATION_ID}"
    timeseries_url = f"{API_ROOT}/timeseries/{TEST_OFFICE}/{TEST_TS_ID}"

    locations.store_location({
        "name": TEST_LOCATION_ID,
        "office-id": TEST_OFFICE,
        "latitude": 45.0,
        "longitude": -93.0,
        "elevation": 200.0,
        "horizontal-datum": "NAD83",
        "vertical-datum": "NAVD88",
        "location-type": "TESTING",
        "public-name": "TS Pytest Location",
        "long-name": "Timeseries Test Location"
    })

    now = datetime.utcnow()
    data = pd.DataFrame({
        "date-time": [now - timedelta(hours=1), now],
        "value": [10.0, 15.0]
    })

    timeseries.store_time_series({
        "name": TEST_TS_ID,
        "office-id": TEST_OFFICE,
        "location-id": TEST_LOCATION_ID,
        "units": TEST_UNIT,
        "time-series-type": "INST-VAL",
        "interval": TEST_INTERVAL,
        "start-time": TEST_START.isoformat(),
        "end-time": TEST_END.isoformat(),
        "values": data
    })

    df = timeseries.get_time_series(
        name=TEST_TS_ID,
        office_id=TEST_OFFICE,
        start_time=TEST_START,
        end_time=TEST_END
    ).df

    assert not df.empty
    assert df["value"].iloc[-1] == 15.0

def test_get_timeseries(requests_mock):

    df = timeseries.get_time_series(
        name=TEST_TS_ID,
        office_id=TEST_OFFICE,
        start_time=TEST_START,
        end_time=TEST_END
    ).df

    assert not df.empty
    assert df["value"].iloc[-1] == 15.0

def test_update_timeseries(requests_mock):
    timeseries_url = f"{API_ROOT}/timeseries/{TEST_OFFICE}/{TEST_TS_ID}"

    requests_mock.get(timeseries_url, text=read_resource_file("timeseries_get_response.json"))
    requests_mock.put(timeseries_url, text=read_resource_file("timeseries_put_response.json"))
    requests_mock.get(timeseries_url, text=read_resource_file("timeseries_get_response_updated.json"))

    df = timeseries.get_time_series(
        name=TEST_TS_ID,
        office_id=TEST_OFFICE,
        start_time=TEST_START,
        end_time=TEST_END
    ).df

    df.iloc[1, df.columns.get_loc("value")] = 25.0

    timeseries.store_time_series({
        "name": TEST_TS_ID,
        "office-id": TEST_OFFICE,
        "location-id": TEST_LOCATION_ID,
        "units": TEST_UNIT,
        "time-series-type": "INST-VAL",
        "interval": TEST_INTERVAL,
        "start-time": TEST_START.isoformat(),
        "end-time": TEST_END.isoformat(),
        "values": df
    })

    df_updated = timeseries.get_time_series(
        name=TEST_TS_ID,
        office_id=TEST_OFFICE,
        start_time=TEST_START,
        end_time=TEST_END
    ).df

    assert df_updated["value"].iloc[-1] == 25.0

def test_delete_timeseries(requests_mock):
    timeseries_url = f"{API_ROOT}/timeseries/{TEST_OFFICE}/{TEST_TS_ID}"
    location_url = f"{API_ROOT}/locations/{TEST_OFFICE}/{TEST_LOCATION_ID}"

    requests_mock.delete(timeseries_url, status_code=204)
    requests_mock.get(timeseries_url, text=read_resource_file("timeseries_get_response_empty.json"))
    requests_mock.delete(location_url, status_code=204)

    timeseries.delete_time_series(name=TEST_TS_ID, office_id=TEST_OFFICE)

    df_final = timeseries.get_time_series(
        name=TEST_TS_ID,
        office_id=TEST_OFFICE,
        start_time=TEST_START,
        end_time=TEST_END
    ).df

    assert df_final.empty

    locations.delete_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE)
"""