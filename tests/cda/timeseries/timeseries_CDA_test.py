from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.timeseries.timeseries as ts

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_ts"
TEST_TSID = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Test"
TEST_TSID_MULTI = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi"
TEST_TSID_MULTI1 = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi-1"
TEST_TSID_MULTI2 = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi-2"
TEST_TSID_STORE = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Store"
TEST_TSID_CHUNK_MULTI = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi-Chunk"
TEST_TSID_COPY = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Copy"
TEST_TSID_DELETE = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Delete"
TEST_TSID_CHUNK_NULLS = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi-Nulls"
TEST_TSID_COPY_NULLS = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Copy-Nulls"
TS_ID_REV_TEST = TEST_TSID_MULTI.replace("Raw-Multi", "Raw-Rev-Test")
TEST_TSID_CHUNK_PARTIAL = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Multi-Partial"
TEST_TSID_MISSING = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.Raw-Missing-404"
# Generate 15-minute interval timestamps
START_DATE_CHUNK_MULTI = datetime(2025, 7, 31, 0, 0, tzinfo=timezone.utc)
END_DATE_CHUNK_MULTI = datetime(2025, 9, 30, 23, 45, tzinfo=timezone.utc)
TSIDS = [
    TS_ID_REV_TEST,
    TEST_TSID,
    TEST_TSID_MULTI,
    TEST_TSID_MULTI1,
    TEST_TSID_MULTI2,
    TEST_TSID_STORE,
    TEST_TSID_CHUNK_MULTI,
    TEST_TSID_COPY,
    TEST_TSID_CHUNK_NULLS,
    TEST_TSID_COPY_NULLS,
    TEST_TSID_CHUNK_PARTIAL,
    TEST_TSID_MISSING,
    TEST_TSID_DELETE,
]


DT_CHUNK_MULTI = pd.date_range(
    start=START_DATE_CHUNK_MULTI,
    end=END_DATE_CHUNK_MULTI,
    freq="15min",
    tz="UTC",
)
# Create DataFrame
DF_CHUNK_MULTI = pd.DataFrame(
    {
        "date-time": DT_CHUNK_MULTI,
        "value": [86.57 + (i % 10) * 0.01 for i in range(len(DT_CHUNK_MULTI))],
        "quality-code": [0] * len(DT_CHUNK_MULTI),
    }
)
# Create a copy of the original DataFrame and introduce null values
DF_WITH_NULLS = DF_CHUNK_MULTI.copy()
# Set the 100 and 200 index value to null
DF_WITH_NULLS.loc[100, "value"] = None
DF_WITH_NULLS.loc[200, "value"] = None


DF_MULTI_TIMESERIES1 = pd.DataFrame(
    {
        "date-time": DT_CHUNK_MULTI,
        "value": [86.57 + (i % 10) * 0.01 for i in range(len(DT_CHUNK_MULTI))],
        "quality-code": [0] * len(DT_CHUNK_MULTI),
        "ts_id": [TEST_TSID_MULTI1] * len(DT_CHUNK_MULTI),
        "units": ["ft"] * len(DT_CHUNK_MULTI),
    }
)

DF_MULTI_TIMESERIES2 = pd.DataFrame(
    {
        "date-time": DT_CHUNK_MULTI,
        "value": [86.57 + (i % 10) * 0.01 for i in range(len(DT_CHUNK_MULTI))],
        "quality-code": [0] * len(DT_CHUNK_MULTI),
        "ts_id": [TEST_TSID_MULTI2] * len(DT_CHUNK_MULTI),
        "units": ["ft"] * len(DT_CHUNK_MULTI),
    }
)

DF_MULTI_TIMESERIES = pd.concat(
    [DF_MULTI_TIMESERIES1, DF_MULTI_TIMESERIES2]
).reset_index(drop=True)

DT = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
EPOCH_MS = int(DT.timestamp() * 1000)
BEGIN = DT - timedelta(minutes=5)
END = DT + timedelta(minutes=5)


def _cleanup_tsids_and_location():
    for ts_id in TSIDS:
        try:
            cwms.delete_timeseries_identifier(
                ts_id=ts_id, office_id=TEST_OFFICE, delete_method="DELETE_ALL"
            )
        except Exception as e:
            print(f"Failed to delete tsid {ts_id}: {e}")
    try:
        cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE, cascade_delete=True)
    except Exception as e:
        print(f"Failed to delete location {TEST_LOCATION_ID}: {e}")


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    # Clean up any leftover state from a prior aborted run before starting.
    _cleanup_tsids_and_location()

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

    ts.store_timeseries(
        {
            "name": TEST_TSID_DELETE,
            "office-id": TEST_OFFICE,
            "units": "ft",
            "values": [[EPOCH_MS, 99, 0]],
        }
    )
    # Warmup GET against /cwms-data/timeseries. The first GET on this
    # endpoint after a cold connection intermittently trips a CDA bug
    # (SESSION_OFFICE_ID_NOT_SET → ORA-01422 for multi-office API-key
    # users). Absorbing the failure here keeps individual tests clean.
    try:
        ts.get_timeseries(TEST_TSID_DELETE, TEST_OFFICE, begin=BEGIN, end=END)
    except Exception as e:
        print(f"Warmup GET failed (expected on cold CDA connection): {e}")
    yield
    _cleanup_tsids_and_location()


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for timeseries tests...")


def test_store_timeseries():
    ts_json = {
        "name": TEST_TSID_STORE,
        "office-id": TEST_OFFICE,
        "units": "ft",
        "values": [[EPOCH_MS, 99, 0]],
    }
    ts.store_timeseries(ts_json)
    data = ts.get_timeseries(TEST_TSID_STORE, TEST_OFFICE, begin=BEGIN, end=END).json
    assert data["name"] == TEST_TSID_STORE
    assert data["office-id"] == TEST_OFFICE
    assert data["units"] == "ft"
    assert data["values"][0][1] == pytest.approx(99)


def test_get_timeseries():
    data = ts.get_timeseries(TEST_TSID_STORE, TEST_OFFICE, begin=BEGIN, end=END).json
    assert data["name"] == TEST_TSID_STORE
    assert data["office-id"] == TEST_OFFICE
    assert data["units"] == "ft"
    assert data["values"][0][1] == pytest.approx(99)


def test_timeseries_df_to_json():
    df = pd.DataFrame(
        {
            "date-time": [DT],
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
        [DT.isoformat(), 42, 0]
    ], "Values do not match expected"


def test_store_multi_timeseries_df():
    df = pd.DataFrame(
        {
            "date-time": [DT, DT],
            "value": [7, 8],
            "quality-code": [0, 0],
            "ts_id": [TEST_TSID_MULTI, TS_ID_REV_TEST],
            "units": ["ft", "ft"],
        }
    )
    ts.store_multi_timeseries_df(
        df,
        TEST_OFFICE,
        store_rule="REPLACE_ALL",
        override_protection=False,
    )
    data1 = ts.get_timeseries(
        TEST_TSID_MULTI, TEST_OFFICE, multithread=False, begin=BEGIN, end=END
    ).json
    data2 = ts.get_timeseries(
        TS_ID_REV_TEST, TEST_OFFICE, multithread=False, begin=BEGIN, end=END
    ).json
    assert data1["name"] == TEST_TSID_MULTI
    assert data1["office-id"] == TEST_OFFICE
    assert data1["units"] == "ft"
    assert data1["values"][0][1] == pytest.approx(7)
    assert data2["name"] == TS_ID_REV_TEST
    assert data2["office-id"] == TEST_OFFICE
    assert data2["units"] == "ft"
    assert data2["values"][0][1] == pytest.approx(8)


def test_store_multi_timeseries_chunks_df():
    # test getting multi timeseries while using the chunk method as well
    ts.store_multi_timeseries_df(
        data=DF_MULTI_TIMESERIES,
        office_id=TEST_OFFICE,
        store_rule="REPLACE_ALL",
        override_protection=False,
        multithread=True,
    )
    data1 = ts.get_timeseries(
        ts_id=TEST_TSID_MULTI1,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        multithread=False,
    ).df
    data2 = ts.get_timeseries(
        ts_id=TEST_TSID_MULTI2,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        multithread=False,
    ).df
    df_multi1 = DF_MULTI_TIMESERIES1[["date-time", "value", "quality-code"]]
    df_multi2 = DF_MULTI_TIMESERIES2[["date-time", "value", "quality-code"]]
    pdt.assert_frame_equal(
        data1, df_multi1
    ), f"Data frames do not match: original = {df_multi1.describe()}, stored = {data1.describe()}"

    pdt.assert_frame_equal(
        data2, df_multi2
    ), f"Data frames do not match: original = {df_multi2.describe()}, stored = {data2.describe()}"


def test_get_multi_timeseries_chunk_df():
    df = ts.get_multi_timeseries_df(
        ts_ids=[TEST_TSID_MULTI1, TEST_TSID_MULTI2],
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        melted=True,
    )
    assert df is not None, "Returned DataFrame is None"
    assert not df.empty, "Returned DataFrame is empty"

    pdt.assert_frame_equal(
        df, DF_MULTI_TIMESERIES
    ), f"Data frames do not match: original = {DF_MULTI_TIMESERIES.describe()}, stored = {df.describe()}"


def test_get_multi_timeseries_df():
    df = ts.get_multi_timeseries_df(
        [TEST_TSID_MULTI, TS_ID_REV_TEST], TEST_OFFICE, begin=BEGIN, end=END
    )
    assert df is not None, "Returned DataFrame is None"
    assert not df.empty, "Returned DataFrame is empty"
    assert any(
        TEST_TSID_MULTI in str(col) for col in df.columns
    ), f"{TEST_TSID_MULTI} not found in DataFrame columns"
    assert any(
        TS_ID_REV_TEST in str(col) for col in df.columns
    ), f"{TS_ID_REV_TEST} not found in DataFrame columns"


def test_store_timeseries_chunk_ts():
    # Define parameters
    ts_id = TEST_TSID_CHUNK_MULTI
    office = TEST_OFFICE
    units = "m"

    # Convert DataFrame to JSON format
    ts_json = ts.timeseries_df_to_json(DF_CHUNK_MULTI, ts_id, units, office)

    ts.store_timeseries(ts_json, multithread=True, chunk_size=2 * 7 * 24 * 4)

    data_multithread = ts.get_timeseries(
        ts_id=TEST_TSID_CHUNK_MULTI,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    )
    df = data_multithread.df
    # make sure the dataframe matches stored dataframe
    pdt.assert_frame_equal(
        df, DF_CHUNK_MULTI
    ), f"Data frames do not match: original = {DF_CHUNK_MULTI.describe()}, stored = {df.describe()}"


def test_store_timeseries_partial_chunk_fail_real_api():
    """One chunk with a corrupt value is rejected by the real API;
    the rest succeed. RuntimeError must surface with exactly 1 failure."""
    chunk_size = 2 * 7 * 24 * 4  # two weeks of 15-min data

    ts_json = ts.timeseries_df_to_json(
        DF_CHUNK_MULTI, TEST_TSID_CHUNK_PARTIAL, "m", TEST_OFFICE
    )

    # Confirm multiple chunks exist so the multithreaded path is taken
    chunks = ts.chunk_timeseries_data(ts_json, chunk_size)
    assert (
        len(chunks) > 1
    ), "Test requires multiple chunks — increase DF_CHUNK_MULTI range"

    # Corrupt the first value of the second chunk so only that chunk is rejected.
    corrupt_index = chunk_size
    original = ts_json["values"][corrupt_index]
    ts_json["values"][corrupt_index] = [original[0], "not_a_number", original[2]]

    with pytest.raises(RuntimeError) as exc_info:
        ts.store_timeseries(ts_json, multithread=True, chunk_size=chunk_size)

    error_msg = str(exc_info.value)
    assert "1 of " in error_msg and "chunk(s) failed to store" in error_msg
    assert "Error storing chunk from" in error_msg


def test_get_timeseries_partial_chunk_fail_real_api():
    """One fetch chunk is forced to fail; the others really hit the API.
    RuntimeError must surface with the chunk details. Depends on
    test_store_timeseries_chunk_ts having populated TEST_TSID_CHUNK_MULTI."""

    max_days = 14
    sabotage_begin = START_DATE_CHUNK_MULTI + timedelta(days=max_days)

    chunks = ts.chunk_timeseries_time_range(
        START_DATE_CHUNK_MULTI,
        END_DATE_CHUNK_MULTI.replace(tzinfo=timezone.utc),
        timedelta(days=max_days),
    )
    assert (
        len(chunks) > 1
    ), "Test requires multiple chunks — increase chunk count or shrink max_days"
    assert any(
        start == sabotage_begin for start, _ in chunks
    ), "sabotage_begin must align with a real chunk boundary"

    original_chunk = ts.get_timeseries_chunk

    def sabotaged(selector, endpoint, param, begin, end):
        if begin == sabotage_begin:
            raise RuntimeError("simulated CDA failure")
        return original_chunk(selector, endpoint, param, begin, end)

    with patch.object(ts, "get_timeseries_chunk", side_effect=sabotaged):
        with pytest.raises(RuntimeError) as exc_info:
            ts.get_timeseries(
                ts_id=TEST_TSID_CHUNK_MULTI,
                office_id=TEST_OFFICE,
                begin=START_DATE_CHUNK_MULTI,
                end=END_DATE_CHUNK_MULTI,
                max_days_per_chunk=max_days,
                unit="SI",
            )

    error_msg = str(exc_info.value)
    assert "1 of " in error_msg and "chunk(s) failed to fetch" in error_msg
    assert "Failed to fetch data from" in error_msg
    assert "simulated CDA failure" in error_msg


def test_get_timeseries_missing_chunk_404_real_api():
    """A missing time series should fail once with a 404, not retry."""

    max_days = 14
    chunks = ts.chunk_timeseries_time_range(
        START_DATE_CHUNK_MULTI,
        END_DATE_CHUNK_MULTI.replace(tzinfo=timezone.utc),
        timedelta(days=max_days),
    )
    assert len(chunks) > 1, "Test requires multiple chunks to exercise retry scope"

    with pytest.raises(RuntimeError) as exc_info:
        ts.get_timeseries(
            ts_id=TEST_TSID_MISSING,
            office_id=TEST_OFFICE,
            begin=START_DATE_CHUNK_MULTI,
            end=END_DATE_CHUNK_MULTI,
            max_days_per_chunk=max_days,
            unit="SI",
        )

    error_msg = str(exc_info.value)
    assert "chunk(s) failed to fetch" in error_msg
    assert "Failed to fetch data from" in error_msg
    assert "Not Found" in error_msg or "404" in error_msg


def test_store_timesereis_chunk_to_with_null_values():
    # Define parameters
    ts_id = TEST_TSID_CHUNK_NULLS
    office = TEST_OFFICE
    units = "m"

    # Convert DataFrame to JSON format
    ts_json = ts.timeseries_df_to_json(DF_WITH_NULLS, ts_id, units, office)

    ts.store_timeseries(ts_json, multithread=True)

    data_nulls = ts.get_timeseries(
        ts_id=ts_id,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        unit="SI",
    )
    df_nulls = data_nulls.df
    # make sure the dataframe matches stored dataframe with null values
    pdt.assert_frame_equal(
        df_nulls, DF_WITH_NULLS
    ), f"Data frames do not match: original with nulls = {DF_WITH_NULLS.describe()}, stored = {df_nulls.describe()}"


def test_copy_timeseries_chunk_json_with_nulls():
    data_json = ts.get_timeseries(
        ts_id=TEST_TSID_CHUNK_NULLS,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    ).json
    data_json["name"] = TEST_TSID_COPY_NULLS
    ts.store_timeseries(data_json)

    data_multithread = ts.get_timeseries(
        ts_id=TEST_TSID_COPY_NULLS,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    )
    df = data_multithread.df
    # make sure the dataframe matches stored dataframe with null values
    pdt.assert_frame_equal(
        df, DF_WITH_NULLS
    ), f"Data frames do not match: original with nulls = {DF_WITH_NULLS.describe()}, stored = {df.describe()}"


def test_copy_timeseries_chunk_json():
    data_json = ts.get_timeseries(
        ts_id=TEST_TSID_CHUNK_MULTI,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    ).json
    data_json["name"] = TEST_TSID_COPY
    ts.store_timeseries(data_json)

    data_multithread = ts.get_timeseries(
        ts_id=TEST_TSID_COPY,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    )
    df = data_multithread.df
    # make sure the dataframe matches stored dataframe
    pdt.assert_frame_equal(
        df, DF_CHUNK_MULTI
    ), f"Data frames do not match: original = {DF_CHUNK_MULTI.describe()}, stored = {df.describe()}"


def test_read_timeseries_chunk_ts():
    # Capture the log output
    data_multithread = ts.get_timeseries(
        ts_id=TEST_TSID_CHUNK_MULTI,
        office_id=TEST_OFFICE,
        begin=START_DATE_CHUNK_MULTI,
        end=END_DATE_CHUNK_MULTI,
        max_days_per_chunk=14,
        unit="SI",
    )

    # Check metadata for multithreaded read
    data_json = data_multithread.json

    df = data_multithread.df.copy()
    assert df is not None, "Returned DataFrame is None"
    assert not df.empty, "Returned DataFrame is empty"

    # make sure the dataframe matches stored dataframe
    pdt.assert_frame_equal(
        df, DF_CHUNK_MULTI
    ), f"Data frames do not match: original = {DF_CHUNK_MULTI.describe()}, stored = {df.describe()}"

    # Check metadata
    assert data_json["name"] == TEST_TSID_CHUNK_MULTI
    assert data_json["office-id"] == TEST_OFFICE
    assert data_json["units"] == "m"


def test_delete_timeseries():
    ts.delete_timeseries(TEST_TSID_DELETE, TEST_OFFICE, BEGIN, END)
    result = ts.get_timeseries(TEST_TSID_DELETE, TEST_OFFICE)
    assert result is None or result.json.get("values", []) == []
