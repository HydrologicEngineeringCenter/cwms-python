from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import cwms

TEST_OFFICE = "MVP"
TEST_LEVEL_ID = "pytest_level"
TEST_LEVEL_JSON = {
    "name": TEST_LEVEL_ID,
    "office-id": TEST_OFFICE,
    "description": "A pytest-generated level",
    "unit": "ft",
    "value": 100.0,
}


@pytest.fixture(scope="module", autouse=True)
def setup_location_level():
    # Store a test location level
    cwms.store_location_level(TEST_LEVEL_JSON)
    yield
    # Cleanup after tests
    cwms.delete_location_level(TEST_LEVEL_ID, TEST_OFFICE, cascade_delete=True)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for location level tests...")


def test_store_location_level():
    # Verify the level exists
    data = cwms.get_location_level(
        TEST_LEVEL_ID, TEST_OFFICE, datetime.now(timezone.utc)
    )
    assert data.json["name"] == TEST_LEVEL_ID
    assert data.json["office-id"] == TEST_OFFICE
    assert data.json.get("unit") == "ft"


def test_get_location_levels():
    data = cwms.get_location_levels(level_id_mask=TEST_LEVEL_ID, office_id=TEST_OFFICE)
    assert isinstance(data.df, pd.DataFrame)
    assert not data.df.empty
    assert TEST_LEVEL_ID in data.df.get("name", []).values


def test_get_location_level():
    effective_date = datetime.now(timezone.utc)
    data = cwms.get_location_level(TEST_LEVEL_ID, TEST_OFFICE, effective_date)
    assert data.json["name"] == TEST_LEVEL_ID
    assert data.json["office-id"] == TEST_OFFICE


def test_get_level_as_timeseries():
    begin = datetime.now(timezone.utc) - timedelta(hours=1)
    end = datetime.now(timezone.utc)
    data = cwms.get_level_as_timeseries(
        TEST_LEVEL_ID, TEST_OFFICE, unit="ft", begin=begin, end=end
    )
    assert isinstance(data.df, pd.DataFrame)
    # Column 'value' should exist
    assert "value" in data.df.columns


def test_delete_location_level():
    # Create a temporary level to delete
    temp_level_id = f"{TEST_LEVEL_ID}-delete"
    temp_json = TEST_LEVEL_JSON.copy()
    temp_json["name"] = temp_level_id
    cwms.store_location_level(temp_json)

    # Delete the level
    cwms.delete_location_level(temp_level_id, TEST_OFFICE)
    # Attempting to retrieve it should fail or return empty
    data = cwms.get_location_levels(level_id_mask=temp_level_id, office_id=TEST_OFFICE)
    assert data.df.empty or temp_level_id not in data.df.get("name", []).values
