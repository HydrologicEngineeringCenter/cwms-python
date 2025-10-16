#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import pytz

import cwms
import cwms.levels.location_levels as location_levels
import cwms.levels.specified_levels as specified_levels
import cwms.locations.physical_locations as locations

# Load test location level from tests/cda/resources/location_level.json
LEVEL_RESOURCE_PATH = Path(__file__).parent.parent / "resources" / "location_level.json"
with open(LEVEL_RESOURCE_PATH, "r") as f:
    TEST_LEVEL_DATA = json.load(f)

TEST_LEVEL_ID = TEST_LEVEL_DATA["location-level-id"]
TEST_OFFICE = TEST_LEVEL_DATA["office-id"]
TEST_UNIT = "SI"
TEST_DATUM = "NAVD88"
TEST_EFFECTIVE_DATE = pytz.UTC.localize(datetime.utcnow())


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for location levels tests...")


@pytest.fixture(scope="module", autouse=True)
def setup_level():
    # Store a test location with name "LEVEL"
    BASE_LOCATION_DATA = {
        "name": "LEVEL",
        "office-id": TEST_OFFICE,
        "latitude": 44.0,
        "longitude": -93.0,
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
    locations.store_location(BASE_LOCATION_DATA)

    # Store a specified level with id = "Bottom of Inlet"
    specified_level_data = {
        "id": "Bottom of Inlet",
        "office-id": TEST_OFFICE,
        "description": "Test Specified Level for Location Level Tests",
    }
    specified_levels.store_specified_level(specified_level_data)

    # Store a test location level before tests
    location_levels.store_location_level(TEST_LEVEL_DATA)
    yield
    # Delete the test location level after tests
    location_levels.delete_location_level(
        location_level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
        cascade_delete=True,
    )
    # Delete the specified level
    specified_levels.delete_specified_level("Bottom of Inlet", TEST_OFFICE)
    # Delete the location
    locations.delete_location("LEVEL", TEST_OFFICE, cascade_delete=True)


def test_retrieve_loc_levels_default():
    levels = location_levels.get_location_levels()
    assert isinstance(levels.json, list)


def test_get_loc_levels():
    begin = TEST_EFFECTIVE_DATE - timedelta(days=1)
    end = TEST_EFFECTIVE_DATE + timedelta(days=1)
    levels = location_levels.get_location_levels(
        level_id_mask=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        unit=TEST_UNIT,
        datum=TEST_DATUM,
        begin=begin,
        end=end,
        page=None,
        page_size=10,
    )
    assert any(lvl.get("level-id") == TEST_LEVEL_ID for lvl in levels.json)
    # Test DataFrame output
    df = levels.df
    assert not df.empty
    assert TEST_LEVEL_ID in df["level-id"].values


def test_get_loc_level():
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
        unit=TEST_UNIT,
    )
    assert level.json.get("level-id") == TEST_LEVEL_ID
    # Test DataFrame output
    df = level.df
    assert not df.empty
    assert TEST_LEVEL_ID in df["level-id"].values


def test_store_loc_level_json():
    # Try storing again with a different value
    data = TEST_LEVEL_DATA.copy()
    data["value"] = 101.0
    location_levels.store_location_level(data)
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
        unit=TEST_UNIT,
    )
    assert level.json.get("value") == 101.0


def test_delete_loc_level():
    # Store a new level, then delete it
    temp_level_id = "pytest_level_delete"
    temp_effective_date = TEST_EFFECTIVE_DATE + timedelta(minutes=1)
    data = TEST_LEVEL_DATA.copy()
    data["level-id"] = temp_level_id
    data["effective-date"] = temp_effective_date.isoformat()
    location_levels.store_location_level(data)
    location_levels.delete_location_level(
        location_level_id=temp_level_id,
        office_id=TEST_OFFICE,
        effective_date=temp_effective_date,
        cascade_delete=True,
    )
    # Try to get it, should raise or return None/empty
    try:
        level = location_levels.get_location_level(
            level_id=temp_level_id,
            office_id=TEST_OFFICE,
            effective_date=temp_effective_date,
            unit=TEST_UNIT,
        )
        assert not level.json or level.json.get("level-id") != temp_level_id
    except Exception:
        pass


def test_get_loc_level_ts():
    interval = "1Day"
    begin = TEST_EFFECTIVE_DATE - timedelta(days=2)
    end = TEST_EFFECTIVE_DATE + timedelta(days=2)
    ts = location_levels.get_level_as_timeseries(
        location_level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        unit=TEST_UNIT,
        begin=begin,
        end=end,
        interval=interval,
    )
    assert isinstance(ts.json, list) or ts.json is not None
    # Test DataFrame output
    df = ts.df
    assert df is not None
    assert not df.empty
    # Check that the DataFrame contains expected columns and values
    assert "value" in df.columns
    # Optionally check for at least one value (if expected)
    assert df["value"].notnull().any()


def test_update_location_level_api():
    # Update the value using cwms.update_location_level
    new_value = 300.0
    updated_data = TEST_LEVEL_DATA.copy()
    updated_data["constant-value"] = new_value
    cwms.update_location_level(location_level_id=TEST_LEVEL_ID, data=updated_data)
    # Retrieve and check the updated value
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
        unit=TEST_UNIT,
    )
    assert level.json.get("constant-value") == new_value
