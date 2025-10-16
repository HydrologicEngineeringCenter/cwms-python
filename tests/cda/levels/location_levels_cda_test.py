#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

import cwms.levels.location_levels as location_levels
import cwms.locations.physical_locations as locations

# Load test location level from tests/cda/resources/location_level.json
LEVEL_RESOURCE_PATH = Path(__file__).parent.parent / "resources" / "location_level.json"
with open(LEVEL_RESOURCE_PATH, "r") as f:
    TEST_LEVEL_DATA = json.load(f)

TEST_LEVEL_ID = TEST_LEVEL_DATA["location-level-id"]
TEST_OFFICE = TEST_LEVEL_DATA["office-id"]
TEST_UNIT = "SI"
TEST_DATUM = "NAVD88"
TEST_EFFECTIVE_DATE = pd.to_datetime(TEST_LEVEL_DATA["level-date"])


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

    # Store a test location level before tests
    location_levels.store_location_level(TEST_LEVEL_DATA)
    yield
    # Delete the test location level after tests
    location_levels.delete_location_level(
        location_level_id=TEST_LEVEL_ID, office_id=TEST_OFFICE, cascade_delete=True
    )
    # Delete the location
    locations.delete_location("LEVEL", TEST_OFFICE, cascade_delete=True)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for location levels tests...")


def test_get_loc_level():
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
    )
    assert level.json.get("location-level-id") == TEST_LEVEL_ID
    # Test DataFrame output
    df = level.df
    assert not df.empty
    assert TEST_LEVEL_ID in df["location-level-id"].values


def test_get_loc_levels():
    levels = location_levels.get_location_levels(
        office_id=TEST_OFFICE,
    )
    ids = [lvl.get("location-level-id") for lvl in levels.json["levels"]]
    assert TEST_LEVEL_ID in ids
    # Test DataFrame output
    df = levels.df
    assert not df.empty
    assert TEST_LEVEL_ID in df["location-level-id"].values


def test_store_loc_level_json():
    # Try storing again with a different value
    level_date = "2010-01-01T06:00:00Z"
    new_effective_date = pd.to_datetime("2010-01-01T06:00:00Z")
    data = TEST_LEVEL_DATA.copy()
    data["level-date"] = level_date
    data["constant-value"] = 101
    location_levels.store_location_level(data)
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=new_effective_date,
        unit=TEST_UNIT,
    )
    print(level.df)
    assert level.df.loc[0, "constant-value"] == 101


def test_delete_loc_level():
    # Store a new level, then delete it
    temp_effective_date = TEST_EFFECTIVE_DATE + timedelta(days=5)
    data = TEST_LEVEL_DATA.copy()
    data["level-date"] = temp_effective_date.isoformat()
    data["constant-value"] = 300
    location_levels.store_location_level(data)
    location_levels.delete_location_level(
        location_level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=temp_effective_date,
    )
    # Try to get it, should raise or return None/empty
    try:
        level = location_levels.get_location_level(
            level_id=TEST_LEVEL_ID,
            office_id=TEST_OFFICE,
            effective_date=temp_effective_date,
            unit=TEST_UNIT,
        )
        assert level.df.empty
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


def test_update_location_level():
    # Update the value using cwms.update_location_level
    new_value = 300
    updated_data = TEST_LEVEL_DATA.copy()
    updated_data["constant-value"] = new_value
    location_levels.update_location_level(
        level_id=TEST_LEVEL_ID, effective_date=TEST_EFFECTIVE_DATE, data=updated_data
    )
    # Retrieve and check the updated value
    level = location_levels.get_location_level(
        level_id=TEST_LEVEL_ID,
        office_id=TEST_OFFICE,
        effective_date=TEST_EFFECTIVE_DATE,
        unit=TEST_UNIT,
    )
    assert level.json.get("constant-value") == new_value
