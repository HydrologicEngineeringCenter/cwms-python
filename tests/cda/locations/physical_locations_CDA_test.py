import pandas as pd
import pytest
import cwms
import cwms.api
import cwms.locations.physical_locations as locations

API_ROOT = "http://localhost:8081/cwms-data/"
API_KEY = "1234567890abcdef1234567890abcdef"  # Replace w actual API key

TEST_OFFICE = "SPK"
TEST_LOCATION_ID = "pytest-loc-123"
TEST_LATITUDE = 44.0
TEST_LONGITUDE = -93.0

BASE_LOCATION_DATA = {
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


@pytest.fixture(autouse=True)
def init_real_session():
    print("Initializing CWMS API session for physical locations CDA test...")
    cwms.api.init_session(api_root=API_ROOT, api_key=API_KEY)


def test_store_location():
    locations.store_location(BASE_LOCATION_DATA)
    df = locations.get_all_locations(office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID in df["location-id"].values


def test_get_location():
    df = locations.get_all_locations(office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID in df["location-id"].values


def test_update_location_public_name():
    updated_data = BASE_LOCATION_DATA.copy()
    updated_data["public-name"] = "Updated Public Name"
    locations.store_location(updated_data)
    df_updated = locations.get_all_locations(office_id=TEST_OFFICE).df
    row = df_updated[df_updated["location-id"] == TEST_LOCATION_ID].iloc[0]
    assert row["public-name"] == "Updated Public Name"


def test_delete_location():
    locations.delete_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE)
    df_final = locations.get_all_locations(office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID not in df_final["location-id"].values
