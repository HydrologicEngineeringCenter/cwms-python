import os
import pytest

import cwms.locations.physical_locations as locations

TEST_OFFICE = os.getenv("OFFICE", "SPK")
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
def init_session(request):
    print("Initializing CWMS API session for locations operations test...")


def test_store_location():
    locations.store_location(BASE_LOCATION_DATA)
    df = locations.get_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID in df["name"].values


def test_update_location_public_name():
    updated_data = BASE_LOCATION_DATA.copy()
    updated_data["public-name"] = "Updated Public Name"
    locations.store_location(updated_data)
    df_updated = locations.get_location(
        location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE
    ).df
    row = df_updated[df_updated["name"] == TEST_LOCATION_ID].iloc[0]
    assert row["public-name"] == "Updated Public Name"


def test_delete_location():
    locations.store_location(BASE_LOCATION_DATA)
    locations.delete_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE)
    df_final = locations.get_locations(
        office_id=TEST_OFFICE, location_ids=TEST_LOCATION_ID
    ).df
    assert df_final.empty or TEST_LOCATION_ID not in df_final.get("name", [])


def test_get_location_returns_expected_data():
    locations.store_location(BASE_LOCATION_DATA)

    result = locations.get_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE)
    df = result.df

    # Check returned data matches stored location
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row["name"] == TEST_LOCATION_ID
    assert row["office-id"] == TEST_OFFICE
    assert row["latitude"] == TEST_LATITUDE
    assert row["longitude"] == TEST_LONGITUDE


def test_get_locations_returns_expected_location():
    # Store both locations
    second_location_id = "pytest-loc-456"
    second_location_data = BASE_LOCATION_DATA.copy()
    second_location_data["name"] = second_location_id
    second_location_data["longitude"] = -94.0
    locations.store_location(BASE_LOCATION_DATA)
    locations.store_location(second_location_data)

    # Retrieve both locations
    pattern = f"{TEST_LOCATION_ID}|{second_location_id}"
    result = locations.get_locations(office_id=TEST_OFFICE, location_ids=pattern)
    df = result.df

    for loc_data in [BASE_LOCATION_DATA, second_location_data]:
        loc_id = loc_data["name"]
        assert loc_id in df["name"].values
        row = df[df["name"] == loc_id].iloc[0]
        for key in ["office-id", "latitude", "longitude"]:
            assert row[key] == loc_data[key]


def test_get_locations_returns_multiple_locations():
    # Store a second location
    second_location_id = "pytest-loc-456"
    second_location_data = BASE_LOCATION_DATA.copy()
    second_location_data["name"] = second_location_id
    second_location_data["longitude"] = -94.0
    locations.store_location(BASE_LOCATION_DATA)
    locations.store_location(second_location_data)

    # Retrieve both locations
    pattern = f"{TEST_LOCATION_ID}|{second_location_id}"
    result = locations.get_locations(office_id=TEST_OFFICE, location_ids=pattern)
    df = result.df

    # Check that both locations are present
    loc_ids = df["name"].values
    assert TEST_LOCATION_ID in loc_ids
    assert second_location_id in loc_ids
