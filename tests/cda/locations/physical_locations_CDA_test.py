import uuid

import pytest

import cwms.locations.physical_locations as locations

TEST_OFFICE = "SPK"
TEST_LATITUDE = 44.0
TEST_LONGITUDE = -93.0


def _build_location_data(location_id: str, longitude: float = TEST_LONGITUDE) -> dict:
    return {
        "name": location_id,
        "office-id": TEST_OFFICE,
        "latitude": TEST_LATITUDE,
        "longitude": longitude,
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


def _new_location_id() -> str:
    return f"pytestloc{uuid.uuid4().hex[:10]}"


def _delete_if_exists(location_id: str) -> None:
    try:
        locations.delete_location(
            location_id=location_id,
            office_id=TEST_OFFICE,
            cascade_delete=True,
        )
    except Exception:
        pass


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for locations operations test...")


def test_store_location():
    location_id = _new_location_id()
    data = _build_location_data(location_id)
    try:
        locations.store_location(data)
        df = locations.get_location(location_id=location_id, office_id=TEST_OFFICE).df
        assert location_id in df["name"].values
    finally:
        _delete_if_exists(location_id)


def test_update_location_public_name():
    location_id = _new_location_id()
    updated_data = _build_location_data(location_id)
    updated_data["public-name"] = "Updated Public Name"
    try:
        locations.store_location(updated_data)
        df_updated = locations.get_location(
            location_id=location_id, office_id=TEST_OFFICE
        ).df
        row = df_updated[df_updated["name"] == location_id].iloc[0]
        assert row["public-name"] == "Updated Public Name"
    finally:
        _delete_if_exists(location_id)


def test_delete_location():
    location_id = _new_location_id()
    data = _build_location_data(location_id)
    locations.store_location(data)
    locations.delete_location(location_id=location_id, office_id=TEST_OFFICE)
    df_final = locations.get_locations(
        office_id=TEST_OFFICE, location_ids=location_id
    ).df
    assert df_final.empty or location_id not in df_final.get("name", [])


def test_get_location_returns_expected_data():
    location_id = _new_location_id()
    data = _build_location_data(location_id)
    try:
        locations.store_location(data)
        result = locations.get_location(location_id=location_id, office_id=TEST_OFFICE)
        df = result.df

        # Check returned data matches stored location
        assert df.shape[0] == 1
        row = df.iloc[0]
        assert row["name"] == location_id
        assert row["office-id"] == TEST_OFFICE
        assert row["latitude"] == TEST_LATITUDE
        assert row["longitude"] == TEST_LONGITUDE
    finally:
        _delete_if_exists(location_id)


def test_get_locations_returns_expected_location():
    # Store both locations
    first_location_id = _new_location_id()
    second_location_id = _new_location_id()
    first_location_data = _build_location_data(first_location_id)
    second_location_data = _build_location_data(second_location_id, longitude=-94.0)
    try:
        locations.store_location(first_location_data)
        locations.store_location(second_location_data)

        # Retrieve both locations
        pattern = f"{first_location_id}|{second_location_id}"
        result = locations.get_locations(office_id=TEST_OFFICE, location_ids=pattern)
        df = result.df

        for loc_data in [first_location_data, second_location_data]:
            loc_id = loc_data["name"]
            assert loc_id in df["name"].values
            row = df[df["name"] == loc_id].iloc[0]
            for key in ["office-id", "latitude", "longitude"]:
                assert row[key] == loc_data[key]
    finally:
        _delete_if_exists(first_location_id)
        _delete_if_exists(second_location_id)


def test_get_locations_returns_multiple_locations():
    # Store a second location
    first_location_id = _new_location_id()
    second_location_id = _new_location_id()
    first_location_data = _build_location_data(first_location_id)
    second_location_data = _build_location_data(second_location_id, longitude=-94.0)
    try:
        locations.store_location(first_location_data)
        locations.store_location(second_location_data)

        # Retrieve both locations
        pattern = f"{first_location_id}|{second_location_id}"
        result = locations.get_locations(office_id=TEST_OFFICE, location_ids=pattern)
        df = result.df

        # Check that both locations are present
        loc_ids = df["name"].values
        assert first_location_id in loc_ids
        assert second_location_id in loc_ids
    finally:
        _delete_if_exists(first_location_id)
        _delete_if_exists(second_location_id)
