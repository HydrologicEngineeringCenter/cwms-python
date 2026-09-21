from uuid import uuid4

import cwms
import cwms.api


def test_get_location_operations():
    """
    Test the retrieval of location operations from the CWMS API.
    """
    TEST_OFFICE = "SPK"
    TEST_LOCATION_ID = f"pytestloc{uuid4().hex[:12]}"
    TEST_LATITUDE = 44.0
    TEST_LONGITUDE = -93.0

    cwms.store_location(
        {
            "name": TEST_LOCATION_ID,
            "office-id": TEST_OFFICE,
            "latitude": TEST_LATITUDE,
            "longitude": TEST_LONGITUDE,
            "elevation": 250.0,
            "horizontal-datum": "NAD83",
            "vertical-datum": "NAVD88",
            "location-type": "TESTING",
            "public-name": "Pytest Location",
            "long-name": "A pytest-generated location",
            "timezone-name": "America/Los_Angeles",
            "location-kind": "SITE",
            "nation": "US",
        }
    )

    try:
        # Other integration tests and seeded databases can contain SPK locations.
        loc_cat = cwms.get_locations_catalog(office_id=TEST_OFFICE)
        location = loc_cat.df.loc[loc_cat.df["name"] == TEST_LOCATION_ID]
        assert len(location) == 1
        assert location.iloc[0]["office"] == TEST_OFFICE
        assert location.iloc[0]["latitude"] == TEST_LATITUDE
        assert location.iloc[0]["longitude"] == TEST_LONGITUDE
    finally:
        cwms.delete_location(TEST_LOCATION_ID, TEST_OFFICE)
