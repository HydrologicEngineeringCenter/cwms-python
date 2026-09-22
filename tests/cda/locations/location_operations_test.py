import pandas as pd
import pytest

import cwms
import cwms.api


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for location operations test...")


def test_get_location_operations():
    """
    Test the retrieval of location operations from the CWMS API.
    """
    import uuid

    TEST_OFFICE = "SPK"
    TEST_LOCATION_ID = f"pytest-loc-123-{uuid.uuid4().hex[:8]}"
    TEST_LATITUDE = 44.0
    TEST_LONGITUDE = -93.0

    loc_cat = cwms.get_locations_catalog(office_id="SPK")
    assert isinstance(loc_cat.df, pd.DataFrame)

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

    loc_cat = cwms.get_locations_catalog(office_id="SPK")
    assert isinstance(loc_cat.df, pd.DataFrame)
    assert not loc_cat.df.empty
    assert TEST_LOCATION_ID in loc_cat.df["name"].astype(str).tolist()
