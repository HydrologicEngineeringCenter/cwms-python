import pandas as pd
import pytest

import os
import cwms
import cwms.api


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for location operations test...")


def test_get_location_operations():
    """
    Test the retrieval of location operations from the CWMS API.
    """
    TEST_OFFICE = os.getenv("OFFICE", "SPK")
    TEST_LOCATION_ID = "pytest-loc-123"
    TEST_LATITUDE = 44.0
    TEST_LONGITUDE = -93.0

    loc_cat = cwms.get_locations_catalog(office_id=TEST_OFFICE)

    assert (
        len(loc_cat.df) == 0
    )  # Assuming no locations are present for office in the test environment
    print(len(loc_cat.df))
    print(loc_cat.df)

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

    loc_cat = cwms.get_locations_catalog(office_id=TEST_OFFICE)

    # assert(len(loc_cat.df)==0)  # Assuming no locations are present for office in the test environment
    print(len(loc_cat.df))
    print(loc_cat.df)
