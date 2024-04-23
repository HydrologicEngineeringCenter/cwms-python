import pandas as pd
import pytest

import cwms.api
import cwms.locations.physical_locations as locations

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

EXAMPLE_LOCATION_GROUP = {
    "office-id": "test-office",
    "id": "test-location-group",
    "location-category": {
        "office-id": "test-office",
        "id": "test-location-category",
        "description": "Location Category Description",
    },
    "description": "Location Group Description",
    "shared-loc-alias-id": "test-shared-loc-alias",
    "shared-ref-location-id": "test-shared-ref-location",
    "loc-group-attribute": 0,
    "assigned-locations": [
        {
            "locationId": "test-location",
            "officeId": "test-office",
            "aliasId": "test-alias",
            "attribute": 0,
            "refLocationId": "test-ref-location",
        }
    ],
}


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_location_group(requests_mock):
    group_id = "test-location-group"
    category_id = "test-location-category"
    office_id = "test-office"

    requests_mock.get(
        f"{_MOCK_ROOT}/location/group/{group_id}?"
        f"office={office_id}&"
        f"category-id={category_id}",
        json=EXAMPLE_LOCATION_GROUP,
    )

    data = locations.get_location_group(group_id, category_id, office_id)
    assert data.json == EXAMPLE_LOCATION_GROUP

    assert type(data.df) == pd.DataFrame
    assert data.df.shape == (1, 5)

    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "test-location",
        "test-office",
        "test-alias",
        0,
        "test-ref-location",
    ]
