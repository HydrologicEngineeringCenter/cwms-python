import pandas as pd
import pytest
import cwms.locations.location_groups as lg
import cwms.locations.physical_locations as locations

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_group"
TEST_LATITUDE = 45.1704758
TEST_LONGITUDE = -92.8411439


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
    "timezone-name": "America/Chicago",
    "location-kind": "SITE",
    "nation": "US",
}


TEST_CATEGORY_ID = "Test Category Name"
TEST_CAT_DESCRIPT = "test cat description"
TEST_GROUP_ID = "Test Group Name"
TEST_GROUP_DESCRIPT = "test group description"


LOC_GROUP_DATA = {
    "office-id": TEST_OFFICE,
    "id": TEST_GROUP_ID,
    "location-category": {
        "office-id": TEST_OFFICE,
        "id": TEST_CATEGORY_ID,
        "description": TEST_CAT_DESCRIPT,
    },
    "description": TEST_GROUP_DESCRIPT,
}

LOC_GROUP_DATA_UPDATE = {
    "office-id": TEST_OFFICE,
    "id": TEST_GROUP_ID,
    "location-category": {
        "office-id": TEST_OFFICE,
        "id": TEST_CATEGORY_ID,
        "description": TEST_CAT_DESCRIPT,
    },
    "description": TEST_GROUP_DESCRIPT,
    "assigned-locations": [{"location-id": TEST_LOCATION_ID, "office-id": TEST_OFFICE}],
}


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for locations operations test...")


def test_store_location():
    locations.store_location(BASE_LOCATION_DATA)
    df = locations.get_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID in df["name"].values


def test_store_location_group():

    lg.store_location_groups(data=LOC_GROUP_DATA)
    data = lg.get_location_group(
        loc_group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        office_id=TEST_OFFICE,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
    ).json
    assert TEST_CATEGORY_ID in data["location-category"]["id"]


def test_get_location_groups():
    data = lg.get_location_groups(
        office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        location_office_id=TEST_OFFICE,
        location_category_like=TEST_CATEGORY_ID,
    ).json
    assert TEST_CATEGORY_ID in data[0]["location-category"]["id"]


def test_location_group_df_to_json():
    data = []
    data.append(
        {
            "location-id": TEST_LOCATION_ID,
            "office-id": TEST_OFFICE,
            "alias-id": "",
            "attribute": "",
            "ref-location-id": "",
        }
    )

    # Create the pandas DataFrame
    df = pd.DataFrame(data)

    json_dict = lg.location_group_df_to_json(
        data=df,
        group_id=TEST_GROUP_ID,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        category_id=TEST_CATEGORY_ID,
    )
    assert TEST_GROUP_ID in json_dict["id"]
    assert TEST_LOCATION_ID in json_dict["assigned-locations"][0]["location-id"]


def test_update_location_group():
    lg.update_location_group(
        group_id=TEST_GROUP_ID,
        office_id=TEST_OFFICE,
        replace_assigned_locs=True,
        data=LOC_GROUP_DATA_UPDATE,
    )
    data = lg.get_location_group(
        loc_group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        office_id=TEST_OFFICE,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
    ).json
    assert data["id"] == TEST_GROUP_ID
    assert len(data["assigned-locations"]) == 1
    assert data["assigned-locations"][0]["location-id"] == TEST_LOCATION_ID
    assert data["assigned-locations"][0]["office-id"] == TEST_OFFICE


def test_delete_location_group():
    lg.delete_location_group(
        group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        office_id=TEST_OFFICE,
        cascade_delete=True,
    )


def test_delete_location():
    locations.store_location(BASE_LOCATION_DATA)
    locations.delete_location(
        location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE, cascade_delete=True
    )
    df_final = locations.get_locations(
        office_id=TEST_OFFICE, location_ids=TEST_LOCATION_ID
    ).df
    assert df_final.empty or TEST_LOCATION_ID not in df_final.get("name", [])
