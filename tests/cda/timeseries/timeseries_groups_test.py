import pandas as pd
import pytest


import cwms.locations.physical_locations as locations
import cwms.timeseries.timeseries as timeseries
import cwms.timeseries.timeseries_group as tg

TEST_OFFICE = "SPK"
TEST_LOCATION_ID = "pytest_group-loc-123"
TEST_LATITUDE = 44.1
TEST_LONGITUDE = -93.1


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

TEST_TSID = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.test1"

TS1_DATA = {
    "name": TEST_TSID,
    "units": "ft",
    "office-id": TEST_OFFICE,
    "values": [[1509654000000, 54.3, 0]],
}

TS2_DATA = {
    "name": f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.test1",
    "office-id": TEST_OFFICE,
    "values": [[1509654000000, 66.0, 0]],
}


TEST_CATEGORY_ID = "Test Category Name"
TEST_CAT_DESCRIPT = "test cat description"
TEST_GROUP_ID = "Test Group Name"
TEST_GROUP_DESCRIPT = "test group description"


TS_GROUP_DATA = {
    "office-id": TEST_OFFICE,
    "id": TEST_GROUP_ID,
    "time-series-category": {
        "office-id": TEST_OFFICE,
        "id": TEST_CATEGORY_ID,
        "description": TEST_CAT_DESCRIPT,
    },
    "description": TEST_GROUP_DESCRIPT,
}


@pytest.fixture(autouse=True)
def init_session(request):
    print("Initializing CWMS API session for locations operations test...")


def test_store_location():
    locations.store_location(BASE_LOCATION_DATA)
    df = locations.get_location(location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE).df
    assert TEST_LOCATION_ID in df["name"].values


def test_store_timeseries():
    timeseries.store_timeseries(TS1_DATA)
    data = timeseries.get_timeseries(ts_id=TEST_TSID, office_id=TEST_OFFICE).json
    assert TEST_TSID in data["name"]


def test_store_timeseries_group():

    tg.store_timeseries_groups(data=TS_GROUP_DATA)
    data = tg.get_timeseries_group(
        group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        office_id=TEST_OFFICE,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
    ).json
    assert TEST_CATEGORY_ID in data["time-series-category"]["id"]


def test_get_timeseries_groups():
    data = tg.get_timeseries_groups(
        office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        timeseries_category_like=TEST_CATEGORY_ID,
    ).json
    assert TEST_CATEGORY_ID in data[0]["time-series-category"]["id"]


def test_timeseries_group_df_to_json():
    data = []
    data.append({"timeseries-id": TEST_TSID, "office-id": TEST_OFFICE, "alias": ""})

    # Create the pandas DataFrame
    df = pd.DataFrame(data)

    json_dict = tg.timeseries_group_df_to_json(
        data=df,
        group_id=TEST_GROUP_ID,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        category_id=TEST_CATEGORY_ID,
    )
    assert TEST_GROUP_ID in json_dict["id"]
    assert TEST_LOCATION_ID in json_dict["assigned-time-series"][0]["timeseries-id"]


def test_update_timeseries_groups():
    data = []
    data.append({"timeseries-id": TEST_TSID, "office-id": TEST_OFFICE, "alias": ""})

    # Create the pandas DataFrame
    df = pd.DataFrame(data)

    json_dict = tg.timeseries_group_df_to_json(
        data=df,
        group_id=TEST_GROUP_ID,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        category_id=TEST_CATEGORY_ID,
    )
    tg.update_timeseries_groups(
        group_id=TEST_GROUP_ID,
        office_id=TEST_OFFICE,
        replace_assigned_ts=True,
        data=json_dict,
    )
    data = tg.get_timeseries_group(
        group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        category_office_id=TEST_OFFICE,
    ).json
    assert data["id"] == TEST_GROUP_ID
    assert len(data["assigned-time-series"]) == 1
    assert data["assigned-time-series"][0]["timeseries-id"] == TEST_TSID
    assert data["assigned-time-series"][0]["office-id"] == TEST_OFFICE


def test_delete_timeseries_group():

    # update with no timeseries in the group first
    df = pd.DataFrame(columns=["timeseries-id", "office-id", "alias"])

    json_dict = tg.timeseries_group_df_to_json(
        data=df,
        group_id=TEST_GROUP_ID,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        category_id=TEST_CATEGORY_ID,
    )
    tg.update_timeseries_groups(
        group_id=TEST_GROUP_ID,
        office_id=TEST_OFFICE,
        replace_assigned_ts=True,
        data=json_dict,
    )

    # delete the group
    tg.delete_timeseries_group(
        group_id=TEST_GROUP_ID, category_id=TEST_CATEGORY_ID, office_id=TEST_OFFICE
    )


def test_delete_location():
    locations.delete_location(
        location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE, cascade_delete=True
    )
    df_final = locations.get_locations(
        office_id=TEST_OFFICE, location_ids=TEST_LOCATION_ID
    ).df
    assert df_final.empty or TEST_LOCATION_ID not in df_final.get("name", [])
