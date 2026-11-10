import numpy as np
import pandas as pd
import pytest

import cwms
import cwms.timeseries.timeseries_group as tg

TEST_OFFICE = "MVP"
TEST_LOCATION_ID = "pytest_group"
TEST_TSID = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.test1"
TEST_TSID_2 = f"{TEST_LOCATION_ID}.Stage.Inst.15Minutes.0.test2"

TEST_CATEGORY_ID = "Test Category Name"
TEST_CAT_DESCRIPT = "test cat description"
TEST_GROUP_ID = "Test Group Name"
TEST_GROUP_DESCRIPT = "test group description"

TEST_CWMS_GROUP_ID = "USGS TS Data Acquisition"
TEST_CWMS_CATEGORY_ID = "Data Acquisition"
TEST_CWMS_GROUP_OFFICE_ID = "CWMS"
TEST_CWMS_CATEGORY_OFFICE_ID = "CWMS"


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


# Setup and teardown fixture for test location
@pytest.fixture(scope="module", autouse=True)
def setup_data():

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

    # Store location before tests
    cwms.store_location(BASE_LOCATION_DATA)

    TS1_DATA = {
        "name": TEST_TSID,
        "units": "ft",
        "office-id": TEST_OFFICE,
        "values": [[1509654000000, 54.3, 0]],
    }
    TS2_DATA = {
        "name": TEST_TSID_2,
        "units": "ft",
        "office-id": TEST_OFFICE,
        "values": [[1509654000000, 55.3, 0]],
    }
    # Store timeseries before tests
    cwms.store_timeseries(TS1_DATA)
    cwms.store_timeseries(TS2_DATA)

    yield

    # Delete location and TS after tests
    cwms.delete_location(
        location_id=TEST_LOCATION_ID, office_id=TEST_OFFICE, cascade_delete=True
    )


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for timeseries groups tests...")


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
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        timeseries_category_like=TEST_CATEGORY_ID,
    ).json
    assert TEST_CATEGORY_ID in data[0]["time-series-category"]["id"]


def test_timeseries_group_df_to_json():
    data = []
    data.append(
        {
            "timeseries-id": TEST_TSID,
            "office-id": TEST_OFFICE,
            "alias-id": np.nan,
            "attribute": np.nan,
        }
    )

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


def test_timeseries_group_patch_df_to_json():
    assign_df = pd.DataFrame(
        [
            {
                "office-id": TEST_OFFICE,
                "timeseries-id": TEST_TSID,
                "alias-id": "test-alias",
                "attribute": 1,
            }
        ]
    )
    unassign_df = pd.DataFrame(
        [{"office-id": TEST_OFFICE, "timeseries-id": "old.Test.Inst.1Hour.0"}]
    )

    patch_json = tg.timeseries_group_patch_df_to_json(
        group_id=TEST_GROUP_ID,
        group_office_id=TEST_OFFICE,
        category_office_id=TEST_OFFICE,
        category_id=TEST_CATEGORY_ID,
        assign_data=assign_df,
        unassign_data=unassign_df,
    )

    assert patch_json == {
        "office-id": TEST_OFFICE,
        "id": TEST_GROUP_ID,
        "time-series-category": {
            "office-id": TEST_OFFICE,
            "id": TEST_CATEGORY_ID,
        },
        "membership": {
            "assign": [
                {
                    "office-id": TEST_OFFICE,
                    "timeseries-id": TEST_TSID,
                    "attribute": 1,
                    "alias-id": "test-alias",
                }
            ],
            "unassign": [{"office-id": TEST_OFFICE, "name": "old.Test.Inst.1Hour.0"}],
        },
    }


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
        group_office_id=TEST_OFFICE,
    ).json
    assert data["id"] == TEST_GROUP_ID
    assert len(data["assigned-time-series"]) == 1
    assert data["assigned-time-series"][0]["timeseries-id"] == TEST_TSID
    assert data["assigned-time-series"][0]["office-id"] == TEST_OFFICE


def test_delete_timeseries_group():

    # delete the group
    tg.delete_timeseries_group(
        group_id=TEST_GROUP_ID,
        category_id=TEST_CATEGORY_ID,
        office_id=TEST_OFFICE,
        cascade_delete=True,
    )


def _patch_installed_group(assign_data=None, unassign_data=None):
    patch_data = tg.timeseries_group_patch_df_to_json(
        group_id=TEST_CWMS_GROUP_ID,
        group_office_id=TEST_CWMS_GROUP_OFFICE_ID,
        category_office_id=TEST_CWMS_CATEGORY_OFFICE_ID,
        category_id=TEST_CWMS_CATEGORY_ID,
        assign_data=assign_data,
        unassign_data=unassign_data,
    )
    tg.update_timeseries_groups(
        data=patch_data,
        group_id=TEST_CWMS_GROUP_ID,
        office_id=TEST_CWMS_GROUP_OFFICE_ID,
    )


def _get_installed_group_members():
    return tg.get_timeseries_group(
        group_id=TEST_CWMS_GROUP_ID,
        category_id=TEST_CWMS_CATEGORY_ID,
        group_office_id=TEST_CWMS_GROUP_OFFICE_ID,
        category_office_id=TEST_CWMS_CATEGORY_OFFICE_ID,
    ).json["assigned-time-series"]


def _group_assignment(ts_id, alias_id=None, attribute=0):
    return pd.DataFrame(
        [
            {
                "office-id": TEST_OFFICE,
                "timeseries-id": ts_id,
                "alias-id": alias_id,
                "attribute": attribute,
            }
        ]
    )


def _group_unassignment(*ts_ids):
    return pd.DataFrame(
        [{"office-id": TEST_OFFICE, "timeseries-id": ts_id} for ts_id in ts_ids]
    )


def _installed_group_member_ids():
    return {
        (member["office-id"], member["timeseries-id"])
        for member in _get_installed_group_members()
    }


def test_patch_existing_group_updates_member_alias_and_attribute():
    ts_key = (TEST_OFFICE, TEST_TSID)
    cleanup = _group_unassignment(TEST_TSID)
    try:
        _patch_installed_group(
            assign_data=_group_assignment(TEST_TSID, "pytest-old", 1)
        )
        _patch_installed_group(
            assign_data=_group_assignment(TEST_TSID, "pytest-updated", 2)
        )

        updated = next(
            member
            for member in _get_installed_group_members()
            if (member["office-id"], member["timeseries-id"]) == ts_key
        )
        assert updated["alias-id"] == "pytest-updated"
        assert updated["attribute"] == 2
    finally:
        _patch_installed_group(unassign_data=cleanup)


def test_patch_existing_group_removes_one_member_and_keeps_others():
    removed_key = (TEST_OFFICE, TEST_TSID)
    retained_key = (TEST_OFFICE, TEST_TSID_2)
    cleanup = _group_unassignment(TEST_TSID, TEST_TSID_2)
    try:
        _patch_installed_group(
            assign_data=pd.DataFrame(
                [
                    {
                        "office-id": TEST_OFFICE,
                        "timeseries-id": TEST_TSID,
                        "attribute": 1,
                    },
                    {
                        "office-id": TEST_OFFICE,
                        "timeseries-id": TEST_TSID_2,
                        "attribute": 2,
                    },
                ]
            )
        )
        _patch_installed_group(unassign_data=_group_unassignment(TEST_TSID))

        member_ids = _installed_group_member_ids()
        assert removed_key not in member_ids
        assert retained_key in member_ids
    finally:
        _patch_installed_group(unassign_data=cleanup)


def test_patch_existing_group_adds_one_member_without_replacing_others():
    ts_key = (TEST_OFFICE, TEST_TSID)
    seed_key = (TEST_OFFICE, TEST_TSID_2)
    cleanup = _group_unassignment(TEST_TSID, TEST_TSID_2)
    try:
        _patch_installed_group(assign_data=_group_assignment(TEST_TSID_2))
        members_before = _installed_group_member_ids()

        _patch_installed_group(assign_data=_group_assignment(TEST_TSID))

        members_after = _installed_group_member_ids()
        assert ts_key in members_after
        assert seed_key in members_after
        assert members_before <= members_after
    finally:
        _patch_installed_group(unassign_data=cleanup)
