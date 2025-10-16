#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import json
from datetime import datetime
from pathlib import Path

import pytest

import cwms
import cwms.levels.specified_levels as specified_levels

# Load test specified level from tests/cda/resources/specified_level.json
RESOURCE_PATH = Path(__file__).parent.parent / "resources" / "specified_level.json"
with open(RESOURCE_PATH, "r") as f:
    TEST_SPECIFIED_LEVEL_DATA = json.load(f)

TEST_SPECIFIED_LEVEL_ID = TEST_SPECIFIED_LEVEL_DATA["id"]
TEST_OFFICE = TEST_SPECIFIED_LEVEL_DATA["office-id"]


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for specified levels tests...")


@pytest.fixture(scope="module", autouse=True)
def setup_specified_level():
    # Store a test specified level before tests
    specified_levels.store_specified_level(TEST_SPECIFIED_LEVEL_DATA)
    yield
    # Delete the test specified level after tests
    specified_levels.delete_specified_level(TEST_SPECIFIED_LEVEL_ID, TEST_OFFICE)


def test_get_specified_level():
    level = specified_levels.get_specified_levels(
        specified_level_mask=TEST_SPECIFIED_LEVEL_ID, office_id=TEST_OFFICE
    )
    assert level.json[0].get("id") == TEST_SPECIFIED_LEVEL_ID
    # DataFrame check
    df = level.df
    assert not df.empty
    assert TEST_SPECIFIED_LEVEL_ID in df["id"].values


def test_get_specified_levels():
    # Store a second specified level
    second_id = "pytest_specified_level_2"
    second_data = TEST_SPECIFIED_LEVEL_DATA.copy()
    second_data["id"] = second_id
    second_data["office-id"] = TEST_OFFICE
    specified_levels.store_specified_level(second_data)

    try:
        levels = specified_levels.get_specified_levels(office_id=TEST_OFFICE)
        ids = [lvl.get("id") for lvl in levels.json]
        assert TEST_SPECIFIED_LEVEL_ID in ids
        assert second_id in ids
        # DataFrame check
        df = levels.df
        assert not df.empty
        assert TEST_SPECIFIED_LEVEL_ID in df["id"].values
        assert second_id in df["id"].values
    finally:
        # Cleanup second specified level
        specified_levels.delete_specified_level(second_id, TEST_OFFICE)


def test_store_specified_level():
    # Try storing again with a different value
    new_specified_level_id = "MVP Test Specified"
    new_specified_level_disc = "MVP Level"
    data = TEST_SPECIFIED_LEVEL_DATA.copy()
    data["id"] = new_specified_level_id
    data["description"] = "MVP Level"
    specified_levels.store_specified_level(data=data)
    try:
        levels = specified_levels.get_specified_levels(
            specified_level_mask=new_specified_level_id, office_id=TEST_OFFICE
        )
        assert levels.json[0].get("description") == new_specified_level_disc
    finally:
        specified_levels.delete_specified_level(
            specified_level_id=new_specified_level_id, office_id=TEST_OFFICE
        )


def test_delete_specified_level():
    # Store a new specified level, then delete it
    temp_id = "pytest delete"
    data = TEST_SPECIFIED_LEVEL_DATA.copy()
    data["id"] = temp_id
    specified_levels.store_specified_level(data=data)
    specified_levels.delete_specified_level(
        specified_level_id=temp_id, office_id=TEST_OFFICE
    )
    # Try to get it, should raise or return None/empty
    try:
        levels = specified_levels.get_specified_levels(
            specified_level_mask=temp_id, office_id=TEST_OFFICE
        )
        assert not any(lvl.get("id") == temp_id for lvl in levels.json)
    except Exception:
        pass
