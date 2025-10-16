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


def test_get_specified_levels_default():
    levels = specified_levels.get_specified_levels()
    assert isinstance(levels.json, list)


def test_get_specified_level():
    level = specified_levels.get_specified_level(TEST_SPECIFIED_LEVEL_ID, TEST_OFFICE)
    assert level.json.get("id") == TEST_SPECIFIED_LEVEL_ID
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
        levels = specified_levels.get_specified_levels("*", TEST_OFFICE)
        ids = [lvl.get("specified-level-id") for lvl in levels.json]
        assert TEST_SPECIFIED_LEVEL_ID in ids
        assert second_id in ids
        # DataFrame check
        df = levels.df
        assert not df.empty
        assert TEST_SPECIFIED_LEVEL_ID in df["specified-level-id"].values
        assert second_id in df["specified-level-id"].values
    finally:
        # Cleanup second specified level
        specified_levels.delete_specified_level(second_id, TEST_OFFICE)


def test_store_specified_level():
    # Try storing again with a different value
    data = TEST_SPECIFIED_LEVEL_DATA.copy()
    data["value"] = 456.78
    specified_levels.store_specified_level(data)
    levels = specified_levels.get_specified_levels(TEST_SPECIFIED_LEVEL_ID, TEST_OFFICE)
    assert any(lvl.get("value") == 456.78 for lvl in levels.json)


def test_delete_specified_level():
    # Store a new specified level, then delete it
    temp_id = "pytest_specified_level_delete"
    data = TEST_SPECIFIED_LEVEL_DATA.copy()
    data["specified-level-id"] = temp_id
    specified_levels.store_specified_level(data)
    specified_levels.delete_specified_level(temp_id, TEST_OFFICE)
    # Try to get it, should not be present
    levels = specified_levels.get_specified_levels(temp_id, TEST_OFFICE)
    assert not any(lvl.get("specified-level-id") == temp_id for lvl in levels.json)
