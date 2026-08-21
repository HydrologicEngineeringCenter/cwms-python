from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.projects.water_supply.water_users as wu

TEST_OFFICE = "SPK"
TEST_OFFICE2 = "LRL"
TEST_PROJECT_ID = "pytest_wu"
TEST_ENTITY_NAME = "Test User"
TEST_WATER_RIGHT = "Test Water Right"
TEST_ENTITY_NAME2 = "Test User 2"
TEST_ENTITY_NAME3 = "Test User 3"
TEST_ENTITY_NAME4 = "Test User 4"
TEST_ENTITY_NAME5 = "Test User 5"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"

PROJECT_LOCATION = {
    "office-id": TEST_OFFICE,
    "name": TEST_PROJECT_ID,
    "latitude": 0,
    "longitude": 0,
    "active": True,
    "public-name": PUBLIC_NAME,
    "long-name": LONG_NAME,
    "description": DESCRIPTION,
    "timezone-name": "UTC",
    "location-type": LOCATION_TYPE,
    "location-kind": "PROJECT",
    "nation": "US",
    "state-initial": "NV",
    "county-name": "Clark",
    "nearest-city": "Sparks",
    "horizontal-datum": "WGS84",
    "published-longitude": 0,
    "published-latitude": 0,
    "vertical-datum": "NGVD29",
    "elevation": 150,
    "map-label": MAP_LABEL,
    "bounding-office-id": TEST_OFFICE,
    "elevation-units": "m",
}


def _cleanup():
    wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    wu.delete_water_user(TEST_OFFICE2, TEST_PROJECT_ID, TEST_ENTITY_NAME3)

    pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    _cleanup()

    pl.store_location(PROJECT_LOCATION, False)

    water_user = {
        "entity-name": TEST_ENTITY_NAME,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    water_user2 = {
        "entity-name": TEST_ENTITY_NAME3,
        "project-id": {"office-id": TEST_OFFICE2, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    wu.create_water_user(water_user2, TEST_OFFICE2, TEST_PROJECT_ID, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water user tests...")


def test_store_water_user():
    water_user = {
        "entity-name": TEST_ENTITY_NAME2,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME2)
    assert data["entity-name"] == TEST_ENTITY_NAME2
    assert data["project-id.name"] == TEST_PROJECT_ID
    assert data["project-id.office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT


def test_get_water_user():
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    assert data["entity-name"] == TEST_ENTITY_NAME
    assert data["project-id.name"] == TEST_PROJECT_ID
    assert data["project-id.office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT


def test_delete_water_user():
    water_user = {
        "entity-name": TEST_ENTITY_NAME4,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    assert data["entity-name"] == TEST_ENTITY_NAME4
    assert data["project-id.name"] == TEST_PROJECT_ID
    assert data["project-id.office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT
    wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    assert data is None


def test_get_water_users():
    data = wu.get_water_users(TEST_OFFICE, TEST_PROJECT_ID)
    assert len(data) >= 2
    found_first = False
    found_second = False
    for value in data:
        assert value["project-id.name"] == TEST_PROJECT_ID
        assert value["water-right"] == TEST_WATER_RIGHT
        if value["entity-name"] == TEST_ENTITY_NAME:
            assert value["project-id.office-id"] == TEST_OFFICE
            found_first = True
        if value["entity-name"] == TEST_ENTITY_NAME3:
            assert value["project-id.office-id"] == TEST_OFFICE2
            found_second = True
    assert found_first
    assert found_second


def test_update_water_user():
    water_user = {
        "entity-name": TEST_ENTITY_NAME5,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5)
    assert data["entity-name"] == TEST_ENTITY_NAME5
    assert data["project-id.name"] == TEST_PROJECT_ID
    assert data["project-id.office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT

    new_name = "New Water User"
    water_rights = "Restricted Water Rights"

    updated_user = {
        "entity-name": new_name,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": water_rights,
    }

    wu.update_water_user(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5, updated_user, new_name
    )
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, new_name)
    assert data["entity-name"] == new_name
    assert data["project-id.name"] == TEST_PROJECT_ID
    assert data["project-id.office-id"] == TEST_OFFICE
    assert data["water-right"] == water_rights
