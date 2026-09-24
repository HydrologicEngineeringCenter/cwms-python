from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.projects.projects as proj
import cwms.projects.water_supply.water_users as wu
from tests._test_utils import read_resource_file

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "pytest_wu"
TEST_ENTITY_NAME = "Test User"
TEST_WATER_RIGHT = "Test Water Right"
TEST_ENTITY_NAME2 = "Test User 2"
TEST_ENTITY_NAME3 = "Test User 3"
TEST_ENTITY_NAME4 = "Test User 4"
TEST_ENTITY_NAME5 = "Test User 5"
TEST_ENTITY_NAME6 = "California DWR"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"
PUMP_LOCATION_ID = "Sac River-Pump 1"
PUMP_LOCATION_ID2 = "Sac River-Pump 2"
PUMP_LOCATION_ID3 = "Sac River-Pump 3"

PROJECT_LOCATION = read_resource_file("project_location.json")
PROJECT_LOCATION["office-id"] = TEST_OFFICE
PROJECT_LOCATION["name"] = TEST_PROJECT_ID
PROJECT_LOCATION["public-name"] = PUBLIC_NAME
PROJECT_LOCATION["long-name"] = LONG_NAME
PROJECT_LOCATION["map-label"] = MAP_LABEL
PROJECT_LOCATION["bounding-office-id"] = TEST_OFFICE
PROJECT_LOCATION["location-type"] = LOCATION_TYPE
PROJECT_LOCATION["description"] = DESCRIPTION

WATER_USER = read_resource_file("water_user.json")
WATER_USER["entity-name"] = TEST_ENTITY_NAME
WATER_USER["project-id"]["name"] = TEST_PROJECT_ID
WATER_USER["project-id"]["office-id"] = TEST_OFFICE
WATER_USER["water-right"] = TEST_WATER_RIGHT

WATER_USER1 = WATER_USER
WATER_USER1["entity-name"] = TEST_ENTITY_NAME2

WATER_USER2 = WATER_USER1
WATER_USER2["entity-name"] = TEST_ENTITY_NAME3

WATER_USER3 = WATER_USER1
WATER_USER3["entity-name"] = TEST_ENTITY_NAME4

WATER_USER4 = WATER_USER3
WATER_USER4["entity-name"] = TEST_ENTITY_NAME5

WATER_USER5 = WATER_USER4
WATER_USER5["entity-name"] = TEST_ENTITY_NAME6

PROJECT = read_resource_file("water_project.json")
PROJECT["location"]["office-id"] = TEST_OFFICE
PROJECT["location"]["name"] = TEST_PROJECT_ID
PROJECT["pump-back-location"]["name"] = PUMP_LOCATION_ID
PROJECT["pump-back-location"]["office-id"] = TEST_OFFICE
PROJECT["near-gage-location"]["name"] = PUMP_LOCATION_ID2
PROJECT["near-gage-location"]["office-id"] = TEST_OFFICE

PUMP_LOCATION1 = PROJECT_LOCATION
PUMP_LOCATION1["name"] = PUMP_LOCATION_ID2
PUMP_LOCATION1["location-kind"] = "PUMP"

PUMP_LOCATION2 = PUMP_LOCATION1
PUMP_LOCATION2["name"] = PUMP_LOCATION_ID3

PUMP_LOCATION3 = PUMP_LOCATION2
PUMP_LOCATION3["name"] = PUMP_LOCATION_ID


def _cleanup():
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME3)
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME2)
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5)
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME6)
    except Exception:
        pass
    try:
        proj.delete_project(TEST_PROJECT_ID, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE)
    except Exception:
        pass


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass

    pl.store_location(PROJECT_LOCATION, False)
    proj.store_project(PROJECT, False)

    wu.create_water_user(WATER_USER, False)
    wu.create_water_user(WATER_USER2, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water user tests...")


def test_store_water_user():
    wu.create_water_user(WATER_USER1, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME2)
    data = data.json
    _assert_match(data, TEST_ENTITY_NAME2)


def test_get_water_user():
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    data = data.json
    _assert_match(data, TEST_ENTITY_NAME)


def test_delete_water_user():
    wu.create_water_user(WATER_USER3, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    data = data.json
    _assert_match(data, TEST_ENTITY_NAME4)
    wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    with pytest.raises(cwms.ApiError):
        wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)


def test_get_water_users():
    data = wu.get_water_users(TEST_OFFICE, TEST_PROJECT_ID)
    assert len(data.json) >= 2
    found_first = False
    found_second = False
    for value in data.json:
        _assert_match(value, null)
        if value["entity-name"] == TEST_ENTITY_NAME:
            found_first = True
        if value["entity-name"] == TEST_ENTITY_NAME3:
            found_second = True
    assert found_first
    assert found_second


def test_update_water_user():
    wu.create_water_user(WATER_USER4, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5)
    data = data.json
    _assert_match(data, TEST_ENTITY_NAME5)

    wu.update_water_user(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5, WATER_USER5, TEST_ENTITY_NAME6
    )
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME6)
    data = data.json
    _assert_match(data, TEST_ENTITY_NAME6)


def _assert_match(data, name):
    if name is not null:
        assert data["entity-name"] == name
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT
