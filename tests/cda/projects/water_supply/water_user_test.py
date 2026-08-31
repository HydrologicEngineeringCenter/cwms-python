from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.projects.projects as proj
import cwms.projects.water_supply.water_users as wu

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

PROJECT = {
    "location": {
        "office-id": TEST_OFFICE,
        "name": TEST_PROJECT_ID,
        "timezone-name": "UTC",
    },
    "federal-cost": 100.0,
    "non-federal-cost": 50.0,
    "cost-year": 1717282800000,
    "cost-unit": "$",
    "federal-o-and-m-cost": 10.0,
    "non-federal-o-and-m-cost": 5.0,
    "authorizing-law": "Authorizing Law",
    "project-owner": "Project Owner",
    "hydropower-desc": "Hydropower Description",
    "sedimentation-desc": "Sedimentation Description",
    "downstream-urban-desc": "Downstream Urban Description",
    "bank-full-capacity-desc": "Bank Full Capacity Description",
    "pump-back-location": {
        "office-id": TEST_OFFICE,
        "name": PUMP_LOCATION_ID,
        "timezone-name": "UTC",
    },
    "near-gage-location": {
        "office-id": TEST_OFFICE,
        "name": PUMP_LOCATION_ID2,
        "timezone-name": "UTC",
    },
    "yield-time-frame-start": 1717282800000,
    "yield-time-frame-end": 1717308000000,
    "project-remarks": "Remarks",
}

PUMP_LOCATION1 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID2,
    "latitude": 0,
    "longitude": 0,
    "active": True,
    "public-name": PUBLIC_NAME,
    "long-name": LONG_NAME,
    "description": DESCRIPTION,
    "timezone-name": "UTC",
    "location-type": LOCATION_TYPE,
    "location-kind": "PUMP",
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

PUMP_LOCATION2 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID3,
    "latitude": 0,
    "longitude": 0,
    "active": True,
    "public-name": PUBLIC_NAME,
    "long-name": LONG_NAME,
    "description": DESCRIPTION,
    "timezone-name": "UTC",
    "location-type": LOCATION_TYPE,
    "location-kind": "PUMP",
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

PUMP_LOCATION3 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID,
    "latitude": 0,
    "longitude": 0,
    "active": True,
    "public-name": PUBLIC_NAME,
    "long-name": LONG_NAME,
    "description": DESCRIPTION,
    "timezone-name": "UTC",
    "location-type": LOCATION_TYPE,
    "location-kind": "PUMP",
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

    water_user = {
        "entity-name": TEST_ENTITY_NAME,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    water_user2 = {
        "entity-name": TEST_ENTITY_NAME3,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    wu.create_water_user(water_user2, TEST_OFFICE, TEST_PROJECT_ID, False)


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
    data = data.json
    assert data["entity-name"] == TEST_ENTITY_NAME2
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT


def test_get_water_user():
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    data = data.json
    assert data["entity-name"] == TEST_ENTITY_NAME
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT


def test_delete_water_user():
    water_user = {
        "entity-name": TEST_ENTITY_NAME4,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": TEST_WATER_RIGHT,
    }

    wu.create_water_user(water_user, TEST_OFFICE, TEST_PROJECT_ID, False)
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    data = data.json
    assert data["entity-name"] == TEST_ENTITY_NAME4
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT
    wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)
    with pytest.raises(cwms.ApiError):
        wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME4)


def test_get_water_users():
    data = wu.get_water_users(TEST_OFFICE, TEST_PROJECT_ID)
    assert len(data.json) >= 2
    found_first = False
    found_second = False
    for value in data.json:
        assert value["project-id"]["name"] == TEST_PROJECT_ID
        assert value["water-right"] == TEST_WATER_RIGHT
        if value["entity-name"] == TEST_ENTITY_NAME:
            found_first = True
        if value["entity-name"] == TEST_ENTITY_NAME3:
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
    data = data.json
    assert data["entity-name"] == TEST_ENTITY_NAME5
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == TEST_WATER_RIGHT

    water_rights = "Restricted Water Rights"

    updated_user = {
        "entity-name": TEST_ENTITY_NAME6,
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "water-right": water_rights,
    }

    wu.update_water_user(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME5, updated_user, TEST_ENTITY_NAME6
    )
    data = wu.get_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME6)
    data = data.json
    assert data["entity-name"] == TEST_ENTITY_NAME6
    assert data["project-id"]["name"] == TEST_PROJECT_ID
    assert data["project-id"]["office-id"] == TEST_OFFICE
    assert data["water-right"] == water_rights
