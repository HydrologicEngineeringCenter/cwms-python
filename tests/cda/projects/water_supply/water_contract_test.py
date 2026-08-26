from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.projects.water_supply.water_contracts as wc
import cwms.projects.water_supply.water_users as wu

TEST_OFFICE = "SPK"
PUMP_LOCATION_ID = "Sac River-Pump 1"
PUMP_LOCATION_ID2 = "Sac River-Pump 2"
PUMP_LOCATION_ID3 = "Sac River-Pump 3"
TEST_CONTRACT_ID = "Sac River Pumps"
TEST_PROJECT_ID = "Sacramento Delta"
TEST_ENTITY_NAME = "California DWR"
TEST_WATER_RIGHT = "CA Water Rights Permit #12345"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"

WATER_USER = {
    "entity-name": TEST_ENTITY_NAME,
    "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
    "water-right": TEST_WATER_RIGHT,
}

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

WATER_CONTRACT = {
    "office-id": TEST_OFFICE,
    "water-user": WATER_USER,
    "contract-id": {"office-id": TEST_OFFICE, "name": TEST_CONTRACT_ID},
    "contract-type": {
        "office-id": TEST_OFFICE,
        "display-value": "Test Display Value",
        "tooltip": "Test Tooltip",
        "active": True,
    },
    "contract-effective-date": 158000,
    "contract-expiration-date": 167000,
    "contracted-storage": 200000.5,
    "initial-use-allocation": 15600,
    "future-use-allocation": 27800.5,
    "storage-units-id": "m3",
    "future-use-percent-activated": 15.6,
    "total-alloc-percent-activated": 65.2,
    "pump-out-location": {"pump-location": PUMP_LOCATION1, "pump-type": "OUT"},
    "pump-out-below-location": {"pump-location": PUMP_LOCATION2, "pump-type": "BELOW"},
    "pump-in-location": {"pump-location": PUMP_LOCATION3, "pump-type": "IN"},
}


def _cleanup():
    wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    pl.delete_location(PUMP_LOCATION_ID, TEST_OFFICE)
    pl.delete_location(PUMP_LOCATION_ID2, TEST_OFFICE)
    pl.delete_location(PUMP_LOCATION_ID3, TEST_OFFICE)
    pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass

    pl.store_location(PROJECT_LOCATION, False)
    wu.create_water_user(WATER_USER, TEST_OFFICE, TEST_PROJECT_ID, False)
    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(PUMP_LOCATION3, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water contract tests...")


def test_store_water_contract():
    wc.create_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, WATER_CONTRACT, False
    )
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, TEST_CONTRACT_ID
    )
    assert data == WATER_CONTRACT


def test_get_water_contract():
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, TEST_CONTRACT_ID
    )
    assert data == WATER_CONTRACT


def test_delete_water_contract():
    WATER_CONTRACT2 = WATER_CONTRACT
    new_contract_name = "Temporary Contract"
    WATER_CONTRACT2["contract-id"]["name"] = new_contract_name
    wc.create_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, WATER_CONTRACT2, False
    )
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    assert data == WATER_CONTRACT2
    wc.delete_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    assert data is None


def test_get_water_contracts():
    WATER_CONTRACT2 = WATER_CONTRACT
    WATER_CONTRACT2["contract-id"]["name"] = "Addendum Contract"
    wc.create_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, WATER_CONTRACT2, False
    )
    data = wc.get_water_contracts(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    assert len(data) == 2
    for item in data:
        if item["contract-id"]["name"] == TEST_CONTRACT_ID:
            assert item == WATER_CONTRACT
        elif item["contract-id"]["name"] == "Addendum Contract":
            assert item == WATER_CONTRACT2
        else:
            assert False, "Unexpected contract found in list"


def test_update_water_contract():
    WATER_CONTRACT2 = WATER_CONTRACT
    new_contract_name = "Additional Contract"
    WATER_CONTRACT2["contract-id"]["name"] = new_contract_name
    WATER_CONTRACT2["future-use-percent-activated"] = 225.6
    wc.update_water_contract(
        TEST_OFFICE,
        TEST_PROJECT_ID,
        TEST_ENTITY_NAME,
        TEST_CONTRACT_ID,
        new_contract_name,
        WATER_CONTRACT2,
    )
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    assert data == WATER_CONTRACT2
