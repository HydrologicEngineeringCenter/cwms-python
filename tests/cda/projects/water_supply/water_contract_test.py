from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.lookups as lk
import cwms.locations.physical_locations as pl
import cwms.projects.projects as proj
import cwms.projects.water_supply.water_contracts as wc
import cwms.projects.water_supply.water_users as wu
from tests.cda.projects.water_supply.water_user_test import PUMP_LOCATION_ID3

TEST_OFFICE = "SPK"
PUMP_LOCATION_ID = "Sac River-Pump Num 1"
PUMP_LOCATION_ID2 = "Sac River-Pump Num 2"
PUMP_LOCATION_ID3 = "Sac River-Pump Num 3"
PUMP_LOCATION_ID4 = "Sac River-Pump Num 4"
PUMP_LOCATION_ID5 = "Sac River-Pump Num 5"
PUMP_LOCATION_ID6 = "Sac River-Pump Num 6"
PUMP_LOCATION_ID7 = "Sac River-Pump Num 7"
PUMP_LOCATION_ID8 = "Sac River-Pump Num 8"
PUMP_LOCATION_ID9 = "Sac River-Pump Num 9"
TEST_CONTRACT_ID = "Sac River Pumps"
TEST_PROJECT_ID = "Sacramento Delta"
TEST_ENTITY_NAME = "California DWR"
TEST_WATER_RIGHT = "CA Water Rights Permit #12345"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"
LOOKUP_CATEGORY = "AT_WS_CONTRACT_TYPE"
LOOKUP_PREFIX = "WS_CONTRACT_TYPE"

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
        "office-id": "SPK",
        "name": PUMP_LOCATION_ID,
        "timezone-name": "UTC",
    },
    "near-gage-location": {
        "office-id": "SPK",
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

PUMP_LOCATION4 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID4,
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

PUMP_LOCATION5 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID5,
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

PUMP_LOCATION6 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID6,
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

PUMP_LOCATION7 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID7,
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

PUMP_LOCATION8 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID8,
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

PUMP_LOCATION9 = {
    "office-id": TEST_OFFICE,
    "name": PUMP_LOCATION_ID9,
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

CONTRACT_LOOKUP = {
    "office-id": TEST_OFFICE,
    "display-value": "Test Display Value",
    "tooltip": "Test Tooltip",
    "active": True,
}

WATER_CONTRACT = {
    "office-id": TEST_OFFICE,
    "water-user": WATER_USER,
    "contract-id": {"office-id": TEST_OFFICE, "name": TEST_CONTRACT_ID},
    "contract-type": CONTRACT_LOOKUP,
    "contract-effective-date": 1717282800000,
    "contract-expiration-date": 1717282800000,
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
    try:
        wc.delete_water_contract(
            TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, TEST_CONTRACT_ID
        )
    except Exception:
        pass
    try:
        wu.delete_water_user(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    except Exception:
        pass
    try:
        proj.delete_project(TEST_PROJECT_ID, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID2, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID3, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID4, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID5, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID6, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID7, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID8, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID9, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        lk.delete_lookup(CONTRACT_LOOKUP, LOOKUP_CATEGORY, LOOKUP_PREFIX, TEST_OFFICE)
    except Exception:
        pass


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass

    pl.store_location(PROJECT_LOCATION, False)
    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(PUMP_LOCATION3, False)
    pl.store_location(PUMP_LOCATION4, False)
    pl.store_location(PUMP_LOCATION5, False)
    pl.store_location(PUMP_LOCATION6, False)
    pl.store_location(PUMP_LOCATION7, False)
    pl.store_location(PUMP_LOCATION8, False)
    pl.store_location(PUMP_LOCATION9, False)
    proj.store_project(PROJECT, False)
    wu.create_water_user(WATER_USER, False)
    lk.create_lookup(CONTRACT_LOOKUP, LOOKUP_CATEGORY, LOOKUP_PREFIX)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water contract tests...")


def test_store_get_water_contract():
    wc.create_water_contract(TEST_ENTITY_NAME, WATER_CONTRACT, False)
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, TEST_CONTRACT_ID
    )
    data = data.json
    assert data["contract-id"]["name"] == WATER_CONTRACT["contract-id"]["name"]
    assert data["office-id"] == WATER_CONTRACT["office-id"]
    assert (
        data["water-user"]["entity-name"] == WATER_CONTRACT["water-user"]["entity-name"]
    )


def test_delete_water_contract():
    WATER_CONTRACT2 = WATER_CONTRACT
    new_contract_name = "Temporary Contract"
    WATER_CONTRACT2["contract-id"]["name"] = new_contract_name
    WATER_CONTRACT2["pump-out-location"]["pump-location"] = PUMP_LOCATION4
    WATER_CONTRACT2["pump-in-location"]["pump-location"] = PUMP_LOCATION5
    WATER_CONTRACT2["pump-out-below-location"]["pump-location"] = PUMP_LOCATION6
    wc.create_water_contract(TEST_ENTITY_NAME, WATER_CONTRACT2, False)
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    data = data.json
    assert data["contract-id"]["name"] == WATER_CONTRACT2["contract-id"]["name"]
    assert data["office-id"] == WATER_CONTRACT2["office-id"]
    assert (
        data["water-user"]["entity-name"]
        == WATER_CONTRACT2["water-user"]["entity-name"]
    )
    wc.delete_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    found = True
    try:
        data = wc.get_water_contract(
            TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
        )
    except Exception:
        found = False
    assert not found


def test_get_water_contracts():
    WATER_CONTRACT2 = WATER_CONTRACT
    new_contract_name = "Addendum Contract"
    WATER_CONTRACT2["contract-id"]["name"] = new_contract_name
    WATER_CONTRACT2["pump-out-location"]["pump-location"] = PUMP_LOCATION7
    WATER_CONTRACT2["pump-in-location"]["pump-location"] = PUMP_LOCATION8
    WATER_CONTRACT2["pump-out-below-location"]["pump-location"] = PUMP_LOCATION9
    wc.create_water_contract(TEST_ENTITY_NAME, WATER_CONTRACT2, False)
    data = wc.get_water_contracts(TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME)
    data = data.json
    assert len(data) > 0
    found = False
    for item in data:
        if item["contract-id"]["name"] == new_contract_name:
            assert item["contract-id"]["name"] == WATER_CONTRACT2["contract-id"]["name"]
            assert item["office-id"] == WATER_CONTRACT2["office-id"]
            assert (
                item["water-user"]["entity-name"]
                == WATER_CONTRACT2["water-user"]["entity-name"]
            )
            found = True
    assert found


def test_update_water_contract():
    wc.create_water_contract(TEST_ENTITY_NAME, WATER_CONTRACT, False)
    WATER_CONTRACT2 = WATER_CONTRACT
    new_contract_name = "Additional Contract"
    WATER_CONTRACT2["future-use-percent-activated"] = 225.6
    wc.update_water_contract(TEST_CONTRACT_ID, new_contract_name, WATER_CONTRACT2)
    data = wc.get_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
    data = data.json
    assert data["contract-id"]["name"] == new_contract_name
    assert data["office-id"] == WATER_CONTRACT2["office-id"]
    assert (
        data["water-user"]["entity-name"]
        == WATER_CONTRACT2["water-user"]["entity-name"]
    )
    wc.delete_water_contract(
        TEST_OFFICE, TEST_PROJECT_ID, TEST_ENTITY_NAME, new_contract_name
    )
