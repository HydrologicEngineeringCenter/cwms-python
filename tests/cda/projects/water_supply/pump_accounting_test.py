from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.projects.projects as proj
import cwms.projects.water_supply.accounting as ac
import cwms.projects.water_supply.water_contracts as wc
import cwms.projects.water_supply.water_users as wu

TEST_OFFICE = "SPK"
TEST_CONTRACT_ID = "Sac River Pumps"
TEST_PROJECT_ID = "Sacramento Delta"
TEST_ENTITY_NAME = "California DWR"
TEST_WATER_RIGHT = "CA Water Rights Permit #12345"
PUMP_LOCATION_ID = "Sac River-Pump N1"
PUMP_LOCATION_ID2 = "Sac River-Pump N2"
PUMP_LOCATION_ID3 = "Sac River-Pump N3"
PUMP_LOCATION_ID4 = "Sac River-Pump N4"
PUMP_LOCATION_ID5 = "Sac River-Pump N5"
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
        "name": PUMP_LOCATION_ID4,
        "timezone-name": "UTC",
    },
    "near-gage-location": {
        "office-id": TEST_OFFICE,
        "name": PUMP_LOCATION_ID5,
        "timezone-name": "UTC",
    },
    "yield-time-frame-start": 1717282800000,
    "yield-time-frame-end": 1717308000000,
    "project-remarks": "Remarks",
}

PUMP_LOCATION1 = {
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

PUMP_LOCATION2 = {
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

PUMP_LOCATION3 = {
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

PUMP_ACCOUNTING = {
    "contract-name": TEST_CONTRACT_ID,
    "water-user": WATER_USER,
    "pump-locations": {
        "pump-in": {"office-id": TEST_OFFICE, "name": PUMP_LOCATION_ID3},
        "pump-out": {"office-id": TEST_OFFICE, "name": PUMP_LOCATION_ID},
        "pump-below": {"office-id": TEST_OFFICE, "name": PUMP_LOCATION_ID2},
    },
    "pump-accounting": {
        "2022-11-20T21:17:28Z": [
            {
                "pump-type": "IN",
                "transfer-type-display": "Conduit",
                "flow": 1.0,
                "flow-unit": "cms",
                "comment": "Added water to the system",
            },
            {
                "pump-type": "OUT",
                "transfer-type-display": "Pipeline",
                "flow": 2.0,
                "flow-unit": "cms",
                "comment": "Removed excess water",
            },
            {
                "pump-type": "BELOW",
                "transfer-type-display": "Pipeline",
                "flow": 3.0,
                "flow-unit": "cms",
                "comment": "Daily water release",
            },
        ],
        "2023-11-21T21:17:28Z": [
            {
                "pump-type": "IN",
                "transfer-type-display": "Pipeline",
                "flow": 4.0,
                "flow-unit": "cms",
                "comment": "Pump transfer for the day",
            },
            {
                "pump-type": "OUT",
                "transfer-type-display": "Pipeline",
                "flow": 5.0,
                "flow-unit": "cms",
                "comment": "Excess water transfer",
            },
            {
                "pump-type": "BELOW",
                "transfer-type-display": "Pipeline",
                "flow": 6.0,
                "flow-unit": "cms",
                "comment": "Water returned to the river",
            },
        ],
        "2024-11-22T21:17:28Z": [
            {
                "pump-type": "IN",
                "transfer-type-display": "Pipeline",
                "flow": 7.0,
                "flow-unit": "cms",
                "comment": "Pump transfer for the day",
            },
            {
                "pump-type": "OUT",
                "transfer-type-display": "Pipeline",
                "flow": 8.0,
                "flow-unit": "cms",
                "comment": "Excess water transfer",
            },
            {
                "pump-type": "BELOW",
                "transfer-type-display": "Pipeline",
                "flow": 9.0,
                "flow-unit": "cms",
                "comment": "Water returned to the river",
            },
        ],
    },
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
        proj.delete_project(TEST_OFFICE, TEST_PROJECT_ID)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID2, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID3, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID4, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION_ID5, TEST_OFFICE)
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

    wu.create_water_user(WATER_USER, False)
    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(PUMP_LOCATION3, False)
    pl.store_location(PUMP_LOCATION4, False)
    pl.store_location(PUMP_LOCATION5, False)
    pl.store_location(PROJECT_LOCATION, False)
    proj.store_project(PROJECT, False)
    wc.create_water_contract(TEST_ENTITY_NAME, WATER_CONTRACT, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water supply accounting tests...")


def test_create_get_accounting():
    ac.store_pump_accounting(
        TEST_OFFICE,
        TEST_PROJECT_ID,
        TEST_ENTITY_NAME,
        TEST_CONTRACT_ID,
        PUMP_ACCOUNTING,
    )
    data = ac.get_pump_accounting(
        TEST_OFFICE,
        TEST_PROJECT_ID,
        TEST_ENTITY_NAME,
        TEST_CONTRACT_ID,
        "2022-11-19T00:00:00Z",
        "2022-11-22T00:00:00Z",
    )
    data = data.json
    assert len(data) > 0
    assert data[0]["contract-name"] == PUMP_ACCOUNTING["contract-name"]
    assert data[0]["water-user"] == PUMP_ACCOUNTING["water-user"]
    assert (
        data[0]["pump-locations"]["pump-in"]["name"]
        == PUMP_ACCOUNTING["pump-locations"]["pump-in"]["name"]
    )
    assert (
        data[0]["pump-locations"]["pump-below"]["name"]
        == PUMP_ACCOUNTING["pump-locations"]["pump-below"]["name"]
    )
    assert (
        data[0]["pump-locations"]["pump-out"]["name"]
        == PUMP_ACCOUNTING["pump-locations"]["pump-out"]["name"]
    )
