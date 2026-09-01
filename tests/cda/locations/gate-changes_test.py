import pytest

import cwms
import cwms.locations.gate_changes as gc
import cwms.locations.lookups as lk
import cwms.locations.physical_locations as pl
import cwms.projects.projects as proj
from cwms import cwms_types
from cwms.cwms_types import DeleteMethod

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
TEST_LOCATION_ID = "BIGH-CG100"
START = "2024-01-01T00:00:00Z"
END = "2024-01-02T00:00:00Z"
PUMP_LOCATION_ID = "Sac River-Pump 1"
PUMP_LOCATION_ID2 = "Sac River-Pump 2"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"
CATEGORY = "AT_GATE_CH_COMPUTATION_CODE"
PREFIX = "DISCHARGE_COMP"
CATEGORY1 = "AT_GATE_RELEASE_REASON_CODE"
PREFIX1 = "RELEASE_REASON"

TEST_PROJECT_LOCATION = {
    "name": TEST_PROJECT_ID,
    "latitude": 40.0,
    "longitude": -105.0,
    "elevation": 1000.0,
    "horizontal-datum": "NAD83",
    "vertical-datum": "NAVD88",
    "office-id": TEST_OFFICE,
    "location-type": "TESTING",
    "location-kind": "PROJECT",
    "public-name": "Test Location",
    "long-name": "A pytest-generated location",
    "timezone-name": "America/Chicago",
    "nation": "US",
}

TEST_LOCATION = {
    "name": TEST_LOCATION_ID,
    "latitude": 40.0,
    "longitude": -105.0,
    "elevation": 1000.0,
    "horizontal-datum": "NAD83",
    "vertical-datum": "NAVD88",
    "office-id": TEST_OFFICE,
    "location-type": "TESTING",
    "location-kind": "SITE",
    "public-name": "Test Location",
    "long-name": "A pytest-generated location",
    "timezone-name": "America/Chicago",
    "nation": "US",
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

LOOKUP1 = {
    "office-id": TEST_OFFICE,
    "display-value": "A",
    "tooltip": "Adjusted by an automated method",
    "active": True,
}

LOOKUP2 = {
    "office-id": TEST_OFFICE,
    "display-value": "E",
    "tooltip": "Estimated by user",
    "active": True,
}

GATE_CHANGE = [
    {
        "type": "gate-change",
        "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
        "change-date": 1704096000000,
        "pool-elevation": 3.0,
        "protected": True,
        "discharge-computation-type": LOOKUP1,
        "reason-type": LOOKUP2,
        "notes": "Test notes",
        "new-total-discharge-override": 1.0,
        "old-total-discharge-override": 2.0,
        "discharge-units": "cfs",
        "tailwater-elevation": 4.0,
        "elevation-units": "ft",
        "settings": [
            {
                "type": "gate-setting",
                "location-id": {"office-id": TEST_OFFICE, "name": TEST_LOCATION_ID},
                "opening": 10.0,
                "opening-parameter": "Opening",
                "opening-units": "ft",
                "invert-elevation": 20.0,
            }
        ],
    }
]

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
        "office-id": "SPK",
        "name": PUMP_LOCATION_ID2,
        "timezone-name": "UTC",
    },
    "yield-time-frame-start": 1717282800000,
    "yield-time-frame-end": 1717308000000,
    "project-remarks": "Remarks",
}


def _cleanup():
    proj.delete_project(TEST_OFFICE, TEST_PROJECT_ID, DeleteMethod.DELETE_ALL)
    pl.delete_location(TEST_PROJECT_LOCATION, TEST_OFFICE)
    pl.delete_location(TEST_LOCATION, TEST_OFFICE)
    pl.delete_location(PUMP_LOCATION1, TEST_OFFICE)
    pl.delete_location(PUMP_LOCATION2, TEST_OFFICE)
    lk.delete_lookup(CATEGORY, PREFIX, TEST_OFFICE)
    lk.delete_lookup(CATEGORY1, PREFIX1, TEST_OFFICE)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass

    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(TEST_PROJECT_LOCATION, False)
    pl.store_location(TEST_LOCATION, False)
    lk.create_lookup(LOOKUP1, CATEGORY, PREFIX)
    lk.create_lookup(LOOKUP2, CATEGORY1, PREFIX1)
    proj.store_project(PROJECT, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for gate change tests...")


def test_create_get_gate_change():
    gc.store_gate_change(GATE_CHANGE, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, TEST_PROJECT_ID, START, END, True, True, "SI", 10
    )
    found = False
    for item in data:
        if data == item:
            found = True
    assert found


def test_catalog_gate_changes():
    GATE_CHANGE2 = GATE_CHANGE
    new_loc = TEST_LOCATION_ID + "_new"
    GATE_CHANGE2["project-id"]["name"] = new_loc
    gc.store_gate_change(GATE_CHANGE2, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, new_loc, START, END, True, True, "SI", 10
    )
    found = False
    assert len(data) >= 1
    for item in data:
        if data == item:
            found = True
    assert found


def test_delete_gate_change():
    GATE_CHANGE2 = GATE_CHANGE
    new_loc = TEST_LOCATION_ID + "_new2"
    GATE_CHANGE2["project-id"]["name"] = new_loc
    gc.store_gate_change(GATE_CHANGE2, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, new_loc, START, END, True, True, "SI", 10
    )
    found = False
    assert len(data) >= 1
    for item in data:
        if data == item:
            found = True
    assert found
    gc.delete_gate_change(TEST_OFFICE, TEST_PROJECT_ID, START, END)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, new_loc, START, END, True, True, "SI", 10
    )
    assert len(data.json) == 0
