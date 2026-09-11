from datetime import datetime

import pytest

import cwms
import cwms.locations.gate_changes as gc
import cwms.locations.location_groups as lg
import cwms.locations.lookups as lk
import cwms.locations.physical_locations as pl
import cwms.outlets.outlets as out
import cwms.projects.projects as proj
from cwms import cwms_types
from cwms.cwms_types import DeleteMethod

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
TEST_LOCATION_ID = "BIGH-CG100"
START = datetime.fromisoformat("2024-01-01")
END = datetime.fromisoformat("2024-01-02")
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
TEST_OUTLET_ID = ""
OUTLET_CATEGORY_ID = "Rating"
OUTLET_GROUP_ID = "Rating-BIGH-TG1"

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
    "elevation-units": "m",
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

OUTLET = {
    "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
    "location": TEST_LOCATION,
    "rating-category-id": {"office-id": TEST_OFFICE, "name": OUTLET_CATEGORY_ID},
    "rating-group-id": {"office-id": TEST_OFFICE, "name": OUTLET_GROUP_ID},
    "rating-spec-id": TEST_PROJECT_ID
    + ".Opening-ConduitGate,Elev;Flow-ConduitGate.Standard.Production",
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
    try:
        proj.delete_project(TEST_OFFICE, TEST_PROJECT_ID, DeleteMethod.DELETE_ALL)
    except Exception:
        pass
    try:
        out.delete_outlet(TEST_OFFICE, TEST_OUTLET_ID, DeleteMethod.DELETE_ALL)
    except Exception:
        pass
    try:
        pl.delete_location(TEST_PROJECT_LOCATION, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(TEST_LOCATION, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION1, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(PUMP_LOCATION2, TEST_OFFICE)
    except Exception:
        pass
    try:
        lk.delete_lookup("E", CATEGORY1, PREFIX1, TEST_OFFICE)
    except Exception:
        pass


def createRatingSpec():
    group = lg.get_location_group(OUTLET_GROUP_ID, OUTLET_CATEGORY_ID, TEST_OFFICE)
    group = group.json
    group["shared-loc-alias-id"] = (
        TEST_PROJECT_ID
        + ".Opening-ConduitGate,Elev;Flow-ConduitGate.Standard.Production"
    )
    lg.delete_location_group(OUTLET_GROUP_ID, OUTLET_CATEGORY_ID, TEST_OFFICE, True)
    lg.store_location_groups(group)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    _cleanup()

    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(TEST_PROJECT_LOCATION, False)
    pl.store_location(TEST_LOCATION, False)
    lk.create_lookup(LOOKUP2, CATEGORY1, PREFIX1)
    proj.store_project(PROJECT, False)
    out.store_outlet(OUTLET, False)
    createRatingSpec()


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for gate change tests...")


def test_create_get_gate_change():
    gc.store_gate_change(GATE_CHANGE, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, TEST_PROJECT_ID, START, END, True, True, "SI", 10
    )
    data = data.json
    found = False
    for item in data:
        if item["change-date"] == GATE_CHANGE[0]["change-date"]:
            found = True
    assert found


def test_catalog_gate_changes():
    GATE_CHANGE2 = GATE_CHANGE
    date = 1704097000000
    GATE_CHANGE2[0]["change-date"] = date
    gc.store_gate_change(GATE_CHANGE2, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, TEST_PROJECT_ID, START, END, True, True, "SI", 10
    )
    data = data.json
    found = False
    assert len(data) >= 1
    for item in data:
        if item["change-date"] == date:
            found = True
    assert found


def test_delete_gate_change():
    GATE_CHANGE2 = GATE_CHANGE
    date = 1704124800000
    GATE_CHANGE2[0]["change-date"] = date
    gc.store_gate_change(GATE_CHANGE2, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, TEST_PROJECT_ID, START, END, True, True, "SI", 10
    )
    data = data.json
    found = False
    assert len(data) >= 1
    for item in data:
        print(item)
        if item["change-date"] == date:
            found = True
    assert found
    gc.delete_gate_change(TEST_OFFICE, TEST_PROJECT_ID, START, END, True)
    found = False
    try:
        data = gc.get_all_gate_changes(
            TEST_OFFICE, TEST_PROJECT_ID, START, END, True, True, "SI", 10
        )
    except Exception:
        pass
