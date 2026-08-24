import pytest

import cwms
import cwms.locations.gate_changes as gc
import cwms.locations.physical_locations as pl

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
TEST_LOCATION_ID = "BIGH-CG100"
START = "2024-01-01T00:00:00Z"
END = "2024-01-02T00:00:00Z"

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

GATE_CHANGE = {
    "type": "gate-change",
    "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
    "change-date": 1704096000000,
    "pool-elevation": 3.0,
    "protected": True,
    "discharge-computation-type": {
        "office-id": TEST_OFFICE,
        "display-value": "A",
        "tooltip": "Adjusted by an automated method",
        "active": True,
    },
    "reason-type": {
        "office-id": TEST_OFFICE,
        "display-value": "O",
        "tooltip": "Other release",
        "active": True,
    },
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


def _cleanup():
    pl.delete_location(TEST_PROJECT_LOCATION, TEST_OFFICE)
    pl.delete_location(TEST_LOCATION, TEST_OFFICE)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    _cleanup()

    pl.store_location(TEST_PROJECT_LOCATION, False)
    pl.store_location(TEST_LOCATION, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for gate change tests...")


def test_create_get_gate_change():
    gc.store_gate_change(GATE_CHANGE, False)
    data = gc.get_all_gate_changes(
        TEST_OFFICE, TEST_PROJECT_ID, "", "", True, True, "SI", 10
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
    data = gc.get_all_gate_changes(TEST_OFFICE, new_loc, "", "", True, True, "SI", 10)
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
    assert len(data) == 0
