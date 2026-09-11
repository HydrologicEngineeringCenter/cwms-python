from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.locks.locks as lk
import cwms.projects.projects as proj
from cwms import DeleteMethod

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
LOCK_ID = "pytest2lock14"
PUMP_LOCATION_ID = "Sac River-Pump 1"
PUMP_LOCATION_ID2 = "Sac River-Pump 2"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "SITE"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"
NEW_LOCK1 = "pytestlock881"
NEW_LOCK2 = "pytestlock996"
NEW_LOCK3 = "pytestlock879"

TEST_LOCK_LOCATION = {
    "office-id": TEST_OFFICE,
    "name": LOCK_ID,
    "latitude": 38.5,
    "longitude": -121.7,
    "active": True,
    "public-name": LOCK_ID,
    "long-name": "TEST_LOCATION",
    "description": "for testing",
    "timezone-name": "UTC",
    "location-type": "SITE",
    "location-kind": "LOCK",
    "nation": "US",
    "state-initial": "CA",
    "county-name": "Sacramento",
    "horizontal-datum": "NGVD29",
    "published-longitude": 38.5,
    "published-latitude": -121.7,
    "elevation": 10.0,
    "elevation-units": "m",
    "bounding-office-id": TEST_OFFICE,
    "nearest-city": "Davis",
}

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

TEST_LOCK = {
    "project-id": {"office-id": TEST_OFFICE, "name": TEST_PROJECT_ID},
    "location": TEST_LOCK_LOCATION,
    "chamber-type": {
        "display-value": "Single Chamber",
        "tooltip": "A lock gate system with a single chamber",
        "active": True,
        "office-id": "CWMS",
    },
    "lock-width": 50.0,
    "lock-length": 50.0,
    "normal-lock-lift": 10.0,
    "volume-per-lockage": 10.0,
    "minimum-draft": 25.5,
    "maximum-lock-lift": 25.6,
    "length-units": "m",
    "volume-units": "m3",
    "elevation-units": "m",
    "high-water-upper-pool-warning-level": 2.0,
    "high-water-lower-pool-warning-level": 2.0,
}

PUMP_LOCATION1 = TEST_LOCK_LOCATION
PUMP_LOCATION1["name"] = PUMP_LOCATION_ID
PUMP_LOCATION1["public-name"] = PUBLIC_NAME
PUMP_LOCATION1["long-name"] = LONG_NAME
PUMP_LOCATION1["description"] = DESCRIPTION
PUMP_LOCATION1["location-type"] = LOCATION_TYPE
PUMP_LOCATION1["location-kind"] = "PUMP"

PUMP_LOCATION2 = PUMP_LOCATION1
PUMP_LOCATION2["name"] = PUMP_LOCATION_ID2

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


def _cleanup():
    try:
        pl.delete_location(LOCK_ID, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        lk.delete_lock(LOCK_ID, TEST_OFFICE, DeleteMethod.DELETE_ALL)
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
        pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(NEW_LOCK1, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(NEW_LOCK2, TEST_OFFICE, True)
    except Exception:
        pass
    try:
        pl.delete_location(NEW_LOCK3, TEST_OFFICE, True)
    except Exception:
        pass


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass
    pl.store_location(PUMP_LOCATION1, False)
    pl.store_location(PUMP_LOCATION2, False)
    pl.store_location(TEST_LOCK_LOCATION, False)
    pl.store_location(TEST_PROJECT_LOCATION, False)
    proj.store_project(PROJECT, False)
    lk.create_lock(TEST_LOCK, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water supply accounting tests...")


def test_get_locks():
    locks = lk.get_locks(TEST_OFFICE, TEST_PROJECT_ID)
    assert locks is not None
    data = locks.json
    assert len(data) > 0
    found = False
    for lock in data:
        if lock["location"]["name"] == LOCK_ID:
            found = True
            assert lock["project-id"] == TEST_LOCK["project-id"]
    assert found


def test_get_lock():
    lock = lk.get_lock(LOCK_ID, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    _assert_match(TEST_LOCK, lock)


def test_create_lock():
    test_lock2 = TEST_LOCK
    new_loc = NEW_LOCK3
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    _assert_match(test_lock2, lock)
    lk.delete_lock(new_loc, TEST_OFFICE)
    try:
        lock = lk.get_lock(new_loc, TEST_OFFICE)
    except Exception:
        found = False
    assert not found


def test_delete_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock456"
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    _assert_match(test_lock2, lock)
    lk.delete_lock(new_loc, TEST_OFFICE)
    try:
        lock = lk.get_lock(new_loc, TEST_OFFICE)
    except Exception:
        found = False
    assert not found


def test_update_lock():
    test_lock2 = TEST_LOCK
    new_loc = NEW_LOCK1
    test_lock2["location"]["name"] = new_loc
    test_lock2["location"]["description"] = "pytest-lock-description"
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    _assert_match(test_lock2, lock)
    updated_loc = NEW_LOCK2
    lk.update_lock(LOCK_ID, TEST_OFFICE, updated_loc)
    lock = lk.get_lock(updated_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    _assert_match(test_lock2, lock)
    lk.delete_lock(updated_loc, TEST_OFFICE)


def _assert_match(expected, actual):
    assert expected["lock-width"] == actual["lock-width"]
    assert expected["lock-length"] == actual["lock-length"]
    assert expected["normal-lock-lift"] == actual["normal-lock-lift"]
    assert expected["volume-per-lockage"] == actual["volume-per-lockage"]
    assert expected["minimum-draft"] == actual["minimum-draft"]
    assert expected["maximum-lock-lift"] == actual["maximum-lock-lift"]
    assert expected["length-units"] == actual["length-units"]
    assert expected["volume-units"] == actual["volume-units"]
    assert expected["elevation-units"] == actual["elevation-units"]
    assert expected["chamber-type"] == actual["chamber-type"]
    assert expected["location"]["name"] == actual["location"]["name"]
    assert expected["project-id"] == actual["project-id"]
