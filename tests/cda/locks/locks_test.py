from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.locks.locks as lk
import cwms.projects.projects as proj

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
LOCK_ID = "pytest-lock123"
PUMP_LOCATION_ID = "Sac River-Pump 1"
PUMP_LOCATION_ID2 = "Sac River-Pump 2"
PUBLIC_NAME = "Test Public Pump Name"
LONG_NAME = "Test Long Name"
LOCATION_TYPE = "Test Location Type"
DESCRIPTION = "Test Description"
MAP_LABEL = "Test Map Label"

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
        lk.delete_lock(LOCK_ID, TEST_OFFICE)
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
        pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE)
    except Exception:
        pass
    try:
        pl.delete_location(LOCK_ID, TEST_OFFICE)
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
    # pl.store_location(TEST_LOCK_LOCATION, False)
    pl.store_location(TEST_PROJECT_LOCATION, False)
    proj.store_project(PROJECT, False)
    lk.create_lock(TEST_LOCK, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water supply accounting tests...")


def test_get_locks():
    locks = lk.get_locks(TEST_OFFICE, TEST_PROJECT_ID)
    assert locks is not None


def test_get_lock():
    lock = lk.get_lock(LOCK_ID, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    assert lock["lock-width"] == TEST_LOCK["lock-width"]
    assert lock["location"]["name"] == TEST_LOCK["location"]["name"]
    assert lock["project-id"] == TEST_LOCK["project-id"]


def test_create_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock879"
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    assert lock["lock-width"] == test_lock2["lock-width"]
    assert lock["location"]["name"] == test_lock2["location"]["name"]
    assert lock["project-id"] == test_lock2["project-id"]


def test_delete_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock456"
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    assert lock["lock-width"] == test_lock2["lock-width"]
    assert lock["location"]["name"] == test_lock2["location"]["name"]
    assert lock["project-id"] == test_lock2["project-id"]
    lk.delete_lock(new_loc, TEST_OFFICE)
    try:
        lock = lk.get_lock(new_loc, TEST_OFFICE)
    except Exception:
        found = False
    assert not found


def test_update_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock881"
    test_lock2["location"]["name"] = new_loc
    test_lock2["location"]["description"] = "pytest-lock-description"
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    assert lock["lock-width"] == test_lock2["lock-width"]
    assert lock["location"]["name"] == test_lock2["location"]["name"]
    assert lock["project-id"] == test_lock2["project-id"]
    updated_loc = "pytest-lock996"
    lk.update_lock(LOCK_ID, TEST_OFFICE, updated_loc)
    lock = lk.get_lock(updated_loc, TEST_OFFICE)
    assert lock is not None
    lock = lock.json
    assert lock["lock-width"] == test_lock2["lock-width"]
    assert lock["location"]["name"] == updated_loc
    assert lock["project-id"] == test_lock2["project-id"]
