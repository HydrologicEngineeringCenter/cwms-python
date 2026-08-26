from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

import cwms
import cwms.locations.physical_locations as pl
import cwms.locks.locks as lk

TEST_OFFICE = "SPK"
TEST_PROJECT_ID = "BIGH"
LOCK_ID = "pytest-lock-123"

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
    "project-id": {"office-id": TEST_OFFICE, "name": "PROJECT"},
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
    "length-units": "ft",
    "volume-units": "ft3",
    "elevation-units": "ft",
    "high-water-upper-pool-location-level": {
        "level-value": 15.96,
        "level-link": "/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Upper Pool?office=SPK",
    },
    "high-water-lower-pool-location-level": {
        "level-value": 22.7,
        "level-link": "/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Lower Pool?office=SPK",
    },
    "low-water-upper-pool-location-level": {
        "level-value": 18.0,
        "level-link": "/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Upper Pool?office=SPK",
    },
    "low-water-lower-pool-location-level": {
        "level-value": 55.0,
        "level-link": "/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Lower Pool?office=SPK",
    },
    "high-water-upper-pool-warning-level": 2.0,
    "high-water-lower-pool-warning-level": 2.0,
}


def _cleanup():
    lk.delete_lock(LOCK_ID, TEST_OFFICE)
    pl.delete_location(TEST_PROJECT_ID, TEST_OFFICE)


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    try:
        _cleanup()
    except Exception:
        pass
    pl.store_location(TEST_PROJECT_LOCATION, False)
    lk.create_lock(TEST_LOCK, False)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for water supply accounting tests...")


def test_get_locks():
    locks = lk.get_locks()
    assert locks is not None


def test_get_lock():
    lock = lk.get_lock(LOCK_ID, TEST_OFFICE)
    assert lock is not None
    assert lock.json == TEST_LOCK


def test_create_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock-879"
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    assert lock.json == test_lock2


def test_delete_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock-456"
    test_lock2["location"]["name"] = new_loc
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    assert lock.json == test_lock2
    lk.delete_lock(new_loc, TEST_OFFICE)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is None


def test_update_lock():
    test_lock2 = TEST_LOCK
    new_loc = "pytest-lock-881"
    test_lock2["location"]["name"] = new_loc
    test_lock2["location"]["description"] = "pytest-lock-description"
    lk.create_lock(test_lock2, False)
    lock = lk.get_lock(new_loc, TEST_OFFICE)
    assert lock is not None
    assert lock.json == test_lock2
    lk.update_lock(LOCK_ID, TEST_OFFICE, new_loc)
    lock = lk.get_lock(LOCK_ID, TEST_OFFICE)
    assert lock is not None
    assert lock.json == test_lock2
