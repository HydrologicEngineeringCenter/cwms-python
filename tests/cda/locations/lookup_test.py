import pytest

import cwms.api as api
import cwms.locations.lookups as lookups
from cwms.cwms_types import Data

OFFICE_ID = "SPK"
CATEGORY = "USACE"
PREFIX = "L"
DISPLAY_VALUE = "Test Lookup"
TOOLTIP = "Test Tooltip"

LOOKUP_DATA = {
    "office-id": OFFICE_ID,
    "display-value": DISPLAY_VALUE,
    "tooltip": TOOLTIP,
    "active": True,
}


def _cleanup():
    try:
        lookups.delete_lookup(DISPLAY_VALUE, CATEGORY, PREFIX, OFFICE_ID)
    except Exception:
        pass


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    _cleanup()
    lookups.create_lookup(LOOKUP_DATA, CATEGORY, PREFIX)


@pytest.fixture(autouse=True)
def init_session():
    print("Initializing CWMS API session for lookup type tests...")


def test_get_all_lookups():
    new_display_value = DISPLAY_VALUE + " Refreshed"
    data = {
        "office-id": OFFICE_ID,
        "display-value": new_display_value,
        "tooltip": TOOLTIP,
        "active": True,
    }
    lookups.create_lookup(data, CATEGORY, PREFIX)

    result = lookups.get_all_lookups(CATEGORY, PREFIX, OFFICE_ID)
    found = False
    found2 = False
    for item in result.json:
        if item.get("display-value") == new_display_value:
            found = True
        if item.get("display-value") == DISPLAY_VALUE:
            found2 = True
    assert (
        found
    ), f"Lookup with display-value {new_display_value} not found after creation"
    assert found2, f"Lookup with display-value {DISPLAY_VALUE} not found after creation"


def test_create_lookup():
    display_value = DISPLAY_VALUE + " Created"
    data = {
        "office-id": OFFICE_ID,
        "display-value": display_value,
        "tooltip": TOOLTIP,
        "active": True,
    }
    lookups.create_lookup(data, CATEGORY, PREFIX)

    result = lookups.get_all_lookups(CATEGORY, PREFIX, OFFICE_ID)
    found = False
    for item in result.json:
        if item.get("display-value") == display_value:
            found = True
            break
    assert found, f"Lookup with display-value {display_value} not found after creation"


def test_update_lookup():
    new_display_value = DISPLAY_VALUE + " Updated"
    data = {
        "office-id": OFFICE_ID,
        "display-value": new_display_value,
        "tooltip": "Updated Tooltip",
        "active": True,
    }
    # The name parameter in update_lookup corresponds to the display-value of the item to update
    lookups.update_lookup(data, DISPLAY_VALUE, CATEGORY, PREFIX)

    result = lookups.get_all_lookups(CATEGORY, PREFIX, OFFICE_ID)
    found = False
    for item in result.json:
        if item.get("display-value") == new_display_value:
            found = True
            break
    assert found, f"Lookup with updated display-value {new_display_value} not found"

    try:
        lookups.delete_lookup(new_display_value, CATEGORY, PREFIX, OFFICE_ID)
    except Exception:
        pass


def test_delete_lookup():
    office = "LRL"
    data = {
        "office-id": office,
        "display-value": DISPLAY_VALUE,
        "tooltip": TOOLTIP,
        "active": True,
    }
    lookups.create_lookup(data, CATEGORY, PREFIX)

    lookups.delete_lookup(DISPLAY_VALUE, CATEGORY, PREFIX, office)

    result = lookups.get_all_lookups(CATEGORY, PREFIX, office)
    found = False
    for item in result.json:
        if item.get("display-value") == DISPLAY_VALUE:
            found = True
            break
    assert (
        not found
    ), f"Lookup with display-value {DISPLAY_VALUE} still found after deletion"
