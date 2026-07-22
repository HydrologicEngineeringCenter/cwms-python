#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pytest

import cwms.api as api
import cwms.properties.properties as properties

TEST_OFFICE = "SPK"
TEST_CATEGORY = "PytestCategory"
TEST_NAME = "PytestProperty"
TEST_VALUE = "PytestValue"
TEST_COMMENT = "PytestComment"

TEST_PROPERTY_DATA = {
    "office-id": TEST_OFFICE,
    "name": TEST_NAME,
    "category": TEST_CATEGORY,
    "value": TEST_VALUE,
    "comment": TEST_COMMENT,
}


@pytest.fixture(scope="module", autouse=True)
def setup_data():
    # Clean up any leftover state from a prior aborted run before starting.
    try:
        properties.delete_property(TEST_NAME, TEST_OFFICE, TEST_CATEGORY)
    except Exception:
        pass

    properties.create_property(TEST_PROPERTY_DATA)
    yield
    try:
        properties.delete_property(TEST_NAME, TEST_OFFICE, TEST_CATEGORY)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def init_session():
    # Session initialization is handled by environment or global config in CDA tests
    print("Initializing CWMS API session for properties tests...")


def test_create_property():
    properties.create_property(TEST_PROPERTY_DATA)
    props = properties.get_property(TEST_NAME, TEST_OFFICE, TEST_CATEGORY)
    assert props.json.get("name") == TEST_NAME
    assert props.json.get("office-id") == TEST_OFFICE
    assert props.json.get("category") == TEST_CATEGORY
    assert props.json.get("value") == TEST_VALUE
    assert props.json.get("comment") == TEST_COMMENT


def test_get_properties():
    data = properties.get_properties(
        office_mask=TEST_OFFICE, category_id_mask=TEST_CATEGORY
    )
    assert data is not None
    # Check if our test property is in the returned list
    found = False
    for prop in data.json:
        if prop.get("name") == TEST_NAME and prop.get("office-id") == TEST_OFFICE:
            found = True
            break
    assert found


def test_get_property():
    data = properties.get_property(TEST_NAME, TEST_OFFICE, TEST_CATEGORY)
    assert data is not None
    assert data.json.get("name") == TEST_NAME
    assert data.json.get("office-id") == TEST_OFFICE
    assert data.json.get("category") == TEST_CATEGORY
    assert data.json.get("value") == TEST_VALUE


def test_update_property():
    updated_value = "UpdatedPytestValue"
    updated_data = TEST_PROPERTY_DATA.copy()
    updated_data["value"] = updated_value

    properties.update_property(TEST_NAME, updated_data)

    data = properties.get_property(TEST_NAME, TEST_OFFICE, TEST_CATEGORY)
    assert data.json.get("value") == updated_value


def test_delete_property():
    # Create a temporary property to delete
    temp_name = "TempDeleteProperty"
    temp_data = TEST_PROPERTY_DATA.copy()
    temp_data["name"] = temp_name

    properties.create_property(temp_data)

    # Verify it was created
    data = properties.get_property(temp_name, TEST_OFFICE, TEST_CATEGORY)
    assert data.json.get("name") == temp_name

    # Delete it
    properties.delete_property(temp_name, TEST_OFFICE, TEST_CATEGORY)

    # Verify it was deleted
    with pytest.raises(api.ApiError):
        properties.get_property(temp_name, TEST_OFFICE, TEST_CATEGORY)
