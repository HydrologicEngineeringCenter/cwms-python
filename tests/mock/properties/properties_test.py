#  Copyright (c) 2026
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pytest

import cwms.api
import cwms.properties.properties as properties
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_PROPERTY_JSON = read_resource_file("property.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_properties(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/properties?office-mask=SWT",
        json=read_resource_file("properties.json"),
    )
    data = properties.get_properties(office_mask="SWT")
    assert data.json == read_resource_file("properties.json")


def test_get_property(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}" "/properties/TestProperty1?office=SWT&category-id=TestCategory",
        json=_PROPERTY_JSON,
    )
    data = properties.get_property(
        name="TestProperty1", office="SWT", category_id="TestCategory"
    )
    assert data.json == _PROPERTY_JSON


def test_create_property(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}" "/properties", status_code=201)
    data = _PROPERTY_JSON
    properties.create_property(data)
    assert requests_mock.called


def test_update_property(requests_mock):
    requests_mock.patch(f"{_MOCK_ROOT}" "/properties/TestProperty1", status_code=200)
    data = _PROPERTY_JSON
    properties.update_property("TestProperty1", data)
    assert requests_mock.called


def test_delete_property(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}" "/properties/TestProperty1?office=SWT&category-id=TestCategory",
        status_code=204,
    )
    properties.delete_property(
        name="TestProperty1", office="SWT", category_id="TestCategory"
    )
    assert requests_mock.called
