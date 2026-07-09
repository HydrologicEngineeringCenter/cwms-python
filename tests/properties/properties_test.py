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
_PROPERTIES_JSON = read_resource_file("properties.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_property(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/properties/PropertyName?office=SPK"
        f"&category-id=CWMS&default-value=DefaultValue",
        json=_PROPERTY_JSON,
    )
    data = properties.get_property("SPK", "CWMS", "PropertyName", "DefaultValue")
    assert data.json == _PROPERTY_JSON


def test_get_properties(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/properties?office-mask=SP%2A"
        f"&category-id-mask=CW%2A&name-mask=Property%2A",
        json=_PROPERTIES_JSON,
    )
    data = properties.get_properties("SP*", "CW*", "Property*")
    assert data.json == _PROPERTIES_JSON


def test_store_property(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/properties",
        status_code=204,
        json=_PROPERTY_JSON,
    )
    properties.store_property(_PROPERTY_JSON)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_update_property(requests_mock):
    requests_mock.patch(
        f"{_MOCK_ROOT}/properties/PropertyName",
        status_code=200,
    )
    properties.update_property("PropertyName", _PROPERTY_JSON)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_property(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/properties/PropertyName?office=SPK&category-id=CWMS",
        status_code=200,
    )
    properties.delete_property("SPK", "CWMS", "PropertyName")
    assert requests_mock.called
    assert requests_mock.call_count == 1
