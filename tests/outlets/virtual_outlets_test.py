#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import cwms.outlets.virtual_outlets as virtual_outlets
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

# constants for json payloads, replace with your actual json payloads
_VIRTUAL_OUTLET_JSON = read_resource_file("virtual_outlet.json")
_VIRTUAL_OUTLETS_JSON = read_resource_file("virtual_outlets.json")


def test_get_virtual_outlet(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/SPK/BIGH/virtual-outlets/"
        f"Compound%20Tainter%20Gates",
        json=_VIRTUAL_OUTLET_JSON,
    )
    data = virtual_outlets.get_virtual_outlet("SPK", "BIGH", "Compound Tainter Gates")
    assert data.json == _VIRTUAL_OUTLET_JSON


def test_get_virtual_outlets(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/SPK/BIGH/virtual-outlets",
        json=_VIRTUAL_OUTLETS_JSON,
    )
    data = virtual_outlets.get_virtual_outlets("SPK", "BIGH")
    assert data.json == _VIRTUAL_OUTLETS_JSON


def test_store_virtual_outlet(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/projects/virtual-outlets?fail-if-exists=True",
        status_code=200,
        json=_VIRTUAL_OUTLET_JSON,
    )
    virtual_outlets.store_virtual_outlet(_VIRTUAL_OUTLET_JSON, True)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_virtual_outlet(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/projects/SWT/PROJ/virtual-outlets/Test?method=DELETE_ALL",
        status_code=200,
    )
    virtual_outlets.delete_virtual_outlet(
        "SWT", "PROJ", "Test", DeleteMethod.DELETE_ALL
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
