#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import urllib.parse
from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.locations.gate_changes as gate_changes
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_GATE_CHANGE_JSON = read_resource_file("gate_change.json")

tz = pytz.timezone("UTC")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_all_gate_changes(requests_mock):
    office_id = "SPK"
    project_id = "SPK.BIGH"
    start = datetime.fromisoformat("2022-01-01T00:00:00")
    end = datetime.fromisoformat("2025-01-01T00:00:00")

    requests_mock.get(
        f"{_MOCK_ROOT}/projects/{office_id}/{project_id}/gate-changes",
        json=_GATE_CHANGE_JSON,
    )

    data = gate_changes.get_all_gate_changes(office_id, project_id, start, end)

    assert data.json == _GATE_CHANGE_JSON


def test_store_gate_change(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/projects/gate-changes?fail-if-exists=False")

    data = _GATE_CHANGE_JSON
    gate_changes.store_gate_change(data, False)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_gate_change(requests_mock):
    office_id = "SPK"
    project_id = "SPK.BIGH"
    start = datetime.fromisoformat("2022-01-01T00:00:00")
    end = datetime.fromisoformat("2025-01-01T00:00:00")

    requests_mock.delete(
        f"{_MOCK_ROOT}/projects/{office_id}/{project_id}/gate-changes",
    )

    gate_changes.delete_gate_change(office_id, project_id, start, end)

    assert requests_mock.called
    assert requests_mock.call_count == 1
