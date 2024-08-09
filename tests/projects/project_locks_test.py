#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
# constant for mock root url

from datetime import datetime

import pytz

import cwms.projects.project_locks as project_locks
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

# constants for json payloads, replace with your actual json payloads
_PROJECT_LOCK_JSON = read_resource_file("project_lock.json")
_PROJECTS_LOCKS_JSON = read_resource_file("project_locks.json")


def test_get_project_lock(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/project-locks/BIGH?office=SPK&application-id=GitHub",
        json=_PROJECT_LOCK_JSON,
    )
    data = project_locks.get_project_lock("SPK", "BIGH", "GitHub")
    assert data.json == _PROJECT_LOCK_JSON


def test_get_project_locks(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/project-locks?office-mask=SPK&project-mask=P%2A"
        f"&application-mask=G*",
        json=_PROJECTS_LOCKS_JSON,
    )
    data = project_locks.get_project_locks("SPK", "P*", "G*")
    assert data.json == _PROJECTS_LOCKS_JSON


def test_revoke_project_lock(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/project-locks/TEST?office=SPK" f"&revoke-timeout=20",
        json=_PROJECTS_LOCKS_JSON,
    )
    project_locks.revoke_project_lock("SPK", "TEST", 20)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_request_project_lock(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/project-locks?revoke-existing=True" f"&revoke-timeout=20",
        status_code=200,
        json=_PROJECT_LOCK_JSON,
    )
    project_locks.request_project_lock(_PROJECT_LOCK_JSON, True, 20)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_deny_project_lock_request(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/project-locks/deny?lock-id=LOCKID123",
        status_code=200,
    )
    project_locks.deny_project_lock_request("LOCKID123")
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_release_project_lock(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/project-locks/release?office=SPK&lock-id=LOCKID123",
        status_code=200,
    )
    project_locks.release_project_lock("SPK", "LOCKID123")
    assert requests_mock.called
    assert requests_mock.call_count == 1
