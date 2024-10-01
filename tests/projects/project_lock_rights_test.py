#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
# constant for mock root url

import cwms.projects.project_lock_rights as project_lock_rights
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

# constants for json payloads, replace with your actual json payloads
_PROJECTS_LOCK_RIGHTS_JSON = read_resource_file("project_lock_rights.json")


def test_get_project_lock_rights(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/project-lock-rights?office-mask=SPK&project-mask=B%2A"
        f"&application-mask=G*",
        json=_PROJECTS_LOCK_RIGHTS_JSON,
    )
    data = project_lock_rights.get_project_lock_rights("SPK", "B*", "G*")
    assert data.json == _PROJECTS_LOCK_RIGHTS_JSON


def test_remove_all_project_lock_rights(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/project-lock-rights/remove-all?office=SPK"
        f"&application-id=GitHub&user-id=Actions",
        status_code=200,
    )
    project_lock_rights.remove_all_project_lock_rights("SPK", "GitHub", "Actions")
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_update_project_lock_rights(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/project-lock-rights/update?office=SPK"
        f"&application-id=GitHub&user-id=Actions&allow=True"
        f"&project-mask=P%2A",
        status_code=200,
    )
    project_lock_rights.update_project_lock_rights(
        "SPK", "GitHub", "Actions", True, "P*"
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
