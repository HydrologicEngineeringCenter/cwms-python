#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
# constant for mock root url

from datetime import datetime

import pytz

import cwms.projects.projects as projects
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"

# constants for json payloads, replace with your actual json payloads
_PROJECT_JSON = read_resource_file("project.json")
_PROJECT_LOCATIONS_JSON = read_resource_file("project_locations.json")
_PROJECTS_JSON = read_resource_file("projects.json")


def test_get_project(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/BIGH?office=SPK",
        json=_PROJECT_JSON,
    )
    data = projects.get_project("SPK", "BIGH")
    assert data.json == _PROJECT_JSON


def test_get_projects(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects?office=SPK&id-mask=B%2A&page=abc&page-size=50",
        json=_PROJECTS_JSON,
    )
    data = projects.get_projects("SPK", "B*", "abc", 50)
    assert data.json == _PROJECTS_JSON


def test_get_project_locations(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/locations?office=SPK&"
        f"project-like=p.%2A&location-kind-like=O.%2A",
        json=_PROJECTS_JSON,
    )
    data = projects.get_project_locations("SPK", "p.*", "O.*")
    assert data.json == _PROJECTS_JSON


def test_store_project(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/projects?fail-if-exists=True",
        status_code=200,
        json=_PROJECT_JSON,
    )
    projects.store_project(_PROJECT_JSON, True)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_project(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/projects/Test?office=SPK&method=DELETE_ALL",
        status_code=200,
    )
    projects.delete_project("SPK", "Test", DeleteMethod.DELETE_ALL)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_rename_project(requests_mock):
    requests_mock.patch(
        f"{_MOCK_ROOT}/projects/Test?office=SPK&name=Test2",
        status_code=200,
    )
    projects.rename_project("SPK", "Test", "Test2")
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_status_update(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/projects/status-update/Test?office=SPK"
        f"&application-id=UnitTest&source-id=GitHub"
        f"&timeseries-id=ABC.Flow.Ave.1Day.1Day.computed"
        f"&begin=2024-02-12T00%3A00%3A00-08%3A00"
        f"&end=2020-02-12T02%3A00%3A00-08%3A00",
        status_code=200,
    )
    timezone = pytz.timezone("US/Pacific")
    begin = timezone.localize(datetime(2024, 2, 12, 0, 0, 0))
    end = timezone.localize(datetime(2020, 2, 12, 2, 0, 0))
    projects.status_update(
        "SPK",
        "Test",
        "UnitTest",
        "GitHub",
        "ABC.Flow.Ave.1Day.1Day.computed",
        begin,
        end,
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
