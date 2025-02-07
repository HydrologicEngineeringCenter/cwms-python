#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

import pandas as pd
import pytest

import cwms.api
from cwms import turbines
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TURBINES = read_resource_file("turbines.json")
_TURBINES_NAME = read_resource_file("turbines_name.json")
_TURBINES_OFFICE_NAME = read_resource_file("turbines_office_name.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)

def test_get_projects_turbines(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/turbines?project-id=KEYS&office=SWT",
        json=_TURBINES,
    )

    office_id = "SWT"
    name = "Test.Turbine"

    data = turbines.get_projects_turbines(name=name, office_id=office_id)

    assert data.json == _TURBINES
    assert type(data.df) is pd.DataFrame
    assert "turbine-id" in data.df.columns
    assert data.df.shape == (1, 7)
    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "SWT",
        "Test.Turbine",
        "Test Turbine",
        "Test Turbine Description",
        0.0,
        0.0,
        0.0,
    ]

def test_get_projects_turbines_by_name(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/turbines/KEYS-Turbine1&office=SWT",
        json=_TURBINES_NAME,
    )

    office_id = "SWT"
    name = "Test.Turbine"

    data = turbines.get_projects_turbines_with_name(name=name, office_id=office_id)

    assert data.json == _TURBINES
    assert type(data.df) is pd.DataFrame
    assert "turbine-id" in data.df.columns
    assert data.df.shape == (1, 7)
    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "SWT",
        "Test.Turbine",
        "Test Turbine",
        "Test Turbine Description",
        0.0,
        0.0,
        0.0,
    ]

def test_get_projects_turbines_by_office_name(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/SWT/KEYS/turbines-changes",
        json=_TURBINES_OFFICE_NAME,
    )

    office_id = "SWT"
    name = "Test.Turbine"

    data = turbines.get_projects_turbines_with_office_with_name_turbine_changes(name=name, office_id=office_id)

    assert data.json == _TURBINES
    assert type(data.df) is pd.DataFrame
    assert "turbine-id" in data.df.columns
    assert data.df.shape == (1, 7)
    values = data.df.to_numpy().tolist()
    assert values[0] == [
        "SWT",
        "Test.Turbine",
        "Test Turbine",
        "Test Turbine Description",
        0.0,
        0.0,
        0.0,
    ]
