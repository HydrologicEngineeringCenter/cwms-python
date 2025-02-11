from datetime import datetime

import pandas as pd
import pytest

import cwms.api
import cwms.turbines.turbines as turbines
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TURBINES = read_resource_file("turbines.json")
_TURBINES_NAME = read_resource_file("turbines_name.json")
_TURBINES_OFFICE_NAME = read_resource_file("turbines_office_name.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


# ==========================================================================
#                             GET CWMS TURBINES
# ==========================================================================


def test_get_projects_turbines(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/turbines?project-id=KEYS&office=SWT",
        json=_TURBINES,
    )

    office = "SWT"
    project_id = "KEYS"

    data = turbines.get_projects_turbines(project_id=project_id, office=office)

    assert data.json == _TURBINES
    assert isinstance(data.df, pd.DataFrame)

    # Validate columns
    expected_columns = [
        "project-id.office-id",
        "project-id.name",
        "location.office-id",
        "location.name",
        "location.latitude",
        "location.longitude",
        "location.active",
        "location.public-name",
        "location.long-name",
        "location.timezone-name",
        "location.location-kind",
        "location.nation",
        "location.state-initial",
        "location.county-name",
        "location.nearest-city",
        "location.horizontal-datum",
        "location.published-longitude",
        "location.published-latitude",
        "location.vertical-datum",
        "location.elevation",
        "location.bounding-office-id",
        "location.elevation-units",
    ]
    assert list(data.df.columns) == expected_columns

    # Validate DataFrame shape (2 rows, 22 columns)
    assert data.df.shape == (2, 22)

    # Validate the first row of data
    expected_values = [
        [
            "SWT",
            "KEYS",
            "SWT",
            "KEYS-Turbine1",
            36.1506371,
            -96.2525088,
            True,
            "Turbine1",
            "Turbine1",
            "US/Central",
            "TURBINE",
            "US",
            "OK",
            "Tulsa",
            "Sand Springs, OK",
            "NAD83",
            0,
            0,
            "NGVD29",
            0,
            "SWT",
            "m",
        ],
        [
            "SWT",
            "KEYS",
            "SWT",
            "KEYS-Turbine2",
            36.1506371,
            -96.2525088,
            True,
            "Turbine2",
            "Turbine2",
            "US/Central",
            "TURBINE",
            "US",
            "OK",
            "Tulsa",
            "Sand Springs, OK",
            "NAD83",
            0,
            0,
            "NGVD29",
            0,
            "SWT",
            "m",
        ],
    ]
    actual_values = data.df.to_numpy().tolist()

    assert actual_values == expected_values


def get_projects_turbines_with_name(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/turbines?name=KEYS&office=SWT",
        json=_TURBINES_NAME,
    )

    project_id = "KEYS"
    office = "SWT"

    data = turbines.get_projects_turbines(office=office, project_id=project_id)
    expected_columns = [
        "project-id.office-id",
        "project-id.name",
        "location.office-id",
        "location.name",
        "location.latitude",
        "location.longitude",
        "location.active",
        "location.public-name",
        "location.long-name",
        "location.timezone-name",
        "location.location-kind",
        "location.nation",
        "location.state-initial",
        "location.county-name",
        "location.nearest-city",
        "location.horizontal-datum",
        "location.published-longitude",
        "location.published-latitude",
        "location.vertical-datum",
        "location.elevation",
        "location.bounding-office-id",
        "location.elevation-units",
    ]

    expected_values = [["SWT", "KEYS", "SWT"]]
    assert list(data.df.columns) == expected_columns
    assert data.df.shape == (1, len(expected_columns))
    assert data.df.to_numpy().tolist() == expected_values


def test_get_projects_turbines_with_office_with_name(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/SWT/KEYS/turbines", json=_TURBINES_OFFICE_NAME
    )

    office = "SWT"
    name = "KEYS"

    data = turbines.get_projects_turbines_with_office_with_name_turbine_changes(
        name=name,
        begin=datetime(2024, 1, 1),
        end=datetime(2024, 12, 31),
        office=office,
        page_size=100,
        unit_system=None,
        start_time_inclusive=True,
        end_time_inclusive=True,
    )
    expected_columns = [
        "change-date",
        "protected",
        "notes",
        "new-total-discharge-override",
        "old-total-discharge-override",
        "discharge-units",
        "tailwater-elevation",
        "elevation-units",
        "settings",
        "pool-elevation",
        "project-id.office-id",
        "project-id.name",
        "discharge-computation-type.office-id",
        "discharge-computation-type.display-value",
        "discharge-computation-type.tooltip",
        "discharge-computation-type.active",
        "reason-type.office-id",
        "reason-type.display-value",
        "reason-type.tooltip",
        "reason-type.active",
    ]

    expected_values = [
        [
            1738713600000,
            False,
            "from SCADA",
            0,
            0,
            "cfs",
            637.7499999999999,
            "ft",
            [
                {
                    "type": "turbine-setting",
                    "location-id": {"office-id": "SWT", "name": "KEYS-Turbine1"},
                    "discharge-units": "cfs",
                    "old-discharge": 0,
                    "new-discharge": 0,
                    "generation-units": "MW",
                    "scheduled-load": 0,
                    "real-power": 0,
                },
                {
                    "type": "turbine-setting",
                    "location-id": {"office-id": "SWT", "name": "KEYS-Turbine2"},
                    "discharge-units": "cfs",
                    "old-discharge": 0,
                    "new-discharge": 0,
                    "generation-units": "MW",
                    "scheduled-load": 0,
                    "real-power": 0,
                },
            ],
            723.12,
            "SWT",
            "KEYS",
            "SWT",
            "R",
            "Reported by powerhouse",
            True,
            "SWT",
            "S",
            "Scheduled release to meet loads",
            True,
        ]
    ]

    assert list(data.df.columns) == expected_columns
    assert data.df.shape == (1, len(expected_columns))
    assert data.df.to_numpy().tolist() == expected_values


# ==========================================================================
#                             POST CWMS TURBINES
# ==========================================================================


def test_store_projects_turbines(requests_mock):
    requests_mock.post(f"{_MOCK_ROOT}/projects/turbines?fail-if-exists=false")

    turbines.store_projects_turbines(data=_TURBINES, fail_if_exists=False)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_store_projects_turbines_with_office_with_name(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/projects/SWT/KEYS/turbines?override-protection=False"
    )

    turbines.store_projects_turbines_with_office_with_name(
        data=_TURBINES_OFFICE_NAME, office="SWT", name="KEYS", override_protection=False
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1


# ==========================================================================
#                             DELETE CWMS TURBINES
# ==========================================================================


def test_delete_projects_turbines_with_name(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/projects/turbines/KEYS-Turbine1?office=SWT&method=DELETE_ALL"
    )

    name = "KEYS-Turbine1"
    office = "SWT"
    method = "DELETE_ALL"

    turbines.delete_projects_turbines_with_name(name=name, office=office, method=method)

    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_projects_turbines_with_office_with_name(requests_mock):
    requests_mock.delete(f"{_MOCK_ROOT}/projects/SWT/KEYS/turbines")

    office = "SWT"
    name = "KEYS"

    turbines.delete_projects_turbines_with_office_with_name(
        office=office,
        name=name,
        override_protection=False,
        begin=datetime(2024, 1, 1),
        end=datetime(2024, 12, 31),
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1
