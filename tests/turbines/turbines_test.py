import pandas as pd
import pytest

import cwms.api
import cwms.turbines.turbines as turbines
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_TURBINES = read_resource_file("turbines.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_projects_turbines(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/projects/turbines?project-id=KEYS&office=SWT",
        json=_TURBINES,
    )

    office = "SWT"
    project_id = "KEYS"

    data = turbines.get_projects_turbines(projectId=project_id, office=office)

    assert data.json == _TURBINES
    assert isinstance(data.df, pd.DataFrame)

    # Validate columns
    expected_columns = [
        "office-id",
        "project-name",
        "turbine-name",
        "latitude",
        "longitude",
        "active",
        "elevation",
    ]
    assert list(data.df.columns) == expected_columns

    # Validate DataFrame shape (2 rows, 7 columns)
    assert data.df.shape == (2, 7)

    # Validate the first row of data
    expected_values = [
        ["SWT", "KEYS", "KEYS-Turbine1", 36.1506371, -96.2525088, True, 0],
        ["SWT", "KEYS", "KEYS-Turbine2", 36.1506371, -96.2525088, True, 0],
    ]
    actual_values = data.df.to_numpy().tolist()
    assert actual_values == expected_values
