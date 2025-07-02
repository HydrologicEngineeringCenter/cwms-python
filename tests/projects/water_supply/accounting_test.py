import pytest

import cwms.api
from cwms.cwms_types import Data
from cwms.projects.water_supply.accounting import (
    get_pump_accounting,
    store_pump_accounting,
)


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root="https://mockwebserver.cwms.gov")


def test_get_pump_accounting(requests_mock):
    endpoint = (
        "https://mockwebserver.cwms.gov/"
        "projects/SWT/KEYS/water-user/TEST_USER/contracts/TEST_CONTRACT/accounting"
    )
    expected_json = {"entries": [{"timestamp": "2024-01-01T00:00:00Z", "value": 1.23}]}

    requests_mock.get(endpoint, json=expected_json)

    data = get_pump_accounting(
        office_id="SWT",
        project_id="KEYS",
        water_user="TEST_USER",
        contract_name="TEST_CONTRACT",
        start="2024-01-01T00:00:00Z",
        end="2024-01-02T00:00:00Z",
    )

    assert isinstance(data, Data)
    assert data.json == expected_json


def test_store_pump_accounting(requests_mock):
    endpoint = (
        "https://mockwebserver.cwms.gov/"
        "projects/SWT/KEYS/water-user/TEST_USER/contracts/TEST_CONTRACT/accounting"
    )
    mock_data = {"entries": [{"timestamp": "2024-01-01T00:00:00Z", "value": 1.23}]}

    requests_mock.post(endpoint, status_code=200)

    store_pump_accounting(
        office="SWT",
        project_id="KEYS",
        water_user="TEST_USER",
        contract_name="TEST_CONTRACT",
        data=mock_data,
    )

    assert requests_mock.called
    assert requests_mock.call_count == 1
