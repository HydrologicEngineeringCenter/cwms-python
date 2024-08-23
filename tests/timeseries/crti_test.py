import pytest
import pandas as pd
import json
from unittest.mock import patch
from cwms.timeseries import critscript as cs


@pytest.fixture
def mock_crit_file_content():
    return """Alias1=TSID1;Alias2
Alias2=TSID2;Alias3
"""


@pytest.fixture
def expected_df():
    data = {
        "office-id": ["CWMS", "CWMS"],
        "ts-id": ["TSID1", "TSID2"],
        "alias": ["Alias1 Alias2", "Alias2 Alias3"],
        "ts-code": ["none", "none"],
        "attribute": [0, 0]
    }
    return pd.DataFrame(data)


@pytest.fixture
def expected_json_dict():
    return {
        "office-id": "CWMS",
        "id": "Data Acquisition",
        "time-series-category": {
            "office-id": "CWMS",
            "id": "Data Acquisition"
        },
        "time-series": [
            {"office-id": "CWMS", "id": "TSID1", "alias": "Alias1 Alias2", "ts-code": "none", "attribute": 0},
            {"office-id": "CWMS", "id": "TSID2", "alias": "Alias2 Alias3", "ts-code": "none", "attribute": 0}
        ]
    }


@pytest.fixture
def incorrect_json_dict():
    return {
        "office-id": "CWMS",
        "id": "Data Acquisition",
        "time-series-category": {
            "office-id": "CWMS",
            "id": "Data Acquisition"
        },
        "time-series": [
            {"office-id": "CWMS", "id": "TSID1", "alias": "Alias1 Alias2", "ts-code": "none", "attribute": 0}
        ]
    }


@patch('module.api.patch')  # Mock the `api.patch` call
def test_crit_script_pass(mock_patch, mock_crit_file_content, expected_df, expected_json_dict):
    file_path = 'test.crit'
    office_id = 'CWMS'
    group_id = 'Data Acquisition'

    # Mock the file reading
    with open(file_path, 'w') as file:
        file.write(mock_crit_file_content)

    # Mock the API call
    mock_patch.return_value.status_code = 200  # Simulate a successful API call

    # Run the script
    cs.crit_script(file_path, office_id, group_id)

    # Check if the API patch was called with the correct arguments
    assert mock_patch.call_count == 1
    call_args = mock_patch.call_args[1]
    assert call_args['endpoint'] == f"timeseries/group/{group_id}"
    assert call_args['params']['replace-assigned-ts'] == False
    assert call_args['params']['office'] == office_id

    # Validate the JSON dictionary used in the patch
    json_data = json.loads(mock_patch.call_args[0][1])  # Get the body of the PATCH request
    assert json_data == expected_json_dict


@patch('module.api.patch')  # Mock the `api.patch` call
def test_crit_script_fail(mock_patch, mock_crit_file_content, expected_df, incorrect_json_dict):
    file_path = 'test.crit'
    office_id = 'CWMS'
    group_id = 'Data Acquisition'

    # Mock the file reading
    with open(file_path, 'w') as file:
        file.write(mock_crit_file_content)

    # Mock the API call
    mock_patch.return_value.status_code = 200  # Simulate a successful API call

    # Run the script
    cs.crit_script(file_path, office_id, group_id)

    # Check if the API patch was called with the incorrect arguments
    assert mock_patch.call_count == 1
    call_args = mock_patch.call_args[1]
    assert call_args['endpoint'] == f"timeseries/group/{group_id}"
    assert call_args['params']['replace-assigned-ts'] == False
    assert call_args['params']['office'] == office_id

    # Validate the JSON dictionary used in the patch
    json_data = json.loads(mock_patch.call_args[0][1])  # Get the body of the PATCH request
    assert json_data != incorrect_json_dict  # This assertion is expected to fail
