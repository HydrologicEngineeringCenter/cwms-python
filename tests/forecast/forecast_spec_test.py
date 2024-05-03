#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC
import pytest
import pytz

import cwms.api
import cwms.forecast.forecast_spec as forecast_spec
from cwms.types import DeleteMethod
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_FORECAST_SPECS_JSON = read_resource_file("forecast_specs.json")
_FORECAST_SPEC_JSON = read_resource_file("forecast_spec.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_forecast_specs(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/forecast-spec?office=office_mask"
        f"&id_mask=id_mask&designator-mask=designator_mask"
        f"&source-entity=source_entity",
        json=_FORECAST_SPECS_JSON,
    )
    forecast = forecast_spec.get_forecast_specs(
        "id_mask",
        "office_mask",
        "designator_mask",
        "source_entity",
    )
    assert forecast.json == _FORECAST_SPECS_JSON


def test_get_forecast_spec(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/forecast-spec/test-spec?" f"office=SWT&designator=designator",
        json=_FORECAST_SPECS_JSON,
    )
    forecast = forecast_spec.get_forecast_spec("test-spec", "SWT", "designator")
    assert forecast.json == _FORECAST_SPECS_JSON


def test_store_forecast_spec_json(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/forecast-spec",
        status_code=200,
        json=_FORECAST_SPEC_JSON,
    )
    forecast_spec.store_forecast_spec(_FORECAST_SPEC_JSON)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_forecast_spec(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/forecast-spec/test-spec?"
        f"office=SWT&designator=designator&method=DELETE_KEY",
        status_code=200,
    )

    forecast_spec.delete_forecast_spec(
        "test-spec", "SWT", "designator", DeleteMethod.DELETE_KEY
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
