#  Copyright (c) 2024
#  United States Army Corps of Engineers - Hydrologic Engineering Center (USACE/HEC)
#  All Rights Reserved.  USACE PROPRIETARY/CONFIDENTIAL.
#  Source may not be released without written approval from HEC

from datetime import datetime

import pytest
import pytz

import cwms.api
import cwms.forecast.forecast_instance as forecast_instance
from tests._test_utils import read_resource_file

_MOCK_ROOT = "https://mockwebserver.cwms.gov"
_FORECAST_INSTANCES_JSON = read_resource_file("forecast_instances.json")
_FORECAST_INSTANCE_JSON = read_resource_file("forecast_instance.json")


@pytest.fixture(autouse=True)
def init_session():
    cwms.api.init_session(api_root=_MOCK_ROOT)


def test_get_forecast_instances(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/forecast-instance?office=SWT"
        f"&name=test-spec&designator=designator",
        json=_FORECAST_INSTANCES_JSON,
    )

    forecast = forecast_instance.get_forecast_instances(
        "test-spec", "SWT", "designator"
    )
    assert forecast.json == _FORECAST_INSTANCES_JSON


def test_retrieve_forecast_instance_json(requests_mock):
    requests_mock.get(
        f"{_MOCK_ROOT}/forecast-instance/test-spec?"
        f"office=SWT&designator=designator"
        f"&forecast-date=2021-06-21T14%3A00%3A10"
        f"&issue-date=2022-05-22T12%3A03%3A40",
        json=_FORECAST_INSTANCES_JSON,
    )

    forecast_date = datetime.utcfromtimestamp(1624284010000 / 1000)
    issue_date = datetime.utcfromtimestamp(1653221020000 / 1000)
    forecast = forecast_instance.get_forecast_instance(
        "test-spec", "SWT", "designator", forecast_date, issue_date
    )
    assert forecast.json == _FORECAST_INSTANCES_JSON


def test_store_forecast_instance_json(requests_mock):
    requests_mock.post(
        f"{_MOCK_ROOT}/forecast-instance",
        status_code=200,
        json=_FORECAST_INSTANCE_JSON,
    )
    forecast_instance.store_forecast_instance(_FORECAST_INSTANCE_JSON)
    assert requests_mock.called
    assert requests_mock.call_count == 1


def test_delete_forecast_instance_json(requests_mock):
    requests_mock.delete(
        f"{_MOCK_ROOT}/forecast-instance/test-spec?"
        f"office=SWT&designator=designator"
        f"&forecast-date=2021-06-21T14%3A00%3A10"
        f"&issue-date=2022-05-22T12%3A03%3A40",
        status_code=200,
    )
    forecast_date = datetime.utcfromtimestamp(1624284010000 / 1000)
    issue_date = datetime.utcfromtimestamp(1653221020000 / 1000)
    forecast_instance.delete_forecast_instance(
        "test-spec", "SWT", "designator", forecast_date, issue_date
    )
    assert requests_mock.called
    assert requests_mock.call_count == 1
